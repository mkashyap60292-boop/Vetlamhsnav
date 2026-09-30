from types import SimpleNamespace
import re
import sqlite3
import json
import time
import threading
import random
import os
import shutil
import html
import traceback
import urllib.request
import urllib.parse
import urllib.error
import uuid
from datetime import datetime, date, timedelta, timezone
try:
    from zoneinfo import ZoneInfo
    IST = ZoneInfo('Asia/Kolkata')
except Exception:
    IST = timezone(timedelta(hours=5, minutes=30))
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton as _BaseInlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from flask import Flask, request, jsonify

# ======================= COLORED / PREMIUM-EMOJI BUTTON SUPPORT =======================
# telebot's InlineKeyboardButton doesn't natively know about "style" (button color)
# or "icon_custom_emoji_id" (premium emoji icon) -- this wrapper adds them safely
# so nothing crashes, and both fields get sent to Telegram in the button JSON,
# exactly like fammprofile.py's aiogram buttons do.
class InlineKeyboardButton(_BaseInlineKeyboardButton):
    def __init__(self, text, style=None, icon_custom_emoji_id=None, **kwargs):
        super().__init__(text, **kwargs)
        self.style = style
        self.icon_custom_emoji_id = icon_custom_emoji_id

    def to_dict(self):
        d = super().to_dict()
        if self.style:
            d['style'] = self.style
        if self.icon_custom_emoji_id:
            d['icon_custom_emoji_id'] = self.icon_custom_emoji_id
        return d

# Default premium emoji ID to use on every button's icon.
# Replace this with a different ID for any specific button if you want variety.
DEFAULT_BUTTON_EMOJI_ID = ""
# A new contact-gate cycle is generated every time this Python process starts.
# Users must share their contact once per bot process/restart; after sharing,
# the normal menu opens immediately.
CONTACT_GATE_CYCLE = uuid.uuid4().hex

# ======================= CONFIG =======================
BOT_TOKEN = "8645401622:AAHrLmsZU5TDqBilEYy1jblYPJ3MhJ4Z9P8"
ADMIN_USER_ID = 8551941196
SUCCESS_IMG = "https://t.me/VetlamOfficial"
UPI_ID = "chut"
UPI_PAYEE_NAME = "VETLAM PANEL SHOP"
OWNER_USERNAME = "@VetlamOfficial_666"

# Reseller panel (bantibhaiya.to) -- used to auto-generate keys for Android-ID-flow
# products that have a Reseller Product ID set, instead of the admin typing the
# key by hand every time.
RESELLER_API_URL = "https://bantibhaiya.to/api/reseller_v1.php"
RESELLER_API_KEY = "035bd4b750a4f8e8f091ec480b50debe"
RESELLER_MASTER_KEY = "a7f3e8b2c9d1f4a6b8c2d5e9f1a3b6c8"

# KeysPanelShop reseller API (second independent provider)
KEYSPANELS_API_URL = "https://keyspanelshop.shop/reseller_api.php"
KEYSPANELS_MASTER_KEY = ""

# ZapUPI (pay.zapupi.com) -- automatic UPI payment gateway used for "Add Balance".
# Replaces the old static-QR + manual-screenshot-approval flow: user taps Add Balance,
# gets a unique ZapUPI payment link, pays with any UPI app, and ZapUPI's webhook
# (handled by the separate webhook_server.py process) credits the balance
# automatically -- no admin approval and no screenshot needed anymore.
ZAPUPI_ZAP_KEY = "zap35bc09f62542fa6449bc1a29a569bf5a"
ZAPUPI_API_BASE = "https://pay.zapupi.com/api"

# FamPay (fampay.anujbots.xyz) -- second automatic UPI gateway, alongside ZapUPI.
# QR-code based (instead of ZapUPI's payment-link based flow): user taps a
# "FamPay" method, gets a UPI QR code, pays, then taps "Check Status" (or the
# background reconciler picks it up) to auto-credit the balance. The API Key
# and UPI ID are NOT hardcoded here -- both are admin-editable from
# Admin Panel -> Settings -> Payment Gateway Settings, same as ZapUPI's key.
FAMPAY_QR_URL = "https://fampay.anujbots.xyz/qr.php"
FAMPAY_VERIFY_URL = "https://fampay.anujbots.xyz/verify.php"
# Public base URL where webhook_server.py is reachable from the internet (VPS IP +
# port for now; swap to a domain later if you set one up). Used to build the
# webhook/success/failed/timeout URLs sent to ZapUPI with every order.
WEBHOOK_BASE_URL = "http://43.242.226.220:5000"

BOT_DIR = os.path.dirname(os.path.abspath(__file__))

# ======================= SINGLE AUTHORITATIVE DATABASE PATH =======================
# The old build derived the DB path from the directory containing main.py.
# If the hosting panel started another copy of main.py from another directory,
# that copy could silently use a second VetlamOfficial_bot.db. That produces the
# exact "new data -> old data -> new data" glitch.
#
# /home/container is the current authoritative hosting directory. You can override
# it with VETLAM_DATA_DIR without editing this file.
_env_data_dir = os.environ.get("VETLAM_DATA_DIR", "").strip()
if _env_data_dir:
    DATA_DIR = os.path.realpath(_env_data_dir)
elif os.path.isdir("/home/container"):
    DATA_DIR = "/home/container"
else:
    DATA_DIR = BOT_DIR

os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.realpath(os.path.join(DATA_DIR, "VetlamOfficial_bot.db"))
BACKUP_DIR = os.path.join(DATA_DIR, "BOT_backups")
INSTANCE_LOCK_PATH = DB_PATH + ".instance.lock"

# Only used for a safe one-time migration if the authoritative DB does not yet
# exist. An existing authoritative DB is NEVER overwritten automatically.
LEGACY_DB_PATH = os.path.realpath(os.path.join(BOT_DIR, "VetlamOfficial_bot.db"))

# SQLite is shared by the Telegram handlers, background workers and Flask thread.
# Use WAL + a busy timeout so short concurrent writes wait instead of immediately
# raising "database is locked".
def get_db():
    # Every handler/background thread opens the SAME absolute DB file.
    # WAL + busy_timeout let Telegram workers and the Flask webhook share SQLite
    # without switching databases or immediately failing on short write contention.
    conn = sqlite3.connect(DB_PATH, timeout=60, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA busy_timeout=60000")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
    except sqlite3.Error as e:
        print(f"[DB PRAGMA] {e}")
    return conn

def sqlite_write_with_retry(write_fn, retries=12, delay=0.25):
    """Retry short SQLite writes when another bot thread temporarily holds the DB lock."""
    last = None
    for attempt in range(retries):
        conn = get_db()
        try:
            result = write_fn(conn)
            conn.commit()
            return result
        except sqlite3.OperationalError as e:
            conn.rollback(); last=e
            if "locked" not in str(e).lower() or attempt == retries-1:
                raise
            time.sleep(delay*(attempt+1))
        finally:
            try: conn.close()
            except Exception: pass
    raise last

def init_db():
    conn = get_db()
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    except sqlite3.Error as e:
        print(f"[DB WAL] {e}")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS plans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            days INTEGER NOT NULL,
            price INTEGER NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS license_keys (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            plan_id INTEGER NOT NULL,
            license_key TEXT NOT NULL UNIQUE,
            used INTEGER DEFAULT 0,
            used_by INTEGER DEFAULT NULL,
            used_at TIMESTAMP NULL DEFAULT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            username TEXT,
            balance INTEGER DEFAULT 0,
            banned INTEGER DEFAULT 0,
            spin_count INTEGER DEFAULT 0,
            last_spin_date TEXT DEFAULT NULL,
            joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            verified INTEGER DEFAULT 0,
            phone_number TEXT DEFAULT NULL,
            contact_gate_cycle TEXT DEFAULT NULL
        )
    """)
    # Robust schema migration: existing SQLite databases created before the
    # contact-gate feature may already have the `users` table, so CREATE TABLE
    # does not add new columns. Always inspect the live schema and add any
    # missing contact-gate columns before any handler can query them.
    try:
        user_columns = {row[1] for row in cursor.execute("PRAGMA table_info(users)").fetchall()}
        if "verified" not in user_columns:
            cursor.execute("ALTER TABLE users ADD COLUMN verified INTEGER DEFAULT 0")
        if "phone_number" not in user_columns:
            cursor.execute("ALTER TABLE users ADD COLUMN phone_number TEXT DEFAULT NULL")
        if "contact_gate_cycle" not in user_columns:
            cursor.execute("ALTER TABLE users ADD COLUMN contact_gate_cycle TEXT DEFAULT NULL")
    except Exception as e:
        print(f"[DB MIGRATION] users schema check failed: {e}")
        raise

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            amount INTEGER NOT NULL,
            type TEXT DEFAULT 'purchase',
            details TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pending_payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            order_id TEXT NOT NULL,
            amount INTEGER NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at INTEGER NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS scheduled_deletions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            delete_at INTEGER NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS product_notify_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(product_id, user_id)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS manual_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            plan_id INTEGER NOT NULL,
            android_id TEXT NOT NULL,
            price INTEGER NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # Migration: older DBs won't have this column yet. It stores the key that
    # was actually delivered (typed by admin OR auto-fetched from reseller),
    # so that manual/API orders can also show up in the user's Order History
    # (previously History only read from license_keys, so manual + reseller
    # orders never appeared there at all).
    try:
        cursor.execute("ALTER TABLE manual_orders ADD COLUMN license_key TEXT DEFAULT NULL")
    except Exception:
        pass
    # Contact-gate columns are ensured above with PRAGMA-based migration.
    try:
        cursor.execute("ALTER TABLE users ADD COLUMN last_active TEXT DEFAULT NULL")
    except:
        pass
    try:
        cursor.execute("ALTER TABLE users ADD COLUMN first_purchase_bonus_claimed INTEGER DEFAULT 0")
    except:
        pass
    for col_def in ["first_name TEXT DEFAULT NULL","referral_code TEXT DEFAULT NULL","referred_by INTEGER DEFAULT NULL","referral_earnings INTEGER DEFAULT 0"]:
        try: cursor.execute(f"ALTER TABLE users ADD COLUMN {col_def}")
        except: pass
    try: cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_referral_code ON users(referral_code)")
    except: pass
    # Default feature settings (preserve existing values when already configured).
    try:
        cursor.execute("SELECT value FROM settings WHERE key='referral_reward'")
        _old_ref_reward = cursor.fetchone()
        _old_ref_reward = _old_ref_reward[0] if _old_ref_reward else "10"
    except:
        _old_ref_reward = "10"
    defaults={
        "referral_enabled":"1",
        "referral_commission_enabled":"1", "referral_commission_percent":"5", "referral_min_purchase":"0",
        "first_purchase_bonus":"2",
        "spin_enabled":"1", "spin_probabilities":"0:50,5:30,10:15,20:5",
        "leaderboard_enabled":"1", "leaderboard_show_amount":"1", "leaderboard_title":"TOP SPENDERS",
    }
    for _k,_v in defaults.items():
        try:
            cursor.execute("SELECT value FROM settings WHERE key=?",(_k,))
            if cursor.fetchone() is None: cursor.execute("INSERT INTO settings(key,value) VALUES(?,?)",(_k,str(_v)))
        except: pass
    # Join bonus is permanently disabled; first completed product purchase pays ₹2 once.
    try:
        cursor.execute("INSERT INTO settings(key,value) VALUES('referral_join_bonus','0') ON CONFLICT(key) DO UPDATE SET value='0'")
        cursor.execute("INSERT INTO settings(key,value) VALUES('referral_reward','0') ON CONFLICT(key) DO UPDATE SET value='0'")
        cursor.execute("INSERT INTO settings(key,value) VALUES('first_purchase_bonus','2') ON CONFLICT(key) DO UPDATE SET value='2'")
    except Exception:
        pass
    # Reseller system: is_reseller marks who has reseller access at all,
    # reseller_banned lets admin temporarily suspend that access without
    # fully revoking the reseller role (for existing databases)
    try:
        cursor.execute("ALTER TABLE users ADD COLUMN is_reseller INTEGER DEFAULT 0")
    except:
        pass
    try:
        cursor.execute("ALTER TABLE users ADD COLUMN reseller_banned INTEGER DEFAULT 0")
    except:
        pass
    # Add extended product fields (for existing databases)
    for col_def in [
        "channel_link TEXT DEFAULT NULL",
        "description TEXT DEFAULT NULL",
        "description_entities TEXT DEFAULT NULL",
        "name_entities TEXT DEFAULT NULL",
        "device_type_entities TEXT DEFAULT NULL",
        "duration_label TEXT DEFAULT NULL",
        "duration_label_entities TEXT DEFAULT NULL",
        "compat_status TEXT DEFAULT NULL",
        "compat_status_entities TEXT DEFAULT NULL",
        "working_status TEXT DEFAULT 'active'",
        "device_type TEXT DEFAULT NULL",
        "video_file_id TEXT DEFAULT NULL",
        "emoji TEXT DEFAULT '📦'",
        "maintenance_enabled INTEGER DEFAULT 0",
        "maintenance_emoji TEXT DEFAULT '🔴'",
        "maintenance_desc TEXT DEFAULT NULL",
        "maintenance_desc_entities TEXT DEFAULT NULL",
    ]:
        try:
            cursor.execute(f"ALTER TABLE products ADD COLUMN {col_def}")
        except:
            pass
    # Track where the QR payment message was sent, so it can be deleted once
    # the payment is approved or rejected (for existing databases)
    for col_def in [
        "qr_chat_id INTEGER DEFAULT NULL",
        "qr_message_id INTEGER DEFAULT NULL",
        "zapupi_txn_id TEXT DEFAULT NULL",
        # Which gateway this top-up used: 'zapupi' or 'fampay'. Both can be
        # independently switched ON/OFF by the admin (Payment Gateway Settings).
        "gateway TEXT DEFAULT 'zapupi'",
        # FamPay's own order_id for this top-up (used to check payment status).
        "fampay_order_id TEXT DEFAULT NULL",
    ]:
        try:
            cursor.execute(f"ALTER TABLE pending_payments ADD COLUMN {col_def}")
        except:
            pass
    # Plans can now be in "days" or "hours" (for existing databases)
    try:
        cursor.execute("ALTER TABLE plans ADD COLUMN duration_unit TEXT DEFAULT 'days'")
    except:
        pass
    # Optional per-plan reseller price (separate from the normal customer
    # price). NULL means "no reseller price set -- use the normal price".
    try:
        cursor.execute("ALTER TABLE plans ADD COLUMN reseller_price INTEGER DEFAULT NULL")
    except:
        pass
    # KeysPanel Variant IDs belong to individual plans/durations, not the whole product.
    try:
        cursor.execute("ALTER TABLE plans ADD COLUMN keyspanels_variant_id TEXT DEFAULT NULL")
    except:
        pass
    # Product display order (for manual reordering) + video caption fields
    for col_def in [
        "sort_order INTEGER DEFAULT 0",
        "video_caption TEXT DEFAULT NULL",
        "video_caption_entities TEXT DEFAULT NULL",
        "requires_android_id INTEGER DEFAULT 0",
        "reseller_product_id TEXT DEFAULT NULL",
        "external_api_enabled INTEGER DEFAULT 0",
        "external_api_provider TEXT DEFAULT 'bunty'",
        "keyspanels_variant_id TEXT DEFAULT NULL",
        "premium_emoji_id TEXT DEFAULT NULL",
        "plan_emoji_id TEXT DEFAULT NULL",
    ]:
        try:
            cursor.execute(f"ALTER TABLE products ADD COLUMN {col_def}")
        except:
            pass
    for col_def in [
        "external_api_provider TEXT DEFAULT 'bunty'",
        "keyspanels_variant_id TEXT DEFAULT NULL",
    ]:
        try:
            cursor.execute(f"ALTER TABLE products ADD COLUMN {col_def}")
        except:
            pass

    # Give existing products a sort_order matching their current id order, and any
    # brand-new product a sort_order after the current highest, so nothing collides.
    try:
        cursor.execute("SELECT id FROM products WHERE sort_order=0 OR sort_order IS NULL ORDER BY id")
        zero_rows = cursor.fetchall()
        if zero_rows:
            cursor.execute("SELECT COALESCE(MAX(sort_order),0) FROM products")
            next_order = cursor.fetchone()[0]
            for row in zero_rows:
                next_order += 10
                cursor.execute("UPDATE products SET sort_order=? WHERE id=?", (next_order, row[0]))
    except:
        pass
    # Preserve an older product-level KeysPanel Variant ID by assigning it only to the first/shortest plan.
    try:
        cursor.execute("SELECT value FROM settings WHERE key='migrated_keyspanels_plan_variants_v1'")
        if not cursor.fetchone():
            for old_prod_id, old_variant in cursor.execute("SELECT id, keyspanels_variant_id FROM products WHERE keyspanels_variant_id IS NOT NULL AND TRIM(keyspanels_variant_id) != ''").fetchall():
                first_plan = cursor.execute("SELECT id FROM plans WHERE product_id=? ORDER BY (CASE WHEN duration_unit='hours' THEN days ELSE days*24 END), id LIMIT 1", (old_prod_id,)).fetchone()
                if first_plan:
                    cursor.execute("UPDATE plans SET keyspanels_variant_id=? WHERE id=? AND (keyspanels_variant_id IS NULL OR TRIM(keyspanels_variant_id)='')", (str(old_variant).strip(), first_plan[0]))
            cursor.execute("INSERT INTO settings(key,value) VALUES('migrated_keyspanels_plan_variants_v1','1') ON CONFLICT(key) DO UPDATE SET value='1'")
    except Exception as e:
        print(f"[DB MIGRATION] KeysPanel plan variants migration failed: {e}")
    # Repair legacy products: if a product has a Bunty PID but provider was left blank,
    # keep it on the Bunty path instead of accidentally treating it as local stock.
    try:
        cursor.execute("UPDATE products SET external_api_provider='bunty' WHERE reseller_product_id IS NOT NULL AND TRIM(reseller_product_id) != '' AND (external_api_provider IS NULL OR TRIM(external_api_provider)='')")
    except Exception as e:
        print(f"[DB MIGRATION] Legacy Bunty provider repair failed: {e}")

    # One-time migration: products that already had a Reseller Product ID set
    # (before the explicit ON/OFF toggle existed) keep auto-delivery working --
    # guarded by a settings flag so this never re-runs and can't undo an
    # admin's later manual "Toggle External API" choice.
    cursor.execute("SELECT value FROM settings WHERE key='migrated_external_api_enable_v1'")
    if not cursor.fetchone():
        try:
            cursor.execute("UPDATE products SET external_api_enabled=1 WHERE reseller_product_id IS NOT NULL AND TRIM(reseller_product_id) != ''")
        except:
            pass
        cursor.execute("INSERT INTO settings (key, value) VALUES ('migrated_external_api_enable_v1','1') ON CONFLICT(key) DO UPDATE SET value=excluded.value")
    conn.commit()
    cursor.close()
    conn.close()

# ======================= SETTINGS (QR CODE etc.) =======================
def get_setting(key):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM settings WHERE key=?", (key,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row[0] if row else None

def now_ist():
    """Current India Standard Time (Asia/Kolkata)."""
    return datetime.now(IST)

def ist_today_str():
    """Today's date according to India/Aligarh time, not the VPS timezone."""
    return now_ist().date().isoformat()

def format_ist_datetime(value, include_seconds=False):
    """Convert SQLite UTC timestamps / Unix timestamps to clean IST display.
    Naive SQLite CURRENT_TIMESTAMP values are treated as UTC."""
    if value is None or value == "":
        return "N/A"
    try:
        if isinstance(value, (int, float)):
            dt = datetime.fromtimestamp(float(value), tz=timezone.utc)
        elif isinstance(value, datetime):
            dt = value
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        else:
            raw = str(value).strip()
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(IST)
        fmt = "%d %b %Y, %I:%M:%S %p IST" if include_seconds else "%d %b %Y, %I:%M %p IST"
        return dt.strftime(fmt)
    except Exception:
        return str(value)

def first_purchase_bonus_amount():
    try:
        return max(0, int(get_setting("first_purchase_bonus") or "2"))
    except Exception:
        return 2

def apply_first_purchase_bonus(cur, user_id):
    """Give the buyer the fixed first-purchase bonus exactly once.
    Only call this after a product order is completed/delivered."""
    bonus = first_purchase_bonus_amount()
    if bonus <= 0:
        return 0
    cur.execute("SELECT COALESCE(first_purchase_bonus_claimed,0) FROM users WHERE id=?", (user_id,))
    row = cur.fetchone()
    if not row or int(row[0] or 0) == 1:
        return 0
    # Count completed product orders from both delivery paths.
    cur.execute("SELECT COUNT(*) FROM license_keys WHERE used_by=?", (user_id,))
    regular_count = int(cur.fetchone()[0] or 0)
    cur.execute("SELECT COUNT(*) FROM manual_orders WHERE user_id=? AND status='completed' AND license_key IS NOT NULL", (user_id,))
    manual_count = int(cur.fetchone()[0] or 0)
    if regular_count + manual_count < 1:
        return 0
    cur.execute("UPDATE users SET balance=balance+?, referral_earnings=COALESCE(referral_earnings,0), first_purchase_bonus_claimed=1 WHERE id=?", (bonus, user_id))
    cur.execute("INSERT INTO transactions(user_id,amount,type,details) VALUES(?,?,?,?)",
                (user_id, bonus, "first_purchase_bonus", "First product purchase bonus"))
    return bonus

def set_setting(key, value):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO settings (key, value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
    conn.commit()
    cursor.close()
    conn.close()

# ======================= PAYMENT GATEWAY CONFIG (admin editable + ON/OFF) =======================
# Both gateways read their live config from settings (admin-editable from
# Admin Panel -> Settings -> Payment Gateway Settings) instead of the hardcoded
# constants above, and each has its own independent ON/OFF switch.
def _mask_key(value):
    """Safely display a payment/API secret in the admin UI without exposing it.
    Always returns a short string, so the settings screen cannot fail because
    the key is missing or unexpectedly short.
    """
    value = str(value or "").strip()
    if not value:
        return "❌ Not set"
    if len(value) <= 8:
        return "••••"
    return html.escape(value[:4] + "••••" + value[-4:])

def get_zapupi_key():
    return get_setting("zapupi_zap_key") or ZAPUPI_ZAP_KEY

def is_zapupi_enabled():
    # ON by default (matches current behaviour) until the admin explicitly
    # switches it OFF, so nothing changes for existing setups after this update.
    return get_setting("zapupi_enabled") != "0"

def get_fampay_api_key():
    return get_setting("fampay_api_key") or ""

def get_fampay_upi_id():
    return get_setting("fampay_upi_id") or ""

def is_fampay_enabled():
    # OFF by default until the admin sets its API Key + UPI ID and turns it ON.
    return get_setting("fampay_enabled") == "1"

# ======================= USD DISPLAY (live rate, admin ON/OFF) =======================
# When admin turns this ON, every place that shows an INR amount (Add Balance
# keypad, plan/product prices, wallet balance) also shows the live USD
# equivalent next to it, e.g. "₹100 (~$1.13)". Turning it OFF removes the $
# part everywhere and it goes back to showing plain ₹ like before. The rate
# itself is always fetched live from the internet (no manual admin rate) and
# cached for an hour so we're not hitting the rate API on every single tap.
_usd_rate_cache = {"rate": None, "fetched_at": 0}

def is_usd_display_on():
    return get_setting("usd_display_enabled") == "1"

def get_usd_inr_rate():
    """Returns the current USD->INR rate (float), using a 1-hour in-memory
    cache. Falls back to the last known-good rate (kept in the settings table
    too, so it survives a bot restart) if the live fetch fails, and finally to
    a rough hardcoded fallback if nothing else is available."""
    now = time.time()
    if _usd_rate_cache["rate"] and (now - _usd_rate_cache["fetched_at"]) < 3600:
        return _usd_rate_cache["rate"]
    try:
        req = urllib.request.Request("https://open.er-api.com/v6/latest/USD", headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="ignore"))
        rate = float(data["rates"]["INR"])
        _usd_rate_cache["rate"] = rate
        _usd_rate_cache["fetched_at"] = now
        set_setting("usd_inr_rate_last_known", str(rate))
        return rate
    except Exception as e:
        print("USD rate fetch failed, using last known/fallback:", e)
        last_known = get_setting("usd_inr_rate_last_known")
        if last_known:
            try:
                rate = float(last_known)
                _usd_rate_cache["rate"] = rate
                _usd_rate_cache["fetched_at"] = now
                return rate
            except (TypeError, ValueError):
                pass
        return 90.0  # rough fallback so the bot never crashes if the API + cache both fail

def usd_hint(inr_amount):
    """Returns ' (~$X.XX)' for an INR amount when USD display is ON, or ''
    when it's OFF -- just tack this onto the end of any '₹{amount}' string."""
    if not is_usd_display_on():
        return ""
    try:
        rate = get_usd_inr_rate()
        if not rate:
            return ""
        usd = float(inr_amount) / rate
        return f" (~${usd:,.2f})"
    except Exception:
        return ""

# ======================= MAINTENANCE MODE =======================
def is_maintenance_on():
    return get_setting("maintenance_mode") == "1"

# ======================= EXTERNAL RESELLER API (single API) =======================
# The RESELLER_API_URL / RESELLER_API_KEY / RESELLER_MASTER_KEY constants above act as
# the DEFAULT values. Admin can override any of them live from Settings -> 🔌 External
# API Settings without touching code/redeploying -- these getters check the settings
# table first and fall back to the hardcoded constant if nothing's been set there yet.
def get_reseller_api_url():
    return get_setting("reseller_api_url") or RESELLER_API_URL

def get_reseller_api_key():
    return get_setting("reseller_api_key") or RESELLER_API_KEY

def get_reseller_master_key():
    return get_setting("reseller_master_key") or RESELLER_MASTER_KEY

def get_keyspanels_api_url():
    return get_setting("keyspanels_api_url") or KEYSPANELS_API_URL

def get_keyspanels_request_url():
    """Return the exact KeysPanelShop reseller_api.php endpoint.
    Admin may save either the full endpoint or the host/base URL.
    """
    url = (get_keyspanels_api_url() or "").strip().rstrip("/")
    if not url:
        return ""
    if url.lower().endswith("/reseller_api.php"):
        return url
    return url + "/reseller_api.php"

def get_keyspanels_master_key():
    return get_setting("keyspanels_master_key") or KEYSPANELS_MASTER_KEY

def get_external_api_provider(value):
    value = str(value or "bunty").strip().lower()
    return "keyspanels" if value in ("keyspanels", "keys_panel", "keys-panel", "keys") else "bunty"

# ======================= PREMIUM EMOJI SYSTEM =======================
# Every emoji used anywhere in the bot's user-facing messages is registered here
# as a named "slot": (default_plain_emoji, human_readable_label, screen/category).
# Admin can override ANY slot with a Telegram Premium custom emoji via
# Settings -> 🎨 Premium Emoji Manager. Until overridden, the plain default is used
# (so the bot always works even with zero custom emojis set).
EMOJI_SLOTS = {
    # ---- Main Menu ----
    "menu_title_l":    ("🏪", "Title icon (left)", "Main Menu"),
    "menu_title_r":    ("🏪", "Title icon (right)", "Main Menu"),
    "menu_wave":       ("🎉", "Welcome wave", "Main Menu"),
    "menu_high_l":     ("⭐", "Highlights icon (left)", "Main Menu"),
    "menu_high_r":     ("⭐", "Highlights icon (right)", "Main Menu"),
    "menu_key":        ("🔑", "Premium Game Keys", "Main Menu"),
    "menu_instant":    ("⚡", "Instant Delivery 24/7", "Main Menu"),
    "menu_secure":     ("🔒", "100% Secure Payment", "Main Menu"),
    "menu_price":      ("💸", "Best Prices Guaranteed", "Main Menu"),
    "menu_safe":       ("🔥", "Safe And Trusted", "Main Menu"),
    "menu_rocket1":    ("🚀", "Safe And Trusted — rocket", "Main Menu"),
    "menu_support":    ("📞", "Professional Support", "Main Menu"),
    "menu_userid":     ("👤", "User ID icon", "Main Menu"),
    "menu_wallet":     ("💰", "Wallet Balance icon", "Main Menu"),
    "menu_rocket2":    ("🚀", "Tap Shop Now — rocket", "Main Menu"),
    "menu_catalog":    ("🏪", "Wide product catalog icon", "Main Menu"),
    "menu_delivery":   ("🔥", "Instant key delivery icon", "Main Menu"),
    "menu_gateways":   ("💰", "Multiple payment gateways icon", "Main Menu"),
    "menu_adminsupport": ("🔒", "24/7 admin support icon", "Main Menu"),
    # ---- Store Highlights box (Main Menu) ----
    "highlight_top_l":  ("🔝", "TOP badge icon (left)", "Store Highlights"),
    "highlight_top_r":  ("🔝", "TOP badge icon (right)", "Store Highlights"),
    "highlight_keys":   ("💯", "Premium Keys / Instant Delivery icon", "Store Highlights"),
    "highlight_secure": ("🛡️", "Secure Payment icon", "Store Highlights"),
    "highlight_price":  ("💸", "Best Prices icon", "Store Highlights"),
    "highlight_trust":  ("✅", "Safe And Trusted icon", "Store Highlights"),
    "highlight_rocket": ("🚀", "Rocket icon next to Safe And Trusted", "Store Highlights"),
    "highlight_support": ("📞", "Professional Support icon", "Store Highlights"),
    # ---- Profile ----
    "profile_title_l": ("👤", "Title icon (left)", "Profile"),
    "profile_title_r": ("👤", "Title icon (right)", "Profile"),
    "profile_id":      ("▶️", "User ID icon", "Profile"),
    "profile_name":    ("▶️", "Name icon", "Profile"),
    "profile_phone":   ("▶️", "Phone icon", "Profile"),
    "profile_account": ("🤖", "Account type icon", "Profile"),
    "profile_user":    ("👤", "Username icon", "Profile"),
    "profile_bal_l":   ("💰", "Balance section icon", "Profile"),
    "profile_wallet":  ("🧾", "Current balance icon", "Profile"),
    "profile_stats":   ("📊", "Statistics section icon", "Profile"),
    "profile_orders":  ("🎯", "Orders icon", "Profile"),
    "profile_spent":   ("💸", "Spent icon", "Profile"),
    "profile_joined":  ("🕐", "Joined date icon", "Profile"),
    # ---- Tutorial ----
    "tut_step1":       ("🎤", "Step 1 icon", "Tutorial"),
    "tut_step2":       ("🛒", "Step 2 icon", "Tutorial"),
    "tut_step3":       ("🛍️", "Step 3 icon", "Tutorial"),
    "tut_step4":       ("🔑", "Step 4 icon", "Tutorial"),
    "tut_step5":       ("📹", "Step 5 icon", "Tutorial"),
    "tut_warn":        ("⚠️", "Problem icon", "Tutorial"),
    "tut_owner":       ("👤", "Owner icon", "Tutorial"),
    "tut_fast":        ("🔥", "Fast reply icon", "Tutorial"),
    # ---- History ----
    "hist_title":      ("📜", "Title icon", "History"),
    "hist_product":    ("📦", "Product icon", "History"),
    "hist_key":        ("🔑", "Key icon", "History"),
    "hist_date":       ("📅", "Date icon", "History"),
    # ---- Add Balance ----
    "addbal_title":    ("💰", "Title icon", "Add Balance"),
    "addbal_limit":    ("💵", "Min/Max limit icon", "Add Balance"),
    "addbal_arrow":    ("👇", "Pointer icon", "Add Balance"),
    "addbal_method_upi":     ("💵", "UPI method icon (in text)", "Add Balance"),
    "addbal_method_fampay":  ("💵", "FamPay method icon (in text)", "Add Balance"),
    "addbal_choice_title":   ("💰", "ADD BALANCE header icon", "Add Balance"),
    "addbal_choice_upi_icon":    ("💸", "UPI Pay line icon", "Add Balance"),
    "addbal_choice_fampay_icon": ("💳", "FamPay line icon", "Add Balance"),
    "addbal_choice_arrow":   ("➡️", "Payment Method Chuno arrow icon", "Add Balance"),
    "addbal_choice_footer":  ("💰", "Payment Method Chuno trailing icon", "Add Balance"),
    # ---- QR Payment Message ----
    "qr_pay":          ("💳", "Pay amount icon", "QR Payment"),
    "qr_order":        ("🧾", "Order ID icon", "QR Payment"),
    "qr_step1":        ("1️⃣", "Step 1 icon", "QR Payment"),
    "qr_step2":        ("2️⃣", "Step 2 icon", "QR Payment"),
    "qr_step3":        ("3️⃣", "Step 3 icon", "QR Payment"),
    "qr_step4":        ("4️⃣", "Step 4 icon", "QR Payment"),
    "qr_warn":         ("⚠️", "Warning icon", "QR Payment"),
    # ---- Purchase Success ----
    "buy_success":     ("✅", "Success icon", "Purchase"),
    "buy_product":     ("📦", "Product icon", "Purchase"),
    "buy_key":         ("🔑", "Key icon", "Purchase"),
    "buy_balance":     ("💰", "New balance icon", "Purchase"),
    # ---- Key Delivery (Android ID / Reseller API flow) ----
    "deliver_success":  ("✅", "Key Generated Successfully title", "Key Delivery"),
    "deliver_product":  ("🎮", "Product icon", "Key Delivery"),
    "deliver_type":     ("🏷️", "Type icon", "Key Delivery"),
    "deliver_regular":  ("🔵", "Regular type dot", "Key Delivery"),
    "deliver_duration": ("⏱️", "Duration icon", "Key Delivery"),
    "deliver_price":    ("💰", "Price icon", "Key Delivery"),
    "deliver_key":      ("🔑", "Key icon", "Key Delivery"),
    "deliver_expires":  ("📅", "Expires icon", "Key Delivery"),
    "deliver_androidid": ("📱", "Android ID icon", "Key Delivery"),
    "deliver_enjoy":    ("⭐", "Enjoy footer icon", "Key Delivery"),
    # ---- Manual Order Pending (Android ID flow, before admin sends key) ----
    "pending_title":    ("⏳", "Order Received title", "Manual Order"),
    "pending_product":  ("🎮", "Product icon", "Manual Order"),
    "pending_duration": ("⏱️", "Duration icon", "Manual Order"),
    "pending_price":    ("💰", "Price icon", "Manual Order"),
    "pending_androidid": ("📱", "Android ID icon", "Manual Order"),
    "reject_title":     ("❌", "Order rejected message icon (to user)", "Manual Order"),
    # ---- Insufficient Balance ----
    "insuff_title":     ("❌", "Title icon", "Insufficient Balance"),
    "insuff_need":      ("💰", "Required amount icon", "Insufficient Balance"),
    "insuff_have":      ("💵", "Current balance icon", "Insufficient Balance"),
    # ---- My Orders (detail card) ----
    "myorders_key":      ("🔑", "Key icon", "My Orders"),
    "myorders_purchase": ("🕐", "Purchase date icon", "My Orders"),
    "history_category": ("📦", "Category button icon", "My Orders"),
    # ---- Referral / Daily Spin ----
    "referral_title": ("🎁", "Referral title icon", "Referral"),
    "referral_link": ("🔗", "Referral link icon", "Referral"),
    "referral_invited": ("👥", "Invited users icon", "Referral"),
    "referral_bonus": ("🎉", "Join bonus icon", "Referral"),
    "referral_earnings": ("💰", "Referral earnings icon", "Referral"),
    "referral_commission": ("💸", "Purchase commission icon", "Referral"),
    "spin_title": ("🎡", "Daily Spin title icon", "Daily Spin"),
    "spin_reward": ("🎯", "Spin reward/probability icon", "Daily Spin"),
    "spin_timer": ("⏰", "Spin frequency icon", "Daily Spin"),
    "spin_result": ("🎉", "Spin result icon", "Daily Spin"),
    # Each animation frame can be replaced with a Telegram Premium custom
    # emoji from the Premium Emoji Manager. Admin can set the same animated
    # emoji on all five slots, or use different custom emojis per frame.
    "spin_anim_1": ("🎡", "Spin animation frame 1", "Daily Spin Animation"),
    "spin_anim_2": ("🔄", "Spin animation frame 2", "Daily Spin Animation"),
    "spin_anim_3": ("🎰", "Spin animation frame 3", "Daily Spin Animation"),
    "spin_anim_4": ("✨", "Spin animation frame 4", "Daily Spin Animation"),
    "spin_anim_5": ("🎉", "Spin animation frame 5", "Daily Spin Animation"),
    # ---- Leaderboard ----
    "leaderboard_title": ("🏆", "Leaderboard title icon", "Leaderboard"),
    "leaderboard_top1": ("🥇", "Rank 1 icon", "Leaderboard"),
    "leaderboard_top2": ("🥈", "Rank 2 icon", "Leaderboard"),
    "leaderboard_top3": ("🥉", "Rank 3 icon", "Leaderboard"),
    "leaderboard_row": ("🏅", "Other rank icon", "Leaderboard"),
    "leaderboard_spent": ("💸", "Spent amount icon", "Leaderboard"),
    "leaderboard_empty": ("📭", "Empty leaderboard icon", "Leaderboard"),
    # ---- Profile status labels ----
    "profile_status_active": ("✅", "Active status icon", "Profile"),
    "profile_status_banned": ("🚫", "Banned status icon", "Profile"),
    "profile_role_admin":    ("👑", "Admin role icon", "Profile"),
    "profile_role_regular":  ("🔵", "Regular role icon", "Profile"),
    "profile_reseller_active": ("🟢", "Reseller active icon", "Profile"),
    "profile_reseller_banned": ("🚫", "Reseller banned icon", "Profile"),
    # ---- Become a Reseller ----
    "reseller_become_title": ("🏷️", "Become a Reseller title", "Reseller"),
    "reseller_price":        ("💰", "Price icon", "Reseller"),
    "reseller_balance":      ("💵", "Balance icon", "Reseller"),
    "reseller_activated_title": ("🎉", "Reseller Activated title", "Reseller"),
    "reseller_new_balance":  ("💵", "New balance icon", "Reseller"),
    # ---- Payment Received (Add Balance auto-credit, shown to user) ----
    "paysucc_title":   ("✅", "Payment Received title", "Payment Received"),
    "paysucc_amount":  ("💰", "Amount added icon", "Payment Received"),
    "paysucc_balance": ("🧾", "New balance icon", "Payment Received"),
    # ---- Support ----
    "support_icon":    ("🛟", "Support icon", "Support"),
    # ---- Admin: New Order notification ----
    "order_title_l":   ("🛒", "Title icon (left)", "Admin New Order"),
    "order_title_r":   ("🛒", "Title icon (right)", "Admin New Order"),
    "order_name":      ("👤", "Name icon", "Admin New Order"),
    "order_userid":    ("🆔", "User ID icon", "Admin New Order"),
    "order_phone":     ("🤖", "Phone icon", "Admin New Order"),
    "order_username":  ("👤", "Username icon", "Admin New Order"),
    "order_product":   ("🎮", "Product icon", "Admin New Order"),
    "order_key":       ("💉", "Key icon", "Admin New Order"),
    "order_amount":    ("💰", "Amount icon", "Admin New Order"),
    "order_balance":   ("📕", "Remaining balance icon", "Admin New Order"),
    "order_time":      ("🕐", "Time icon", "Admin New Order"),
    # ---- Admin: New Deposit notification ----
    "deposit_title":   ("💰", "Title icon", "Admin New Deposit"),
    "deposit_name":    ("👤", "Name icon", "Admin New Deposit"),
    "deposit_userid":  ("🆔", "User ID icon", "Admin New Deposit"),
    "deposit_phone":   ("🤖", "Phone icon", "Admin New Deposit"),
    "deposit_username": ("👤", "Username icon", "Admin New Deposit"),
    "deposit_amount":  ("💰", "Amount icon", "Admin New Deposit"),
    "deposit_bonus":   ("🎁", "Bonus icon", "Admin New Deposit"),
    "deposit_balance": ("📕", "New balance icon", "Admin New Deposit"),
    "deposit_time":    ("🕐", "Time icon", "Admin New Deposit"),

    # ---- Buttons (icon_custom_emoji_id + color) ----
    "btn_support": ('📞', 'Contact Support', 'Buttons - Main Menu'),
    "btn_shop_now": ('🛍️', 'Shop', 'Buttons - Main Menu'),
    "btn_amount_digit": ('💰', 'Custom amount keypad number buttons (0-9)', 'Buttons - Other'),
    "btn_amount_delete": ('❌', 'Custom amount keypad Delete button', 'Buttons - Other'),
    "btn_amount_confirm": ('✅', 'Custom amount keypad Confirm button', 'Buttons - Other'),
    "btn_amount_back": ('🔙', 'Custom amount keypad Back button', 'Buttons - Other'),
    "btn_gateway_upi": ('💵', 'Add Balance - UPI method button', 'Buttons - Other'),
    "btn_addbal_choice_zapupi": ('🏦', 'Add Balance: choose ZapUPI button', 'Add Balance'),
    "btn_addbal_choice_fampay": ('💵', 'Add Balance: choose FamPay button', 'Add Balance'),
    "btn_product_maintenance": ('🔴', 'Product button icon when under maintenance', 'Buttons - Main Menu'),
    "btn_notifyme": ('🔔', 'Notify Me (maintenance product page)', 'Buttons - Main Menu'),
    "btn_add_balance": ('💰', 'Add Balance', 'Buttons - Main Menu'),
    "btn_history": ('📜', 'My Orders', 'Buttons - Main Menu'),
    "btn_referral": ('🎁', 'Referral', 'Buttons - Main Menu'),
    "btn_daily_spin": ('🎡', 'Daily Spin', 'Buttons - Main Menu'),
    "btn_profile": ('👤', 'My Profile', 'Buttons - Main Menu'),
    "btn_tutorial": ('🎬', 'How To Use', 'Buttons - Main Menu'),
    "btn_become_reseller": ('🏷️', 'Become a Reseller', 'Buttons - Main Menu'),
    "btn_link_selling_proof": ('🏆', 'Selling Proof', 'Buttons - Links'),
    "btn_link_leaderboard": ('🏆', 'Leaderboard', 'Buttons - Links'),
    "btn_admin_panel": ('🔧', 'Admin Panel', 'Buttons - Main Menu'),
    "btn_admin_cat_product": ('📦', 'Product Management', 'Buttons - Admin Panel'),
    "btn_admin_cat_keys": ('🔑', 'Keys Management', 'Buttons - Admin Panel'),
    "btn_admin_cat_user": ('👤', 'User Management', 'Buttons - Admin Panel'),
    "btn_admin_cat_reseller": ('🏷️', 'Reseller Management', 'Buttons - Admin Panel'),
    "btn_admin_cat_settings": ('⚙️', 'Settings', 'Buttons - Admin Panel'),
    "btn_admin_announcement": ('📢', 'Announcement', 'Buttons - Admin Panel'),
    "announce_1h":   ('🕐', '1 Hour option icon', 'Announcement'),
    "announce_2h":   ('🕑', '2 Hours option icon', 'Announcement'),
    "announce_3h":   ('🕒', '3 Hours option icon', 'Announcement'),
    "announce_perm": ('♾️', 'Permanent option icon', 'Announcement'),
    "btn_back_main": ('◀️', 'Back to Main', 'Buttons - Main Menu'),
    "btn_admin_reseller_toggle": ('🔁', 'Reseller ON/OFF', 'Buttons - Admin Panel'),
    "btn_admin_reseller_setprice": ('💰', 'Set Reseller Price', 'Buttons - Admin Panel'),
    "btn_admin_reseller_buy_menu": ('🛒', 'Reseller Buy Settings', 'Buttons - Admin Panel'),
    "btn_admin_reseller_list": ('📋', 'View Resellers', 'Buttons - Admin Panel'),
    "btn_admin_reseller_ban": ('🚫', 'Ban Reseller', 'Buttons - Admin Panel'),
    "btn_admin_reseller_unban": ('✅', 'Unban Reseller', 'Buttons - Admin Panel'),
    "btn_admin_add_product": ('➕', 'Add Product', 'Buttons - Admin Panel'),
    "btn_admin_edit_product_all": ('✏️', 'Edit Product', 'Buttons - Admin Panel'),
    "btn_admin_edit_product_plans": ('💰', 'Edit Product Plans/Price', 'Buttons - Admin Panel'),
    "btn_admin_del_product": ('🗑️', 'Delete Product', 'Buttons - Admin Panel'),
    "btn_admin_add_plan": ('📅', 'Add Plan', 'Buttons - Admin Panel'),
    "btn_admin_del_plan": ('🗑️', 'Delete Plan', 'Buttons - Admin Panel'),
    "btn_admin_maintenance_list": ('🛠️', 'Maintenance Mode', 'Buttons - Admin Panel'),
    "btn_admin_add_keys": ('🔑', 'Add Keys', 'Buttons - Admin Panel'),
    "btn_admin_list_keys": ('📋', 'List Keys', 'Buttons - Admin Panel'),
    "btn_admin_expired_keys": ('⌛', 'Expired/Used Keys', 'Buttons - Admin Panel'),
    "btn_admin_stock_overview": ('📊', 'Stock Overview', 'Buttons - Admin Panel'),
    "btn_admin_del_key": ('❌', 'Delete Key', 'Buttons - Admin Panel'),
    "btn_admin_add_balance": ('💰', 'Add Balance', 'Buttons - Admin Panel'),
    "btn_admin_remove_balance": ('💰', 'Remove Balance', 'Buttons - Admin Panel'),
    "btn_admin_ban_user": ('🚫', 'Ban User', 'Buttons - Admin Panel'),
    "btn_admin_unban_user": ('✅', 'Unban User', 'Buttons - Admin Panel'),
    "btn_admin_user_details": ('🔍', 'User Details', 'Buttons - Admin Panel'),
    "btn_admin_all_users_0": ('👥', 'All Users', 'Buttons - Admin Panel'),
    "btn_admin_pending_payments": ('💳', 'Pending Payments', 'Buttons - Admin Panel'),
    "btn_admin_set_selling_proof": ('🏆', 'Set Selling Proof Link', 'Buttons - Admin Panel'),
    "btn_admin_leaderboard_settings": ('🏆', 'Leaderboard Settings', 'Buttons - Admin Panel'),
    "btn_admin_set_updatechannel": ('📢', 'Set Update Channel Link', 'Buttons - Admin Panel'),
    "btn_admin_set_tutorial_video": ('🎥', 'Set Tutorial Video Link', 'Buttons - Admin Panel'),
    "btn_admin_custom_button_menu": ('🔘', 'Custom Menu Button', 'Buttons - Admin Panel'),
    "btn_admin_emoji_manager": ('🎨', 'Premium Emoji Manager', 'Buttons - Admin Panel'),
    "btn_admin_referral_settings": ('🎁', 'Referral Settings', 'Buttons - Admin Panel'),
    "btn_admin_spin_settings": ('🎡', 'Spin Settings', 'Buttons - Admin Panel'),
    "btn_admin_set_order_notify": ('📝', 'Set Order Notify Text', 'Buttons - Admin Panel'),
    "btn_admin_set_support_text": ('📝', 'Set Support Text', 'Buttons - Admin Panel'),
    "btn_admin_set_highlights": ('📝', 'Set Store Highlights Text', 'Buttons - Admin Panel'),
    "btn_admin_external_api_settings": ('🔌', 'External API Settings', 'Buttons - Admin Panel'),
    "btn_admin_payment_gateways": ('💳', 'Payment Gateway Settings', 'Buttons - Admin Panel'),
    "btn_admin_zapupi_toggle": ('🏦', 'ZapUPI ON/OFF', 'Buttons - Admin Panel'),
    "btn_admin_set_zapupi_key": ('🔑', 'Set ZapUPI Zap Key', 'Buttons - Admin Panel'),
    "btn_admin_fampay_toggle": ('💵', 'FamPay ON/OFF', 'Buttons - Admin Panel'),
    "btn_admin_set_fampay_key": ('🔑', 'Set FamPay API Key', 'Buttons - Admin Panel'),
    "btn_admin_set_fampay_upi": ('🏦', 'Set FamPay UPI ID', 'Buttons - Admin Panel'),
    "btn_admin_fampay_debug": ('🐞', 'FamPay Debug (raw API response)', 'Buttons - Admin Panel'),
    "btn_admin_backup_now": ('📦', 'Backup Database Now', 'Buttons - Admin Panel'),
    "btn_admin_stats": ('📊', 'Stats', 'Buttons - Admin Panel'),
    "btn_admin_maintenance_toggle": ('🛠', 'Maintenance ON bot band hai', 'Buttons - Admin Panel'),
    "btn_admin_usd_toggle": ('💲', 'USD Display ON/OFF toggle', 'Buttons - Admin Panel'),
    "btn_admin_set_external_api_url": ('🌐', 'Set API Endpoint URL', 'Buttons - Admin Panel'),
    "btn_admin_set_external_api_key": ('🔑', 'Set API Key', 'Buttons - Admin Panel'),
    "btn_admin_set_external_api_masterkey": ('🔒', 'Set Master Key', 'Buttons - Admin Panel'),
    "btn_zapcheck": ('🔄', 'Check Payment Status', 'Buttons - Other'),
    "btn_link_watch_tutorial_video": ('🎥', 'Watch Tutorial Video', 'Buttons - Links'),
    "btn_link_chat_on_telegram": ('💬', 'Chat on Telegram', 'Buttons - Links'),
    "btn_demo_buy": ('🛒', 'Buy Now - ₹499', 'Buttons - Other'),
    "btn_addkeys_plan": ('📆', 'format_durationpl1 pl3 - ₹pl2', 'Buttons - Other'),
    "btn_prodpick_hub": ('❌', 'Cancel', 'Buttons - Admin Panel'),
    "btn_emojireset": ('♻️', 'Reset to Default', 'Buttons - Admin Panel'),
    "btn_emojicat": ('◀️', 'Back', 'Buttons - Admin Panel'),
    "btn_resellerprice_prod": ('❌', 'Cancel', 'Buttons - Other'),
    "btn_editplanprice": ('✏️', 'format_durationpl1 pl3 - ₹pl2', 'Buttons - Other'),
    "btn_link_back": ('◀️', 'Back', 'Buttons - Links'),
    "btn_reorderup": ('⬆️', 'Move Up', 'Buttons - Other'),
    "btn_reorderdown": ('⬇️', 'Move Down', 'Buttons - Other'),
    "btn_hubfield_name": ('🔘', 'Name', 'Buttons - Admin Panel'),
    "btn_hubfield_compat_status": ('🔧', 'Compatibility Root/Non-Root', 'Buttons - Admin Panel'),
    "btn_hubfield_description": ('🔘', 'Description', 'Buttons - Admin Panel'),
    "btn_hubfield_device_type": ('🔘', 'Device Type', 'Buttons - Admin Panel'),
    "btn_hubfield_duration_label": ('🗓️', 'Duration Label', 'Buttons - Admin Panel'),
    "btn_hubfield_channel_link": ('🔗', 'Channel Link', 'Buttons - Admin Panel'),
    "btn_hubvideo": ('🎬', 'Video', 'Buttons - Admin Panel'),
    "btn_hubfield_video_caption": ('📝', 'Video Caption', 'Buttons - Admin Panel'),
    "btn_hubplans": ('💰', 'Edit Plan Prices', 'Buttons - Admin Panel'),
    "btn_hubtoggle": ('🔁', 'Toggle Active/Inactive', 'Buttons - Admin Panel'),
    "btn_hubaidtoggle": ('📱', 'Toggle Android ID Flow', 'Buttons - Admin Panel'),
    "btn_hubextapitoggle": ('🔌', 'Toggle External API', 'Buttons - Admin Panel'),
    "btn_hubfield_reseller_product_id": ('🆔', 'Set External API PID', 'Buttons - Admin Panel'),
    "btn_hubreorder": ('⬆️⬇️', 'Reorder', 'Buttons - Admin Panel'),
    "btn_hubmainttoggle": ('🛠️', 'Toggle Maintenance ON/OFF', 'Buttons - Admin Panel'),
    "btn_hubfield_maintenance_emoji": ('🔴', 'Set Maintenance Emoji', 'Buttons - Admin Panel'),
    "btn_hubfield_maintenance_desc": ('📝', 'Set Maintenance Description', 'Buttons - Admin Panel'),
    "btn_reseller_buy_confirm": ('✅', 'Buy Reseller - ₹price', 'Buttons - Other'),
    "btn_watchvid": ('🎬', 'Watch Demo', 'Buttons - Other'),
    "btn_sendkey": ('✍️', 'Send Key', 'Buttons - Other'),
    "btn_rejectorder": ('❌', 'Reject & Refund', 'Buttons - Other'),
    "btn_link_join_updates": ('📢', 'Join prod_name Updates', 'Buttons - Links'),
    "btn_link_pay_now": ('💳', 'Pay Now', 'Buttons - Links'),
    "btn_paycancel": ('❌', 'Cancel Payment', 'Buttons - Other'),
    "btn_editprod": ('✏️', 'strip_custom_emojip1 p2', 'Buttons - Other'),
    "btn_delprod": ('🗑️', 'strip_custom_emojip1 p2', 'Buttons - Other'),
    "btn_delplan": ('🗑️', 'pl0 - format_durationpl2 pl4 ₹pl3', 'Buttons - Other'),
    "btn_addkeys_prod": ('🔑', 'strip_custom_emojip1 p2', 'Buttons - Other'),
    "btn_approve_pay": ('✅', 'Force Approve r2', 'Buttons - Other'),
    "btn_reject_pay": ('❌', 'Reject r2', 'Buttons - Other'),
    "btn_admin_set_custom_btn_text": ('✏️', 'Set Button Text', 'Buttons - Admin Panel'),
    "btn_admin_set_custom_btn_link": ('🔗', 'Set Button Link', 'Buttons - Admin Panel'),
    "btn_admin_toggle_custom_btn": ('🔁', 'Toggle ON/OFF', 'Buttons - Admin Panel'),
    "btn_prodpick": ('{', 'status_dot strip_custom_emojip1 p3', 'Buttons - Other'),
    "btn_mainttoggle": ('{', 'status_dot strip_custom_emojip1 p4', 'Buttons - Other'),
    "btn_emojislot": ('{', 'dot default label', 'Buttons - Admin Panel'),
    "btn_admin_all_users": ('⬅️', 'Prev', 'Buttons - Admin Panel'),
    "btn_admin_reseller_buy_setprice": ('💰', 'Set Price', 'Buttons - Admin Panel'),
    "btn_admin_reseller_buy_setterms": ('📝', 'Set Terms Text', 'Buttons - Admin Panel'),
    "btn_admin_reseller_buy_setsuccessmsg": ('🎉', 'Set Success Message', 'Buttons - Admin Panel'),
    "btn_admin_reseller_buy_toggle": ('🔁', 'Toggle ON/OFF', 'Buttons - Admin Panel'),
    "btn_resellerprice_plan": ('💰', 'format_durationpl1 pl3 - Normal ₹pl2 Res', 'Buttons - Other'),
    "btn_custom_admin_button": ('🔘', 'Custom admin button textlink both admin-', 'Buttons - Admin Panel'),
    "btn_cancel_product_detail": ('❌', 'Cancel', 'Buttons - Admin Panel'),
}

def emo(slot_key):
    """Return the HTML for an emoji slot: the admin-set Telegram Premium custom
    emoji (rendered via <tg-emoji>) if one has been assigned, otherwise the plain
    default emoji. Safe to call even if the slot has never been customized."""
    default = EMOJI_SLOTS.get(slot_key, ("❓", slot_key, "Other"))[0]
    custom_id = get_setting(f"emoji_slot_{slot_key}")
    if custom_id:
        return f'<tg-emoji emoji-id="{custom_id}">{default}</tg-emoji>'
    return default

def btn_emo(slot_key):
    """Return the custom Premium emoji ID to use as a BUTTON's icon
    (icon_custom_emoji_id) -- managed from the same "🎨 Premium Emoji Manager"
    admin screen as the text emoji slots above (look under the "Buttons - ..."
    categories). Falls back to DEFAULT_BUTTON_EMOJI_ID until the admin sets a
    custom one for that specific button."""
    custom_id = get_setting(f"emoji_slot_btn_{slot_key}")
    return custom_id if custom_id else DEFAULT_BUTTON_EMOJI_ID

def is_notify_subscribed(product_id, user_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM product_notify_requests WHERE product_id=? AND user_id=?", (product_id, user_id))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return bool(row)

def toggle_notify_subscription(product_id, user_id):
    """Adds the user to the notify list for this product, or removes them if
    they were already on it (tapping the button again cancels it). Returns
    True if the user is now subscribed, False if they were just removed."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM product_notify_requests WHERE product_id=? AND user_id=?", (product_id, user_id))
    row = cursor.fetchone()
    if row:
        cursor.execute("DELETE FROM product_notify_requests WHERE product_id=? AND user_id=?", (product_id, user_id))
        conn.commit()
        cursor.close()
        conn.close()
        return False
    else:
        cursor.execute("INSERT OR IGNORE INTO product_notify_requests (product_id, user_id) VALUES (?,?)", (product_id, user_id))
        conn.commit()
        cursor.close()
        conn.close()
        return True

def get_notify_subscribers(product_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM product_notify_requests WHERE product_id=?", (product_id,))
    rows = [r[0] for r in cursor.fetchall()]
    cursor.close()
    conn.close()
    return rows

def clear_notify_subscribers(product_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM product_notify_requests WHERE product_id=?", (product_id,))
    conn.commit()
    cursor.close()
    conn.close()

def send_notify_broadcast(prod_id, admin_chat_id, custom_text=None, custom_entities=None):
    """Sends the 'product is back' message to everyone who tapped Notify Me on
    this product, then clears the list. custom_text/custom_entities let the
    admin write their own message (Premium emoji, bold, links, etc. preserved,
    same as the other admin text templates in this bot); if omitted, a default
    message is sent instead."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT name, name_entities FROM products WHERE id=?", (prod_id,))
    prow = cursor.fetchone()
    cursor.close()
    conn.close()
    if not prow:
        bot.send_message(admin_chat_id, "⚠️ Product not found, broadcast cancel kar diya gaya.")
        return
    name_html = render_text_with_custom_emoji(prow[0], prow[1])
    if custom_text:
        text = render_text_with_custom_emoji(custom_text, custom_entities)
    else:
        text = f"✅ <b>{name_html}</b> ab available hai! Jaake abhi order karo. 🛍️"
    subscribers = get_notify_subscribers(prod_id)
    sent, failed = 0, 0
    for uid in subscribers:
        try:
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🛒 Ab Order Karo", callback_data=f"product_{prod_id}", style="success", icon_custom_emoji_id=btn_emo("shop_now")))
            bot.send_message(uid, text, parse_mode="HTML", reply_markup=markup)
            sent += 1
        except Exception:
            failed += 1
    clear_notify_subscribers(prod_id)
    result_note = f"✅ Notify broadcast bhej diya gaya!\n👥 Sent: {sent}"
    if failed:
        result_note += f"\n⚠️ Failed: {failed}"
    bot.send_message(admin_chat_id, result_note)

def prompt_notify_broadcast_if_needed(chat_id, prod_id, new_val):
    """Call this right after a product's maintenance_enabled flips. If it just
    turned OFF and people are waiting on the Notify Me list, ask the admin to
    write the announcement (or skip for a default one); otherwise do nothing."""
    if new_val != 0:
        return
    subscribers = get_notify_subscribers(prod_id)
    if not subscribers:
        return
    user_states[ADMIN_USER_ID] = {"action": "awaiting_notify_broadcast", "prod_id": prod_id}
    cancel_markup = InlineKeyboardMarkup()
    cancel_markup.add(InlineKeyboardButton("⏭️ Skip (default message bhejo)", callback_data=f"notifyskip_{prod_id}", style="danger"))
    msg = bot.send_message(
        chat_id,
        f"🔔 <b>{len(subscribers)} user(s)</b> is product ke 'Notify Me' list mein hain.\n\n"
        "Unhe bhejne wala message likho (Premium emoji, bold/links sab preserve hoga), "
        "ya neeche 'Skip' dabao default message bhejne ke liye:",
        parse_mode="HTML",
        reply_markup=cancel_markup
    )
    bot.register_next_step_handler(msg, receive_notify_broadcast_text)

def receive_notify_broadcast_text(message):
    if _check_cancel(message):
        return
    user_id = message.from_user.id
    state = user_states.get(user_id, {})
    if state.get("action") != "awaiting_notify_broadcast":
        return
    prod_id = state.get("prod_id")
    user_states.pop(user_id, None)
    text = message.text.strip() if message.text else ""
    if not text:
        bot.reply_to(message, "❌ Empty text. Broadcast cancel kar diya gaya.")
        return
    if text.lower() in ("skip", "default"):
        send_notify_broadcast(prod_id, message.chat.id)
        return
    entities_json = extract_custom_emoji_json(message)
    send_notify_broadcast(prod_id, message.chat.id, custom_text=text, custom_entities=entities_json)

def get_product_emoji(row_value):
    """Return the per-product custom Premium emoji ID (set via the product's
    'Set Product Emoji' field in the admin Edit Product screen), or the global
    default button emoji if this product hasn't been given its own."""
    return row_value if row_value else DEFAULT_BUTTON_EMOJI_ID

def extract_custom_emoji_json(message):
    """Pull out ALL formatting entities from a message (its text or caption) as a
    JSON string -- not just custom emoji, but also Bold/Italic/Underline/
    Strikethrough/Spoiler/Code/Blockquote/Links -- so free-text fields like a
    product Description keep whatever rich formatting the admin typed, exactly
    as Telegram's own formatting toolbar produced it."""
    entities = message.entities or message.caption_entities
    if not entities:
        return None
    SUPPORTED = {"bold", "italic", "underline", "strikethrough", "spoiler", "code",
                 "pre", "blockquote", "expandable_blockquote", "text_link", "custom_emoji"}
    ents = []
    for e in entities:
        etype = getattr(e, "type", None)
        if etype not in SUPPORTED:
            continue
        item = {"offset": e.offset, "length": e.length, "type": etype}
        if etype == "custom_emoji":
            cid = getattr(e, "custom_emoji_id", None)
            if not cid:
                continue
            item["custom_emoji_id"] = cid
        elif etype == "text_link":
            item["url"] = getattr(e, "url", None) or ""
        ents.append(item)
    return json.dumps(ents) if ents else None

def delete_active_demo_video(chat_id):
    """Delete the 'Watch Demo' video message for this chat, if one is currently
    showing, and forget it. Called whenever the person navigates away (Back,
    /start) so the demo video only stays visible while inside that product."""
    msg_id = active_demo_video.pop(chat_id, None)
    if msg_id:
        try:
            bot.delete_message(chat_id, msg_id)
        except:
            pass

def format_duration(value, unit):
    """Format plan labels. Existing days/hours remain unchanged; credits/tokens
    are supported for non-time products.
    """
    unit = (unit or "days").strip().lower()
    n = str(value)
    if unit == "hours":
        return f"{n} Hour" if n == "1" else f"{n} Hours"
    if unit in ("credit", "credits"):
        return f"{n} Credit" if n == "1" else f"{n} Credits"
    if unit in ("token", "tokens"):
        return f"{n} Token" if n == "1" else f"{n} Tokens"
    return f"{n} Day" if n == "1" else f"{n} Days"


def format_duration_for_reseller(value, unit):
    """Return the normal Bunty duration spelling used by the API.

    Bunty panels in the wild use slightly different day labels (for example
    ``1 Day``, ``1 Days`` or ``1 Day's``), while the hours format is normally
    ``1 Hours``.  The purchase function below handles the day-label aliases
    safely only after an explicit OUT OF STOCK response.
    """
    unit = (unit or "days").strip().lower()
    if unit == "hours":
        return f"{value} Hours"
    if unit in ("credit", "credits", "token", "tokens"):
        return ""
    n = str(value).strip()
    return f"{n} Day" if n == "1" else f"{n} Days"


def reseller_duration_candidates(duration_label):
    """Build safe equivalent spellings for the SAME numeric duration.

    The Bunty endpoint can have stock indexed with a slightly different
    spelling/casing (for example ``1 Day`` vs ``1 Days``).  We never change
    the numeric duration or PID, and aliases are only attempted after the
    provider explicitly returns an out-of-stock response.
    """
    label = str(duration_label or "").strip()
    if not label:
        return []

    candidates = [label]
    seen = {label.lower()}

    # Hours: keep the existing working contract first, then harmless casing /
    # singular-plural aliases if the provider explicitly reports no stock.
    mh = re.fullmatch(r"(\d+)\s*hours?", label, flags=re.I)
    if mh:
        n = mh.group(1)
        aliases = [f"{n} Hours", f"{n} Hour", f"{n} hours", f"{n} hour"]
    else:
        # Days: support the common Day/Days/Day's spellings, plus casing.
        md = re.fullmatch(r"(\d+)\s*days?['’]?s?", label, flags=re.I)
        if md:
            n = md.group(1)
            aliases = [
                f"{n} Day", f"{n} Days", f"{n} Day's",
                f"{n} day", f"{n} days", f"{n} day's",
            ]
        else:
            aliases = []

    for alias in aliases:
        key = alias.lower()
        if key not in seen:
            candidates.append(alias)
            seen.add(key)
    return candidates

def _load_entities(entities_json):
    """Parse the stored entities JSON, treating any entry missing a 'type' key
    (saved by an older version of the bot, before rich formatting was supported)
    as a custom_emoji entry for backward compatibility."""
    try:
        entities = json.loads(entities_json)
    except Exception:
        return []
    if not entities:
        return []
    for e in entities:
        e.setdefault("type", "custom_emoji")
    return entities

def strip_custom_emoji(raw_text, entities_json):
    """Remove any custom_emoji placeholder characters from plain text entirely.
    Telegram buttons cannot render <tg-emoji> or any other HTML formatting, and
    a raw custom-emoji character left in would look like a stray leftover emoji.
    Other formatting entities (bold/italic/blockquote/etc.) don't add extra
    characters, so they're simply ignored here -- the underlying text stays."""
    if not raw_text:
        return ""
    if not entities_json:
        return raw_text
    entities = [e for e in _load_entities(entities_json) if e["type"] == "custom_emoji"]
    if not entities:
        return raw_text
    utf16 = raw_text.encode("utf-16-le")
    entities = sorted(entities, key=lambda e: e["offset"])
    out = []
    cursor = 0
    for e in entities:
        start = e["offset"] * 2
        length = e["length"] * 2
        if start < cursor or start > len(utf16):
            continue
        out.append(utf16[cursor:start].decode("utf-16-le", errors="ignore"))
        cursor = start + length
    out.append(utf16[cursor:].decode("utf-16-le", errors="ignore"))
    return "".join(out).strip()

def _entity_tags(e):
    """Map a stored entity to its (open_tag, close_tag) HTML pair."""
    t = e["type"]
    if t == "bold":
        return "<b>", "</b>"
    if t == "italic":
        return "<i>", "</i>"
    if t == "underline":
        return "<u>", "</u>"
    if t == "strikethrough":
        return "<s>", "</s>"
    if t == "spoiler":
        return "<tg-spoiler>", "</tg-spoiler>"
    if t == "code":
        return "<code>", "</code>"
    if t == "pre":
        return "<pre>", "</pre>"
    if t == "blockquote":
        return "<blockquote>", "</blockquote>"
    if t == "expandable_blockquote":
        return "<blockquote expandable>", "</blockquote>"
    if t == "text_link":
        return f'<a href="{html.escape(e.get("url") or "")}">', "</a>"
    if t == "custom_emoji":
        return f'<tg-emoji emoji-id="{html.escape(str(e.get("custom_emoji_id") or ""))}">', "</tg-emoji>"
    return "", ""

def render_text_with_custom_emoji(raw_text, entities_json):
    """Turn plain text + its saved formatting entities back into safe HTML --
    Bold/Italic/Underline/Strikethrough/Spoiler/Code/Blockquote/Links/Custom
    Emoji, exactly as Telegram's formatting toolbar produced them, nested
    correctly. Telegram entity offsets/lengths are in UTF-16 code units, so we
    work in UTF-16 to stay correct for any language/script."""
    if not raw_text:
        return ""
    entities = _load_entities(entities_json) if entities_json else []
    if not entities:
        return html.escape(raw_text)
    utf16 = raw_text.encode("utf-16-le")
    prepared = []
    for e in entities:
        s = e["offset"] * 2
        en = s + e["length"] * 2
        if 0 <= s < en <= len(utf16):
            prepared.append({**e, "_s": s, "_e": en})

    def render_range(lo, hi, exclude=None):
        relevant = [e for e in prepared if e["_s"] >= lo and e["_e"] <= hi]
        if exclude is not None:
            # Exclude by POSITION match (not object identity). If the saved
            # entities list ever has two duplicate entries at the exact same
            # offset/length (e.g. an admin accidentally embedded the same
            # custom emoji twice at one spot), matching only by object identity
            # let the two duplicates keep re-including each other every time we
            # recursed into their own span -- infinite recursion, crashing with
            # "maximum recursion depth exceeded" (this is what broke the
            # "Drip Client Apkmod" video caption). Matching by position instead
            # guarantees the recursion always shrinks the range and terminates.
            relevant = [e for e in relevant if not (e["_s"] == exclude["_s"] and e["_e"] == exclude["_e"])]
        # Keep only "top-level" entities within this range (drop ones fully
        # nested inside another one already picked) so nesting renders correctly.
        top = []
        for e in sorted(relevant, key=lambda e: (e["_s"], -(e["_e"] - e["_s"]))):
            if any((t["_s"], t["_e"]) == (e["_s"], e["_e"]) for t in top):
                continue
            if any(t["_s"] <= e["_s"] and e["_e"] <= t["_e"] for t in top):
                continue
            top.append(e)
        top.sort(key=lambda e: e["_s"])
        out = []
        cursor = lo
        for e in top:
            if e["_s"] > cursor:
                out.append(html.escape(utf16[cursor:e["_s"]].decode("utf-16-le", errors="ignore")))
            open_tag, close_tag = _entity_tags(e)
            out.append(open_tag)
            out.append(render_range(e["_s"], e["_e"], exclude=e))
            out.append(close_tag)
            cursor = e["_e"]
        if hi > cursor:
            out.append(html.escape(utf16[cursor:hi].decode("utf-16-le", errors="ignore")))
        return "".join(out)

    return render_range(0, len(utf16))

def generate_order_id():
    return f"ORD{int(time.time())}{random.randint(100,999)}"

# ======================= CUSTOM ORDER NOTIFY TEXT =======================
# By default the "NEW ORDER!" message (sent to both admin and the buyer right
# after an instant/stock-key purchase) uses the hardcoded layout below, with
# icons controlled per-slot from Premium Emoji Manager. If the admin sets their
# own template (Settings -> Set Order Notify Text), that full custom wording is
# used instead -- placeholders get swapped in, and any Premium emoji the admin
# typed/forwarded into that template are preserved via saved entities.
ORDER_NOTIFY_PLACEHOLDERS = "{name} {user_id} {username} {product} {key} {amount} {balance} {time}"

def build_order_notify_text(display_name, user_id, username_str, prod_name, key_text, price, new_bal, time_str):
    """All string args are expected RAW (not pre-escaped) -- this function
    escapes them itself before building either the default or custom-template HTML."""
    display_name = html.escape(str(display_name))
    username_str = html.escape(str(username_str))
    prod_name = html.escape(str(prod_name))
    key_text = html.escape(str(key_text))
    template = get_setting("order_notify_template")
    if not template:
        # ---- Original hardcoded default (unchanged behaviour until admin customizes) ----
        return (
            f"{emo('order_title_l')} <b>NEW ORDER!</b> {emo('order_title_r')}\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"{emo('order_name')} Name: {display_name}\n"
            f"{emo('order_userid')} User ID: {user_id}\n"
            f"{emo('order_phone')} Phone: N/A\n"
            f"{emo('order_username')} Username: {username_str}\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"{emo('order_product')} Product: {prod_name}\n"
            f"{emo('order_key')} Key: {key_text}\n"
            f"{emo('order_amount')} Amount: ₹{price}\n"
            f"{emo('order_balance')} Available Balance: ₹{new_bal}\n"
            f"{emo('order_time')} Time: {time_str}\n"
            f"━━━━━━━━━━━━━━━━━"
        )
    entities_json = get_setting("order_notify_template_entities")
    rendered = render_text_with_custom_emoji(template, entities_json)
    return (rendered
            .replace("{name}", display_name)
            .replace("{user_id}", str(user_id))
            .replace("{username}", username_str)
            .replace("{product}", prod_name)
            .replace("{key}", key_text)
            .replace("{amount}", f"₹{price}")
            .replace("{balance}", f"₹{new_bal}")
            .replace("{time}", time_str))

def build_support_text():
    """Support screen text -- fully admin-editable (Settings -> Set Support
    Text), with Premium emoji preserved via saved entities. Falls back to the
    original hardcoded default until the admin customizes it."""
    template = get_setting("support_text_template")
    if not template:
        return (
            f"{emo('support_icon')} <b>VETLAM PANEL SHOP — Support</b>\n\n"
            f"🎧 <i>Our team is here to help you.</i>\n\n"
            f"<b>We can assist with:</b>\n"
            f"— 🔑 Orders &amp; key delivery\n"
            f"— 💰 Payments &amp; balance\n"
            f"— 💲 Product questions\n"
            f"— 🔧 Any other issue\n\n"
            f"🕐 <i>Typical reply time: within a few hours.</i>\n\n"
            f"<i>Tap a button below to contact us directly</i> 👇"
        )
    entities_json = get_setting("support_text_template_entities")
    return render_text_with_custom_emoji(template, entities_json)

def build_store_highlights_text():
    """The '— STORE HIGHLIGHTS —' box shown on the main menu screen --
    fully admin-editable (Settings -> Set Store Highlights Text), with
    Premium emoji preserved via saved entities. Falls back to a default
    block (icons individually customizable via Premium Emoji Manager)
    until the admin overrides the whole block themselves."""
    template = get_setting("store_highlights_template")
    if not template:
        return (
            f"{emo('highlight_top_l')} — <b>STORE HIGHLIGHTS</b> — {emo('highlight_top_r')}\n\n"
            f"{emo('highlight_keys')} Premium Game Keys, Instant Delivery 24/7\n"
            f"{emo('highlight_secure')} 100% Secure Payment\n"
            f"{emo('highlight_price')} Best Prices Guaranteed\n"
            f"{emo('highlight_trust')} Safe And Trusted {emo('highlight_rocket')}\n"
            f"{emo('highlight_support')} Professional Support"
        )
    entities_json = get_setting("store_highlights_template_entities")
    return render_text_with_custom_emoji(template, entities_json)

# ======================= ZAPUPI (automatic UPI payments) =======================
def create_zapupi_order(order_id, amount, user_id):
    """Calls ZapUPI's create-order API to get a unique payment link for this order.
    Returns (True, payment_url, txn_id) on success or (False, error_message, None)
    on any failure (network error, bad response, etc) so the caller can fail
    gracefully instead of showing the user a broken payment button."""
    try:
        payload = {
            "zap_key": get_zapupi_key(),
            "order_id": order_id,
            "amount": str(amount),
            "remark": f"VETLAM PANEL SHOP | UID {user_id}",
            "success_url": f"{WEBHOOK_BASE_URL}/zapupi/success",
            "failed_url": f"{WEBHOOK_BASE_URL}/zapupi/failed",
            "timeout_url": f"{WEBHOOK_BASE_URL}/zapupi/timeout",
            "webhook_url": f"{WEBHOOK_BASE_URL}/zapupi/webhook",
        }
        data = json.dumps(payload).encode()
        req = urllib.request.Request(f"{ZAPUPI_API_BASE}/create-order", data=data, method="POST")
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
    except Exception as e:
        return False, f"ZapUPI request failed: {e}", None
    try:
        parsed = json.loads(raw)
    except Exception:
        return False, f"ZapUPI returned a non-JSON response: {raw[:300]}", None
    if str(parsed.get("status", "")).lower() != "success":
        return False, parsed.get("message") or f"ZapUPI error response: {raw[:300]}", None
    payment_url = parsed.get("payment_url")
    txn_id = parsed.get("txn_id")
    if not payment_url:
        return False, f"ZapUPI succeeded but returned no payment_url: {raw[:300]}", None
    return True, payment_url, txn_id

def check_zapupi_order_status(order_id):
    """Polls ZapUPI's order-status API directly (used as a manual '🔄 Check Status'
    fallback, and by the background reconciliation thread, in case the webhook
    never arrives). Returns the 'data' dict from ZapUPI, or None on any failure."""
    try:
        payload = {"zap_key": get_zapupi_key(), "order_id": order_id}
        data = json.dumps(payload).encode()
        req = urllib.request.Request(f"{ZAPUPI_API_BASE}/order-status", data=data, method="POST")
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
        parsed = json.loads(raw)
        if str(parsed.get("status", "")).lower() != "success":
            return None
        return parsed.get("data")
    except Exception as e:
        print("ZapUPI order-status error:", e)
        return None

# ======================= FAMPAY (automatic UPI QR payments) =======================
def create_fampay_order(order_id, amount, user_id):
    """Calls FamPay's QR-generate API to get a UPI QR code for this order.
    Returns (True, data_dict, fampay_order_id) on success or (False, error_message,
    None) on any failure -- same (ok, result, ref) shape as create_zapupi_order()
    so process_deposit_amount() can drive either gateway with the same branching."""
    api_key = get_fampay_api_key()
    if not api_key:
        return False, "FamPay API key not configured by admin.", None
    upi_id = get_fampay_upi_id()
    if not upi_id:
        return False, "FamPay UPI ID not configured by admin.", None
    try:
        params = urllib.parse.urlencode({
            "upi": upi_id,
            "amount": str(amount),
            "api_key": api_key,
            "order_id": order_id,
            "remark": f"VETLAM PANEL SHOP | UID {user_id}",
        })
        req = urllib.request.Request(f"{FAMPAY_QR_URL}?{params}", method="GET")
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
    except Exception as e:
        set_setting("fampay_debug_last_create", f"[REQUEST FAILED] {e}")
        return False, f"FamPay request failed: {e}", None
    # Always keep the raw response so admin can inspect the real API shape from
    # Admin Panel -> Payment Gateway Settings -> FamPay Debug (network access
    # here can't be tested live, so this is how the exact field names get confirmed).
    set_setting("fampay_debug_last_create", raw[:1500])
    try:
        parsed = json.loads(raw)
    except Exception:
        return False, f"FamPay returned a non-JSON response: {raw[:300]}", None
    if str(parsed.get("status", "")).lower() != "success":
        return False, parsed.get("message") or f"FamPay error response: {raw[:300]}", None
    data = parsed.get("data") or {}
    if not data.get("qr_url"):
        return False, f"FamPay succeeded but returned no qr_url: {raw[:300]}", None
    return True, data, data.get("order_id", order_id)

def check_fampay_order_status(order_id):
    """Polls FamPay's verify API (used as a manual '🔄 Check Status' fallback, and
    by the background reconciliation thread). Returns the 'data' dict from FamPay,
    or None on any failure."""
    api_key = get_fampay_api_key()
    if not api_key or not order_id:
        return None
    try:
        params = urllib.parse.urlencode({"order_id": order_id, "api_key": api_key})
        req = urllib.request.Request(f"{FAMPAY_VERIFY_URL}?{params}", method="GET")
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
        # Always keep the raw response (see FamPay Debug screen) -- this is what
        # will let us match the exact field names FamPay actually uses.
        set_setting("fampay_debug_last_check", raw[:1500])
        parsed = json.loads(raw)
        if str(parsed.get("status", "")).lower() != "success":
            return None
        return parsed.get("data")
    except Exception as e:
        set_setting("fampay_debug_last_check", f"[REQUEST FAILED] order_id={order_id} | {e}")
        print("FamPay order-status error:", e)
        return None

def credit_pending_payment(pay_id, txn_id=None, utr=None):
    """Shared balance-credit logic for a pending_payments row moving to Success.
    Called from the '🔄 Check Status' button and from the reconciliation thread
    below. webhook_server.py runs as a SEPARATE process (it has to, since it's a
    Flask web server and bot.py is a long-polling Telegram bot) so it has its own
    copy of this same logic against the same sqlite DB file -- keep both in sync
    if this ever changes."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, amount, status, gateway FROM pending_payments WHERE id=?", (pay_id,))
    row = cursor.fetchone()
    if not row or row[2] == "success":
        cursor.close()
        conn.close()
        return False
    uid, amount, _status, gateway = row[0], row[1], row[2], (row[3] or "zapupi")
    gateway_label = "FamPay (UPI)" if gateway == "fampay" else "ZapUPI (UPI)"
    cursor.execute("SELECT balance FROM users WHERE id=?", (uid,))
    urow = cursor.fetchone()
    new_bal = (urow[0] if urow else 0) + amount
    if not urow:
        cursor.execute("INSERT INTO users (id, balance) VALUES (?,?)", (uid, amount))
    else:
        cursor.execute("UPDATE users SET balance=? WHERE id=?", (new_bal, uid))
    cursor.execute("INSERT INTO transactions (user_id, amount, type, details) VALUES (?,?,'add_money',?)",
                   (uid, amount, f"{gateway_label} auto top-up (txn: {txn_id or 'N/A'})"))
    cursor.execute("UPDATE pending_payments SET status='success', zapupi_txn_id=? WHERE id=?", (txn_id, pay_id))
    conn.commit()
    cursor.close()
    conn.close()
    try:
        bot.send_message(uid, f"{emo('paysucc_title')} <b>Payment Received!</b>\n\n{emo('paysucc_amount')} ₹{amount} added to your balance.\n{emo('paysucc_balance')} New Balance: ₹{new_bal}", parse_mode="HTML")
    except:
        pass
    try:
        bot.send_message(ADMIN_USER_ID, f"💰 <b>{gateway_label} Auto Top-up</b>\n\n👤 User: {uid}\n💵 Amount: ₹{amount}\n🧾 Txn: {txn_id or 'N/A'}\n🏦 UTR: {utr or 'N/A'}", parse_mode="HTML")
    except:
        pass
    return True

def reconcile_zapupi_payments():
    """Fallback safety net (runs every 2 minutes): polls the order-status API of
    whichever gateway each pending payment used (ZapUPI or FamPay) for any
    pending_payments still 'pending' after at least 90 seconds (gives the webhook /
    manual Check Status a head start first) in case it was never picked up for
    some reason (VPS restart, brief network hiccup, firewall blip)."""
    while True:
        try:
            conn = get_db()
            cursor = conn.cursor()
            cutoff = int(time.time()) - 90
            cursor.execute("SELECT id, order_id, gateway, fampay_order_id FROM pending_payments WHERE status='pending' AND created_at < ?", (cutoff,))
            rows = cursor.fetchall()
            cursor.close()
            conn.close()
            for pay_id, order_id, gateway, fampay_order_id in rows:
                if (gateway or "zapupi") == "fampay":
                    fdata = check_fampay_order_status(fampay_order_id or order_id)
                    # check_fampay_order_status() already only returns the data dict
                    # when FamPay's outer response was status="success" (payment
                    # confirmed) -- that dict itself has no separate "status" field
                    # (see fields: order_id, transaction_id, amount, utr, sender_name),
                    # so a plain truthy check is correct here.
                    if fdata:
                        credit_pending_payment(pay_id, txn_id=fdata.get("transaction_id") or fdata.get("txn_id"), utr=fdata.get("utr"))
                else:
                    zdata = check_zapupi_order_status(order_id)
                    if zdata and str(zdata.get("status", "")).lower() == "success":
                        credit_pending_payment(pay_id, txn_id=zdata.get("txn_id"), utr=zdata.get("utr"))
        except Exception as e:
            print("Reconcile thread error:", e)
        time.sleep(120)

def expire_old_pending_payments():
    """Auto-expire pending payments older than 30 minutes (runs in background)."""
    while True:
        try:
            conn = get_db()
            cursor = conn.cursor()
            cutoff = int(time.time()) - 1800
            cursor.execute("SELECT id, user_id FROM pending_payments WHERE status='pending' AND created_at < ?", (cutoff,))
            expired = cursor.fetchall()
            for row in expired:
                cursor.execute("UPDATE pending_payments SET status='expired' WHERE id=?", (row[0],))
                try:
                    bot.send_message(row[1], "⌛ Your pending payment request expired (30 min). Please try again from ➕ Add Balance.")
                except:
                    pass
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            print("Expire thread error:", e)
        time.sleep(60)

def process_scheduled_deletions():
    """Runs every minute. Deletes any message whose scheduled delete_at time has
    passed (used by the 1-Hour Announcement feature). Stored in the DB rather than
    kept only in memory, so it still works correctly even if the bot restarts."""
    while True:
        try:
            conn = get_db()
            cursor = conn.cursor()
            now = int(time.time())
            cursor.execute("SELECT id, chat_id, message_id FROM scheduled_deletions WHERE delete_at <= ?", (now,))
            due = cursor.fetchall()
            for row in due:
                row_id, chat_id, message_id = row
                try:
                    bot.delete_message(chat_id, message_id)
                except:
                    pass
                cursor.execute("DELETE FROM scheduled_deletions WHERE id=?", (row_id,))
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            print("Scheduled deletion thread error:", e)
        time.sleep(60)

# ======================= DATABASE BACKUP (survives Termux data wipe) =======================
def backup_database():
    """Copy the sqlite DB file to phone/server storage, keeping the SAME filename as the
    live DB (e.g. abhipanelshop_bot.db), overwritten every time — no timestamped copies pile up.
    Naming it after the real DB filename (instead of a generic 'latest.db') means a backup
    from one DB file can never get restored into a different-named DB file by mistake."""
    try:
        os.makedirs(BACKUP_DIR, exist_ok=True)
        backup_path = os.path.join(BACKUP_DIR, os.path.basename(DB_PATH))
        shutil.copy2(DB_PATH, backup_path)
        return backup_path
    except Exception as e:
        print("Backup error:", e)
        return None

def auto_backup_loop():
    """Runs a fresh backup every 15 minutes automatically (overwrites the same file)."""
    while True:
        time.sleep(900)  # 15 minutes
        backup_database()

# ======================= TELEBOT INIT =======================
bot = telebot.TeleBot(BOT_TOKEN, threaded=True)
user_states = {}
# Prevent duplicate auto-delivery when the same user taps Buy twice quickly or
# Telegram delivers the same callback while the first provider request is still running.
# One lock per user keeps Bunty/legacy and KeysPanelShop purchases independent in code.
_purchase_locks = {}
_purchase_locks_guard = threading.Lock()

def _get_purchase_lock(user_id):
    with _purchase_locks_guard:
        return _purchase_locks.setdefault(int(user_id), threading.Lock())

active_demo_video = {}  # chat_id -> message_id of the last "Watch Demo" video sent, so it can be auto-deleted when the person navigates away (Back/Start)

# ======================= ZAPUPI WEBHOOK SERVER (merged from webhook_server.py) =======================
# Originally a separate Flask process (webhook_server.py) so it could run independently
# of the Telegram long-polling loop. Merged here into a single file: the Flask app is
# started on its own background thread from the __main__ block below, alongside the
# bot's polling loop and the other background threads. It reuses the SAME get_db(),
# credit_pending_payment() and check_zapupi_order_status() functions defined above
# instead of keeping duplicate copies, so there's only one place to update this logic.
WEBHOOK_LISTEN_PORT = 5000

flask_app = Flask(__name__)

def port_is_in_use(port):
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.settimeout(0.5)
        return sock.connect_ex(("127.0.0.1", int(port))) == 0
    finally:
        sock.close()


@flask_app.route("/zapupi/webhook", methods=["POST"])
def zapupi_webhook():
    try:
        data = request.get_json(force=True, silent=True) or {}
    except Exception:
        data = {}

    order_id = data.get("order_id")
    status = data.get("status")
    txn_id = data.get("txn_id")
    utr = data.get("utr")
    environment = data.get("environment")
    print(f"ZapUPI webhook received: order_id={order_id} status={status} environment={environment}")

    if not order_id:
        return jsonify({"status": "ignored", "reason": "no order_id in payload"}), 200

    if environment == "test":
        # ZapUPI's own test-mode pings (txn_id starts with DUMMY) -- never treat as real money.
        return jsonify({"status": "ok", "note": "test environment ignored"}), 200

    if status != "Success":
        # Failed / other statuses -- mark it if we have a matching pending row, nothing to credit.
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM pending_payments WHERE order_id=? AND status='pending'", (order_id,))
        row = cursor.fetchone()
        if row and status == "Failed":
            cursor.execute("UPDATE pending_payments SET status='failed' WHERE id=?", (row[0],))
            conn.commit()
        cursor.close()
        conn.close()
        return jsonify({"status": "ok"}), 200

    # status == "Success" claimed by the webhook -- verify directly with ZapUPI
    # before crediting anything (raw incoming POSTs carry no HMAC signature, so
    # trusting one blindly would let anyone who guesses/leaks an order_id fire a
    # fake "Success" webhook and get free balance).
    verified = check_zapupi_order_status(order_id)
    if not verified or str(verified.get("status", "")).lower() != "success":
        print(f"⚠️ Webhook claimed Success for order {order_id} but ZapUPI verification did not confirm it -- NOT crediting.")
        return jsonify({"status": "ignored", "reason": "could not verify with ZapUPI"}), 200

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id, amount FROM pending_payments WHERE order_id=?", (order_id,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    if not row:
        return jsonify({"status": "ignored", "reason": "unknown order_id"}), 200

    # Sanity check: the verified amount from ZapUPI should match what we expect
    # for this order, so a tampered/replayed webhook can't credit the wrong amount.
    try:
        expected = float(row[1])
        got = float(verified.get("amount", 0))
        if abs(expected - got) > 0.5:
            print(f"⚠️ Amount mismatch for order {order_id}: expected {expected}, ZapUPI reports {got} -- NOT crediting.")
            return jsonify({"status": "ignored", "reason": "amount mismatch"}), 200
    except Exception:
        pass

    credit_pending_payment(row[0], txn_id=txn_id or verified.get("txn_id"), utr=utr or verified.get("utr"))
    return jsonify({"status": "ok"}), 200


def _webhook_landing_page(title, message, color):
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>
body {{ font-family: -apple-system, Segoe UI, sans-serif; background:#0f0f13; color:#fff;
        display:flex; align-items:center; justify-content:center; min-height:100vh; margin:0; text-align:center; padding:24px; box-sizing:border-box; }}
.card {{ padding:32px 28px; border-radius:16px; background:#1a1a20; max-width:360px; }}
h1 {{ color:{color}; margin-bottom:8px; font-size:22px; }}
p {{ color:#aaa; line-height:1.5; }}
</style></head>
<body><div class="card"><h1>{title}</h1><p>{message}</p><p>Aap Telegram par wapas jaake bot check kar sakte ho.</p></div></body></html>"""


@flask_app.route("/zapupi/success")
def zapupi_success():
    return _webhook_landing_page("✅ Payment Successful", "Aapka payment receive ho gaya hai. Balance thodi hi der mein automatically add ho jaayega.", "#22c55e")


@flask_app.route("/zapupi/failed")
def zapupi_failed():
    return _webhook_landing_page("❌ Payment Failed", "Aapka payment fail ho gaya. Bot mein dobara Add Balance try karo.", "#ef4444")


@flask_app.route("/zapupi/timeout")
def zapupi_timeout():
    return _webhook_landing_page("⌛ Payment Timed Out", "Ye payment link expire ho gaya. Bot mein dobara Add Balance try karo.", "#f59e0b")


@flask_app.route("/health")
def webhook_health():
    return jsonify({"status": "ok", "time": int(time.time())})


def run_webhook_server():
    """Runs Flask beside Telegram polling without crashing on a duplicate start.
    If port 5000 already belongs to another copy, leave that existing webhook
    server alone instead of starting a second copy and causing SQLite contention."""
    if port_is_in_use(WEBHOOK_LISTEN_PORT):
        print(f"⚠️ Port {WEBHOOK_LISTEN_PORT} already in use; existing webhook server kept. Skipping duplicate Flask server.")
        return
    try:
        flask_app.run(host="0.0.0.0", port=WEBHOOK_LISTEN_PORT, use_reloader=False, threaded=True)
    except OSError as e:
        print(f"⚠️ Webhook server could not start: {e}. Telegram bot will continue running.")

def _check_cancel(message):
    """Guard used at the start of every 'waiting for a text reply' handler
    (send new name, send description, send price, etc). If the person actually
    sent a command (like /start) instead of the expected value -- usually by
    accident -- we must NOT save that command text as the data. Instead we let
    Telegram's normal command handling run it properly, and tell the person
    their previous action was cancelled. Returns True if this happened (caller
    should return immediately without saving anything)."""
    if message.text and message.text.startswith('/'):
        try:
            bot.reply_to(message, "❌ Cancelled (command detected).")
        except:
            pass
        try:
            bot.process_new_messages([message])
        except:
            pass
        return True
    return False

# ======================= HELPER FUNCTIONS =======================
def is_verified(user_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT verified FROM users WHERE id=?", (user_id,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row and row[0] == 1

def set_verified(user_id, phone_number):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET verified=1, phone_number=? WHERE id=?", (phone_number, user_id))
    conn.commit()
    cursor.close()
    conn.close()

def is_banned(user_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT banned FROM users WHERE id=?", (user_id,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row and row[0] == 1

def ban_user(user_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET banned=1 WHERE id=?", (user_id,))
    conn.commit()
    cursor.close()
    conn.close()

def unban_user(user_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET banned=0 WHERE id=?", (user_id,))
    conn.commit()
    cursor.close()
    conn.close()

# ======================= RESELLER SYSTEM =======================
def is_active_reseller(user_id):
    """True only if the user has been granted reseller status (is_reseller=1)
    AND is not currently suspended (reseller_banned=0). Used to decide whether
    to show/charge reseller pricing to this specific user."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT is_reseller, reseller_banned FROM users WHERE id=?", (user_id,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return bool(row and row[0] and not row[1])

def effective_price(user_id, price, reseller_price):
    """Returns the price this specific user should pay for a plan: the
    reseller_price if the user is an active (non-banned) reseller AND a
    reseller_price has been set for this plan, otherwise the normal price."""
    if reseller_price is not None and is_active_reseller(user_id):
        return reseller_price
    return price

# ======================= NEW MAIN MENU TEXT =======================
def get_main_menu_text(name, user_id=None):
    name = html.escape(str(name))
    balance_block = ""
    if user_id is not None:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT balance FROM users WHERE id=?", (user_id,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        bal = row[0] if row else 0
        balance_block = f"""
━━━━━━━━━━━━━━━━━━━━━
{emo('menu_userid')} User ID: {user_id}
{emo('menu_wallet')} Wallet Balance: ₹{bal}{usd_hint(bal)}
━━━━━━━━━━━━━━━━━━━━━
"""
    return f"""{emo('menu_title_l')} — VETLAM PANEL SHOP — {emo('menu_title_r')}

{emo('menu_wave')} <i>Hello, {name}!</i>

{build_store_highlights_text()}
{balance_block}
<i>Tap any button below to begin.</i>"""

# ======================= KEYBOARDS =======================
def get_main_menu(user_id):
    if is_banned(user_id):
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton(" Contact Support", callback_data="support", style="success", icon_custom_emoji_id=btn_emo("support")))
        return markup

    # Create markup with row_width=2 for better layout
    markup = InlineKeyboardMarkup(row_width=2)

    # Layout matches the White X Modz Store reference: full-width Shop on top,
    # then paired rows below.
    markup.add(
        InlineKeyboardButton("Shop", callback_data="shop_now", style="primary", icon_custom_emoji_id=btn_emo("shop_now"))
    )
    markup.add(
        InlineKeyboardButton(" Add Balance", callback_data="add_balance", style="success", icon_custom_emoji_id=btn_emo("add_balance")),
        InlineKeyboardButton(" My Orders", callback_data="history", style="success", icon_custom_emoji_id=btn_emo("history")),
    )
    markup.add(
        InlineKeyboardButton(" My Profile", callback_data="profile", style="success", icon_custom_emoji_id=btn_emo("profile")),
        InlineKeyboardButton(" How To Use ", callback_data="tutorial", style="success", icon_custom_emoji_id=btn_emo("tutorial")),
    )
    markup.add(
        InlineKeyboardButton(" Referral", callback_data="referral", style="success", icon_custom_emoji_id=btn_emo("referral")),
        InlineKeyboardButton(" Daily Spin", callback_data="spin", style="success", icon_custom_emoji_id=btn_emo("daily_spin")),
    )
    markup.add(
        InlineKeyboardButton(" Support", callback_data="support", style="success", icon_custom_emoji_id=btn_emo("support")),
    )

    # Self-serve "Become a Reseller" button -- only shown when admin has
    # turned the reseller-buy program ON, and hidden for users who are
    # already an active (non-banned) reseller.
    if get_setting("reseller_buy_enabled") == "1":
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT is_reseller, reseller_banned FROM users WHERE id=?", (user_id,))
        rrow = cursor.fetchone()
        cursor.close()
        conn.close()
        already_reseller = bool(rrow and rrow[0] and not rrow[1])
        if not already_reseller:
            markup.add(InlineKeyboardButton(" Become a Reseller", callback_data="become_reseller", style="success", icon_custom_emoji_id=btn_emo("become_reseller")))

    # Selling Proof + Leaderboard (Paid Store was intentionally removed).
    selling_proof_link = get_setting("selling_proof_link")
    link_row = []
    if selling_proof_link:
        link_row.append(InlineKeyboardButton(" Selling Proof", url=selling_proof_link, style="success", icon_custom_emoji_id=btn_emo("link_selling_proof")))
    # Leaderboard replaces Paid Store and stays visible as the navigation button.
    _lb_btn_custom = get_setting("emoji_slot_btn_link_leaderboard")
    _lb_btn_text = "Leaderboard" if _lb_btn_custom else "🏆 Leaderboard"
    link_row.append(InlineKeyboardButton(_lb_btn_text, callback_data="leaderboard", style="success", icon_custom_emoji_id=btn_emo("link_leaderboard")))
    if link_row:
        markup.add(*link_row)

    # Optional single custom admin-controlled button
    if get_setting("custom_button_enabled") == "1":
        cbtn_text = get_setting("custom_button_text")
        cbtn_link = get_setting("custom_button_link")
        if cbtn_text and cbtn_link:
            markup.add(InlineKeyboardButton(cbtn_text, url=cbtn_link, style="success", icon_custom_emoji_id=btn_emo("custom_admin_button")))
    
    # Admin panel button (if admin) on a new row
    if user_id == ADMIN_USER_ID:
        markup.add(InlineKeyboardButton(" Admin Panel", callback_data="admin_panel", style="success", icon_custom_emoji_id=btn_emo("admin_panel")))

    return markup

def get_admin_panel():
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(
        InlineKeyboardButton(" Product Management", callback_data="admin_cat_product", style="success", icon_custom_emoji_id=btn_emo("admin_cat_product")),
        InlineKeyboardButton(" Keys Management", callback_data="admin_cat_keys", style="success", icon_custom_emoji_id=btn_emo("admin_cat_keys")),
        InlineKeyboardButton(" User Management", callback_data="admin_cat_user", style="success", icon_custom_emoji_id=btn_emo("admin_cat_user")),
        InlineKeyboardButton(" Reseller Management", callback_data="admin_cat_reseller", style="success", icon_custom_emoji_id=btn_emo("admin_cat_reseller")),
        InlineKeyboardButton(" Settings", callback_data="admin_cat_settings", style="success", icon_custom_emoji_id=btn_emo("admin_cat_settings")),
        InlineKeyboardButton(" Announcement", callback_data="admin_announcement", style="success", icon_custom_emoji_id=btn_emo("admin_announcement")),
        InlineKeyboardButton(" Back to Main", callback_data="back_main", style="primary", icon_custom_emoji_id=btn_emo("back_main")),
    )
    return markup

def get_reseller_mgmt_menu():
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(
        InlineKeyboardButton(" Reseller ON/OFF", callback_data="admin_reseller_toggle", style="success", icon_custom_emoji_id=btn_emo("admin_reseller_toggle")),
        InlineKeyboardButton(" Set Reseller Price", callback_data="admin_reseller_setprice", style="success", icon_custom_emoji_id=btn_emo("admin_reseller_setprice")),
        InlineKeyboardButton(" Reseller Buy Settings", callback_data="admin_reseller_buy_menu", style="success", icon_custom_emoji_id=btn_emo("admin_reseller_buy_menu")),
        InlineKeyboardButton(" View Resellers", callback_data="admin_reseller_list", style="success", icon_custom_emoji_id=btn_emo("admin_reseller_list")),
        InlineKeyboardButton(" Ban Reseller", callback_data="admin_reseller_ban", style="danger", icon_custom_emoji_id=btn_emo("admin_reseller_ban")),
        InlineKeyboardButton(" Unban Reseller", callback_data="admin_reseller_unban", style="success", icon_custom_emoji_id=btn_emo("admin_reseller_unban")),
        InlineKeyboardButton(" Back", callback_data="admin_panel", style="primary", icon_custom_emoji_id=btn_emo("admin_panel")),
    )
    return markup

def get_product_mgmt_menu():
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(
        InlineKeyboardButton(" Add Product", callback_data="admin_add_product", style="success", icon_custom_emoji_id=btn_emo("admin_add_product")),
        InlineKeyboardButton(" Edit Product", callback_data="admin_edit_product_all", style="success", icon_custom_emoji_id=btn_emo("admin_edit_product_all")),
        InlineKeyboardButton(" Edit Product Plans/Price", callback_data="admin_edit_product_plans", style="success", icon_custom_emoji_id=btn_emo("admin_edit_product_plans")),
        InlineKeyboardButton(" Delete Product", callback_data="admin_del_product", style="danger", icon_custom_emoji_id=btn_emo("admin_del_product")),
        InlineKeyboardButton(" Add Plan", callback_data="admin_add_plan", style="success", icon_custom_emoji_id=btn_emo("admin_add_plan")),
        InlineKeyboardButton(" Delete Plan", callback_data="admin_del_plan", style="danger", icon_custom_emoji_id=btn_emo("admin_del_plan")),
        InlineKeyboardButton(" Maintenance Mode", callback_data="admin_maintenance_list", style="success", icon_custom_emoji_id=btn_emo("admin_maintenance_list")),
        InlineKeyboardButton(" Back", callback_data="admin_panel", style="primary", icon_custom_emoji_id=btn_emo("admin_panel")),
    )
    return markup

def get_keys_mgmt_menu():
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(
        InlineKeyboardButton(" Add Keys", callback_data="admin_add_keys", style="success", icon_custom_emoji_id=btn_emo("admin_add_keys")),
        InlineKeyboardButton(" List Keys", callback_data="admin_list_keys", style="success", icon_custom_emoji_id=btn_emo("admin_list_keys")),
        InlineKeyboardButton(" Expired/Used Keys", callback_data="admin_expired_keys", style="success", icon_custom_emoji_id=btn_emo("admin_expired_keys")),
        InlineKeyboardButton(" Stock Overview", callback_data="admin_stock_overview", style="success", icon_custom_emoji_id=btn_emo("admin_stock_overview")),
        InlineKeyboardButton(" Delete Key", callback_data="admin_del_key", style="danger", icon_custom_emoji_id=btn_emo("admin_del_key")),
        InlineKeyboardButton(" Back", callback_data="admin_panel", style="primary", icon_custom_emoji_id=btn_emo("admin_panel")),
    )
    return markup

def get_user_mgmt_menu():
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(
        InlineKeyboardButton(" Add Balance", callback_data="admin_add_balance", style="success", icon_custom_emoji_id=btn_emo("admin_add_balance")),
        InlineKeyboardButton(" Remove Balance", callback_data="admin_remove_balance", style="danger", icon_custom_emoji_id=btn_emo("admin_remove_balance")),
        InlineKeyboardButton(" Ban User", callback_data="admin_ban_user", style="danger", icon_custom_emoji_id=btn_emo("admin_ban_user")),
        InlineKeyboardButton(" Unban User", callback_data="admin_unban_user", style="success", icon_custom_emoji_id=btn_emo("admin_unban_user")),
        InlineKeyboardButton(" User Details", callback_data="admin_user_details", style="success", icon_custom_emoji_id=btn_emo("admin_user_details")),
        InlineKeyboardButton(" All Users", callback_data="admin_all_users_0", style="success", icon_custom_emoji_id=btn_emo("admin_all_users_0")),
        InlineKeyboardButton(" Back", callback_data="admin_panel", style="primary", icon_custom_emoji_id=btn_emo("admin_panel")),
    )
    return markup

def get_settings_menu():
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(
        InlineKeyboardButton(" Pending Payments", callback_data="admin_pending_payments", style="success", icon_custom_emoji_id=btn_emo("admin_pending_payments")),
        InlineKeyboardButton(" Set Selling Proof Link", callback_data="admin_set_selling_proof", style="success", icon_custom_emoji_id=btn_emo("admin_set_selling_proof")),
        InlineKeyboardButton(" Leaderboard Settings", callback_data="admin_leaderboard_settings", style="success", icon_custom_emoji_id=btn_emo("admin_leaderboard_settings")),
        InlineKeyboardButton(" Set Update Channel Link", callback_data="admin_set_updatechannel", style="success", icon_custom_emoji_id=btn_emo("admin_set_updatechannel")),
        InlineKeyboardButton(" Set Tutorial Video Link", callback_data="admin_set_tutorial_video", style="success", icon_custom_emoji_id=btn_emo("admin_set_tutorial_video")),
        InlineKeyboardButton(" Custom Menu Button", callback_data="admin_custom_button_menu", style="success", icon_custom_emoji_id=btn_emo("admin_custom_button_menu")),
        InlineKeyboardButton(" Premium Emoji Manager", callback_data="admin_emoji_manager", style="success", icon_custom_emoji_id=btn_emo("admin_emoji_manager")),
        InlineKeyboardButton(" Set Order Notify Text", callback_data="admin_set_order_notify", style="success", icon_custom_emoji_id=btn_emo("admin_set_order_notify")),
        InlineKeyboardButton(" Set Support Text", callback_data="admin_set_support_text", style="success", icon_custom_emoji_id=btn_emo("admin_set_support_text")),
        InlineKeyboardButton(" Set Store Highlights Text", callback_data="admin_set_highlights", style="success", icon_custom_emoji_id=btn_emo("admin_set_highlights")),
        InlineKeyboardButton(" External API Settings", callback_data="admin_external_api_settings", style="success", icon_custom_emoji_id=btn_emo("admin_external_api_settings")),
        InlineKeyboardButton(" Payment Gateway Settings", callback_data="admin_payment_gateways", style="success", icon_custom_emoji_id=btn_emo("admin_payment_gateways")),
        InlineKeyboardButton("🔗 Product Share Links", callback_data="admin_product_share_links", style="success"),
        InlineKeyboardButton(" Stats", callback_data="admin_stats", style="success", icon_custom_emoji_id=btn_emo("admin_stats")),
        InlineKeyboardButton(" Referral Settings", callback_data="admin_referral_settings", style="success", icon_custom_emoji_id=btn_emo("admin_referral_settings")),
        InlineKeyboardButton(" Spin Settings", callback_data="admin_spin_settings", style="success", icon_custom_emoji_id=btn_emo("admin_spin_settings")),
        InlineKeyboardButton(
            "🛠 Maintenance: ON (bot band hai)" if is_maintenance_on() else "🛠 Maintenance: OFF (bot chalu hai)",
            callback_data="admin_maintenance_toggle"
        , style="danger", icon_custom_emoji_id=btn_emo("admin_maintenance_toggle")),
        InlineKeyboardButton(
            "💲 USD Display: ON" if is_usd_display_on() else "💲 USD Display: OFF",
            callback_data="admin_usd_toggle"
        , style=("success" if is_usd_display_on() else "danger"), icon_custom_emoji_id=btn_emo("admin_usd_toggle")),
        InlineKeyboardButton(" Back", callback_data="admin_panel", style="primary", icon_custom_emoji_id=btn_emo("admin_panel")),
    )
    return markup

def get_external_api_settings_menu():
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(
        InlineKeyboardButton(" Set API Endpoint URL", callback_data="admin_set_external_api_url", style="success", icon_custom_emoji_id=btn_emo("admin_set_external_api_url")),
        InlineKeyboardButton(" Set API Key", callback_data="admin_set_external_api_key", style="success", icon_custom_emoji_id=btn_emo("admin_set_external_api_key")),
        InlineKeyboardButton(" Set Master Key", callback_data="admin_set_external_api_masterkey", style="success", icon_custom_emoji_id=btn_emo("admin_set_external_api_masterkey")),
        InlineKeyboardButton(" Back", callback_data="admin_cat_settings", style="primary", icon_custom_emoji_id=btn_emo("admin_cat_settings")),
    )
    return markup

def get_external_api_settings_text():
    url = get_reseller_api_url()
    key_status = "✅ Set" if get_reseller_api_key() else "❌ Not set"
    mkey_status = "✅ Set" if get_reseller_master_key() else "❌ Not set"
    kp_url = get_keyspanels_api_url()
    kp_mkey_status = "✅ Set" if get_keyspanels_master_key() else "❌ Not set"
    return (
        "🔌 <b>External API Settings</b>\\n\\n"
        "Dono external APIs independent hain. Har product ke Edit Product hub mein decide hoga "
        "ki <b>Bunty</b> ya <b>KeysPanelShop</b> use karna hai.\\n\\n"
        "─────────────\\n"
        "🟢 <b>Bunty / bantibhaiya.to</b> (legacy)\\n"
        f"🌐 Endpoint: <code>{html.escape(url)}</code>\\n"
        f"🔑 API Key: {key_status}\\n"
        f"🔒 Master Key: {mkey_status}\\n\\n"
        "─────────────\\n"
        "🟣 <b>KeysPanelShop</b> (new)\\n"
        f"🌐 Endpoint: <code>{html.escape(kp_url)}</code>\\n"
        f"🔒 Master Key: {kp_mkey_status}\\n\\n"
        "Format: <code>action=buy</code> + <code>variant_id</code> + <code>quantity=1</code> "
        "+ <code>x-master-key</code> header.\\n"
        "Per-product ON/OFF + provider + PID/Variant ID product Edit hub mein set hota hai."
    )

def get_external_api_settings_menu():
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(
        InlineKeyboardButton("🟢 Bunty: Set Endpoint URL", callback_data="admin_set_external_api_url", style="success", icon_custom_emoji_id=btn_emo("admin_set_external_api_url")),
        InlineKeyboardButton("🟢 Bunty: Set API Key", callback_data="admin_set_external_api_key", style="success", icon_custom_emoji_id=btn_emo("admin_set_external_api_key")),
        InlineKeyboardButton("🟢 Bunty: Set Master Key", callback_data="admin_set_external_api_masterkey", style="success", icon_custom_emoji_id=btn_emo("admin_set_external_api_masterkey")),
        InlineKeyboardButton("🟣 KeysPanel: Set Endpoint URL", callback_data="admin_set_keyspanels_url", style="success", icon_custom_emoji_id=btn_emo("admin_set_external_api_url")),
        InlineKeyboardButton("🟣 KeysPanel: Set Master Key", callback_data="admin_set_keyspanels_masterkey", style="success", icon_custom_emoji_id=btn_emo("admin_set_external_api_masterkey")),
        InlineKeyboardButton(" Back", callback_data="admin_cat_settings", style="primary", icon_custom_emoji_id=btn_emo("admin_cat_settings")),
    )
    return markup

def get_payment_gateway_settings_text():
    zap_status = "🟢 ON (users ko dikhega)" if is_zapupi_enabled() else "🔴 OFF (hidden)"
    fam_status = "🟢 ON (users ko dikhega)" if is_fampay_enabled() else "🔴 OFF (hidden)"
    return (
        "💳 <b>Payment Gateway Settings</b>\n\n"
        "Dono gateway independently ON/OFF kar sakte ho. Jab dono ON honge, user ko "
        "➕ Add Balance par dono options dikhenge; sirf ek ON hone par seedha wahi use hoga; "
        "dono OFF hone par Add Balance temporarily unavailable dikhega.\n\n"
        "─────────────\n"
        f"🏦 <b>ZapUPI</b> — {zap_status}\n"
        f"🔑 Zap Key: {_mask_key(get_zapupi_key())}\n"
        "─────────────\n"
        f"💵 <b>FamPay</b> — {fam_status}\n"
        f"🔑 API Key: {_mask_key(get_fampay_api_key())}\n"
        f"🏦 UPI ID: {get_fampay_upi_id() or '❌ Not set'}\n"
        "─────────────"
    )

def get_payment_gateway_settings_menu():
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(
        InlineKeyboardButton(
            "🏦 ZapUPI: ON (chalu)" if is_zapupi_enabled() else "🏦 ZapUPI: OFF (band)",
            callback_data="admin_zapupi_toggle",
            style=("success" if is_zapupi_enabled() else "danger"),
            icon_custom_emoji_id=btn_emo("admin_zapupi_toggle"),
        ),
        InlineKeyboardButton(" Set ZapUPI Zap Key", callback_data="admin_set_zapupi_key", style="success", icon_custom_emoji_id=btn_emo("admin_set_zapupi_key")),
        InlineKeyboardButton(
            "💵 FamPay: ON (chalu)" if is_fampay_enabled() else "💵 FamPay: OFF (band)",
            callback_data="admin_fampay_toggle",
            style=("success" if is_fampay_enabled() else "danger"),
            icon_custom_emoji_id=btn_emo("admin_fampay_toggle"),
        ),
        InlineKeyboardButton(" Set FamPay API Key", callback_data="admin_set_fampay_key", style="success", icon_custom_emoji_id=btn_emo("admin_set_fampay_key")),
        InlineKeyboardButton(" Set FamPay UPI ID", callback_data="admin_set_fampay_upi", style="success", icon_custom_emoji_id=btn_emo("admin_set_fampay_upi")),
        InlineKeyboardButton(" FamPay Debug (raw API response)", callback_data="admin_fampay_debug", style="danger", icon_custom_emoji_id=btn_emo("admin_fampay_debug")),
        InlineKeyboardButton(" Back", callback_data="admin_cat_settings", style="primary", icon_custom_emoji_id=btn_emo("admin_cat_settings")),
    )
    return markup

def get_fampay_debug_text():
    last_create = get_setting("fampay_debug_last_create") or "Abhi tak koi FamPay order create nahi hua."
    last_check = get_setting("fampay_debug_last_check") or "Abhi tak koi Check Status nahi dabaya gaya."
    return (
        "🐞 <b>FamPay Debug — Raw API Response</b>\n\n"
        "Ye wahi raw JSON hai jo FamPay ke server se seedha aaya tha, bina kisi "
        "processing ke. Agar FamPay galat kaam kar raha hai, to yahan se copy "
        "karke developer ko bhejo — isi se pata chalega ki API ka actual field "
        "format kya hai.\n\n"
        "─────────────\n"
        "📤 <b>Last Create-Order response:</b>\n"
        f"<code>{html.escape(str(last_create))}</code>\n\n"
        "─────────────\n"
        "📥 <b>Last Check-Status response:</b>\n"
        f"<code>{html.escape(str(last_check))}</code>"
    )

# ======================= VERIFICATION PROMPT =======================
# ======================= BOT HANDLERS =======================
@bot.message_handler(commands=['start'])
def send_welcome(message):
    uid = message.from_user.id
    username = message.from_user.username
    first_name = message.from_user.first_name or "user"
    delete_active_demo_video(message.chat.id)

    def _welcome_db_write(conn):
        cursor = conn.cursor()
        cursor.execute("INSERT OR IGNORE INTO users (id, username, first_name, balance, banned, verified, contact_gate_cycle) VALUES (?,?,?,0,0,0,?)",
                       (uid, username, first_name, CONTACT_GATE_CYCLE))
        cursor.execute("UPDATE users SET username=?, first_name=?, last_active=? WHERE id=?", (username, first_name, ist_today_str(), uid))
        cursor.close()
    sqlite_write_with_retry(_welcome_db_write)
    display_name = first_name
    parts=(message.text or "").split(maxsplit=1)
    start_payload = parts[1].strip() if len(parts)>1 else ""
    process_referral_start(uid, start_payload)

    # Product deep-link: /start product_<ID> (optionally with referral payload).
    product_match = re.match(r"^product_(\d+)(?:_ref_([A-Za-z0-9_-]+))?$", start_payload)
    pending_product_id = int(product_match.group(1)) if product_match else None
    if pending_product_id:
        try:
            conn = get_db()
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM products WHERE id=?", (pending_product_id,))
            valid_product = cursor.fetchone()
            cursor.close()
            conn.close()
            if not valid_product:
                pending_product_id = None
        except Exception:
            pending_product_id = None
    if pending_product_id:
        user_states.setdefault(uid, {})["pending_product_id"] = pending_product_id

    # Contact verification is permanent and is read from the users table.
    # Only users whose `verified` flag is still 0 are asked to share a contact.
    # Once a valid contact is stored, restarting/redeploying the bot will NOT
    # ask that user again.
    if uid != ADMIN_USER_ID:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT verified, phone_number FROM users WHERE id=?", (uid,))
        gate_row = cursor.fetchone()
        cursor.close()
        conn.close()

        verified = bool(gate_row and int(gate_row[0] or 0) == 1)
        phone_saved = bool(gate_row and (gate_row[1] or '').strip())

        # Treat either a previous verified flag OR an already stored phone as
        # completed verification. This also protects existing databases where
        # the phone was saved but the old flag was not updated correctly.
        if not (verified or phone_saved):
            markup = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
            markup.add(KeyboardButton("📱 Share Contact & Start", request_contact=True))
            bot.send_message(
                message.chat.id,
                "🔐 <b>Bot start karne ke liye ek baar apna Telegram contact share karo.</b>\n\n"
                "Jinka contact database mein verified hai unse dobara contact nahi maanga jayega.\n"
                "👇 Neeche <b>Share Contact & Start</b> dabao.",
                parse_mode="HTML", reply_markup=markup
            )
            return

    if is_maintenance_on() and uid != ADMIN_USER_ID:
        bot.send_message(
            message.chat.id,
            "🛠 <b>Bot Under Maintenance</b>\n\nHum kuch improvements kar rahe hain. Please thodi der baad wapas try karo. 🙏",
            parse_mode="HTML"
        )
        return

    pending_product_id = user_states.get(uid, {}).pop("pending_product_id", None)
    if pending_product_id:
        open_product_from_start(message, pending_product_id)
        return
    bot.send_message(message.chat.id, get_main_menu_text(display_name, uid), parse_mode="HTML", reply_markup=get_main_menu(uid))

@bot.message_handler(content_types=['contact'])
def handle_contact(message):
    uid=message.from_user.id
    contact=message.contact
    # Telegram clients normally include contact.user_id for request_contact.
    # Some clients/proxies omit it; in that case still accept the contact because
    # it came through our request_contact keyboard. If an explicit user_id is
    # present and belongs to somebody else, reject it.
    if not contact:
        bot.send_message(message.chat.id, "❌ Contact share karo.")
        return
    shared_uid=getattr(contact, 'user_id', None)
    if shared_uid not in (None, 0) and int(shared_uid) != int(uid):
        bot.send_message(message.chat.id, "❌ Apna hi Telegram contact share karo.")
        return
    phone=(getattr(contact, 'phone_number', '') or '').strip()
    if not phone:
        bot.send_message(message.chat.id, "❌ Phone number receive nahi hua. Share Contact button se dobara try karo.")
        return
    conn=get_db(); cur=conn.cursor()
    cur.execute("UPDATE users SET verified=1, phone_number=?, contact_gate_cycle=NULL WHERE id=?", (phone, uid))
    conn.commit(); cur.close(); conn.close()
    try:
        bot.delete_message(message.chat.id, message.message_id)
    except Exception:
        pass
    try:
        bot.send_message(message.chat.id, "✅ Contact verified! Bot start ho gaya. 👇", reply_markup=telebot.types.ReplyKeyboardRemove())
    except Exception:
        pass
    name=message.from_user.first_name or "user"
    pending_product_id = user_states.get(uid, {}).pop("pending_product_id", None)
    if pending_product_id:
        open_product_from_start(message, pending_product_id)
        return
    bot.send_message(message.chat.id, get_main_menu_text(name, uid), parse_mode="HTML", reply_markup=get_main_menu(uid))

@bot.message_handler(content_types=['video'])
def handle_video(message):
    user_id = message.from_user.id
    state = user_states.get(user_id, {})
    if user_id == ADMIN_USER_ID and state.get("action") == "awaiting_prod_video":
        prod_id = state.get("prod_id")
        file_id = message.video.file_id
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("UPDATE products SET video_file_id=? WHERE id=?", (file_id, prod_id))
        conn.commit()
        cursor.close()
        conn.close()
        user_states.pop(user_id, None)
        bot.reply_to(message, "✅ Demo video saved for this product!")

@bot.message_handler(content_types=['photo'])
def handle_photo(message):
    """Payments no longer need a screenshot -- ZapUPI confirms automatically via
    webhook (see webhook_server.py) and the '🔄 Check Status' button on the payment
    message can force a check. This handler just avoids a photo being silently
    ignored, and still lets the admin attach a demo video-adjacent photo... actually
    demo media is video-only, so this simply informs the sender."""
    user_id = message.from_user.id
    state = user_states.get(user_id, {})
    if state.get("action") == "awaiting_prod_video":
        bot.reply_to(message, "🎬 Ye ek video field hai, photo nahi. Product ka demo video bhejo.")
        return
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM pending_payments WHERE user_id=? AND status='pending' ORDER BY id DESC LIMIT 1", (user_id,))
    pending = cursor.fetchone()
    cursor.close()
    conn.close()
    if pending:
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton(" Check Payment Status", callback_data=f"zapcheck_{pending[0]}", style="success", icon_custom_emoji_id=btn_emo("zapcheck")))
        bot.reply_to(message, "ℹ️ Ab payment screenshot bhejne ki zaroorat nahi hai — payment link se pay karte hi balance apne aap add ho jaata hai. Neeche button dabao status check karne ke liye:", reply_markup=markup)
    else:
        bot.reply_to(message, "ℹ️ No pending payment found. Tap ➕ Add Balance first.")


# ===== Product Share Links =====
# This feature only creates Telegram deep-links and opens the existing product
# plan screen. Payment gateways and reseller APIs are not changed.
def make_product_share_link(bot_obj, product_id, referral_code=None):
    try:
        username = bot_obj.get_me().username
    except Exception:
        username = None
    if not username:
        return None
    payload = f"product_{int(product_id)}"
    if referral_code:
        safe_ref = re.sub(r"[^A-Za-z0-9_-]", "", str(referral_code))
        if safe_ref:
            payload += f"_ref_{safe_ref}"
    return f"https://t.me/{username}?start={payload}"

def get_product_share_links_menu():
    markup = InlineKeyboardMarkup(row_width=1)
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id, name FROM products ORDER BY sort_order, id")
    products = cursor.fetchall()
    cursor.close()
    conn.close()
    if not products:
        markup.add(InlineKeyboardButton("◀️ Back", callback_data="admin_cat_settings", style="primary"))
        return markup
    for pid, name in products:
        markup.add(InlineKeyboardButton(
            f"🔗 {name}", callback_data=f"admin_share_product_{int(pid)}", style="success"
        ))
    markup.add(InlineKeyboardButton("◀️ Back", callback_data="admin_cat_settings", style="primary"))
    return markup

def open_product_from_start(message, product_id):
    """Open the bot's existing product-plan screen from a /start deep-link."""
    try:
        product_id = int(product_id)
    except Exception:
        return False
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM products WHERE id=?", (product_id,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    if not row:
        return False
    # Open the existing product plan screen directly. Do NOT send the main
    # Shop/menu screen or a temporary "Product load..." message.
    placeholder = bot.send_message(message.chat.id, "🛒")
    fake_call = SimpleNamespace(
        from_user=message.from_user,
        message=placeholder
    )
    show_product_plans(fake_call, product_id)
    return True

@bot.callback_query_handler(func=lambda call: True)
def callback_handler(call):
    try:
        _callback_handler_impl(call)
    except Exception as e:
        err_text = str(e)
        # Telegram raises "message is not modified" whenever a tap would redraw a
        # screen with EXACTLY the same text + buttons that are already showing --
        # e.g. a double-tap, a slow tap that Telegram's client retried, or tapping
        # a button that leads back into the exact screen you're already on. This
        # is completely harmless (nothing is actually wrong), so we just quietly
        # acknowledge the tap instead of scaring the user with an error popup.
        if "message is not modified" in err_text.lower():
            try:
                bot.answer_callback_query(call.id)
            except:
                pass
            return
        # "query is too old" / "query id is invalid" happens if the tap sat too
        # long before we could respond (slow network) -- also harmless, nothing
        # to show the user since Telegram already invalidated the tap itself.
        if "query is too old" in err_text.lower() or "query id is invalid" in err_text.lower():
            return
        # Any other, genuinely unexpected error: log full details (not just the
        # message) so it's actually possible to diagnose from the VPS console/logs,
        # let the admin know it happened, and only THEN show the user a popup.
        print(f"⚠️ Callback handler error (bot kept running): {e}")
        traceback.print_exc()
        try:
            bot.send_message(ADMIN_USER_ID, f"⚠️ <b>Bot Error</b>\n\nUser: {call.from_user.id}\nAction: <code>{html.escape(call.data or '')}</code>\nError: <code>{html.escape(err_text[:500])}</code>", parse_mode="HTML")
        except:
            pass
        try:
            bot.answer_callback_query(call.id, "⚠️ Something went wrong. Try again.", show_alert=True)
        except:
            pass

def _callback_handler_impl(call):
    user_id = call.from_user.id

    if is_banned(user_id) and call.data not in ["support", "back_main", "profile"]:
        bot.answer_callback_query(call.id, " You are banned. Contact support.", show_alert=True)
        return

    if is_maintenance_on() and user_id != ADMIN_USER_ID:
        bot.answer_callback_query(call.id, "🛠 Bot abhi maintenance mode mein hai. Thodi der baad try karo.", show_alert=True)
        return

    chat_id = call.message.chat.id
    msg_id = call.message.message_id
    data = call.data
    if data == "referral":
        show_referral(call, call.from_user.id); return
    if data == "spin":
        show_spin(call, call.from_user.id); return
    if data == "spin_now":
        perform_spin(call, call.from_user.id); return
    if data == "leaderboard":
        show_leaderboard(call); return
    # Compatibility aliases for leaderboard settings buttons from earlier builds.
    if data in ("leaderboard_settings", "admin_leaderboard_emoji_settings", "leaderboard_emoji_settings") and user_id == ADMIN_USER_ID:
        if data != "leaderboard_settings":
            show_emoji_slots(chat_id, msg_id, "Leaderboard")
        else:
            _show_leaderboard_admin_settings(chat_id, msg_id)
        try: bot.answer_callback_query(call.id)
        except: pass
        return

    # Any button press cancels whatever "waiting for a text reply" state was active
    # (e.g. "send new description"). This means every existing " Back"/"❌ Cancel"
    # button in the bot now safely gets the admin/user out of an accidental flow,
    # instead of their next message being silently swallowed as data.
    try:
        bot.clear_step_handler_by_chat_id(chat_id)
    except:
        pass
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET last_active=? WHERE id=?", (ist_today_str(), user_id))
        conn.commit()
        cursor.close()
        conn.close()
    except:
        pass

    # ----- Main Menu -----
    if data == "shop_now":
        delete_active_demo_video(chat_id)
        show_products(call)
    elif data == "profile":
        show_profile(call, user_id)
    elif data == "add_balance":
        show_add_balance_gateway_choice(call)
    elif data == "gateway_upi":
        ask_amount(call, gateway="zapupi")
    elif data == "gateway_fampay":
        ask_amount(call, gateway="fampay")
    elif data == "history":
        show_history(call, user_id)
    elif data.startswith("myorder_prod_"):
        # Legacy callback kept compatible with older menus.
        prod_id = int(data.split("_")[-1])
        show_order_category(call, user_id, str(prod_id))
    elif data.startswith("orderprod_"):
        # Current My Orders product/category buttons.
        anchor = data[len("orderprod_"):]
        show_order_category(call, user_id, anchor)
    elif data == "tutorial":
        tutorial_text = (
            " <b>HOW TO USE BOT</b> \n"
            "─────────────\n"
            f"{emo('tut_step1')} Step 1 → /start karke menu kholo\n"
            f"{emo('tut_step2')} Step 2 → Shop mein jaake product chuno\n"
            f"{emo('tut_step3')} Step 3 → Add Balance karke fund badao\n"
            f"{emo('tut_step4')} Step 4 → Product kharido, key milegi\n"
            f"{emo('tut_step5')} Step 5 → Video Tutorial dekho 👇\n"
            "─────────────\n\n"
            f"{emo('tut_warn')} Koi problem?\n"
            f"{emo('tut_owner')} {OWNER_USERNAME}\n"
            f"{emo('tut_fast')} Fast Reply • 24x7"
        )
        markup = InlineKeyboardMarkup(row_width=1)
        tutorial_video_link = get_setting("tutorial_video_link")
        if tutorial_video_link:
            markup.add(InlineKeyboardButton(" Watch Tutorial Video", url=tutorial_video_link, style="success", icon_custom_emoji_id=btn_emo("link_watch_tutorial_video")))
        markup.add(InlineKeyboardButton(" Back", callback_data="back_main", style="primary", icon_custom_emoji_id=btn_emo("back_main")))
        bot.edit_message_text(tutorial_text, chat_id, msg_id, parse_mode="HTML", reply_markup=markup)
    elif data == "support":
        support_text = build_support_text()
        support_markup = InlineKeyboardMarkup(row_width=1)
        support_markup.add(InlineKeyboardButton(" Chat on Telegram", url=f"https://t.me/{OWNER_USERNAME.lstrip('@')}", style="success", icon_custom_emoji_id=btn_emo("link_chat_on_telegram")))
        support_markup.add(InlineKeyboardButton(" Back to Home", callback_data="back_main", style="primary", icon_custom_emoji_id=btn_emo("back_main")))
        bot.edit_message_text(support_text, chat_id, msg_id, parse_mode="HTML", reply_markup=support_markup)
    elif data == "back_main":
        delete_active_demo_video(chat_id)
        display_name = call.from_user.first_name or "user"
        bot.edit_message_text(get_main_menu_text(display_name, user_id), chat_id, msg_id, parse_mode="HTML", reply_markup=get_main_menu(user_id))

    # ----- Self-serve Reseller Purchase -----
    elif data == "become_reseller":
        show_become_reseller_screen(call, user_id)
    elif data == "reseller_buy_confirm":
        process_reseller_buy(call, user_id)

    # ----- Shop / Products (direct purchase, no discount) -----
    elif data.startswith("product_"):
        product_id = int(data.split("_")[1])
        show_product_plans(call, product_id)
    elif data.startswith("buy_"):
        parts = data.split("_")
        if len(parts) == 3:
            _, product_id, plan_id = parts
            product_id, plan_id = int(product_id), int(plan_id)
            conn = get_db()
            cursor = conn.cursor()
            cursor.execute("SELECT requires_android_id, external_api_enabled, external_api_provider, reseller_product_id FROM products WHERE id=?", (product_id,))
            prow = cursor.fetchone()
            cursor.close()
            conn.close()
            if prow and prow[1] and get_external_api_provider(prow[2]) == "keyspanels":
                create_keyspanels_order(call.from_user.id, call.message.chat.id, call.from_user.first_name,
                                        call.from_user.username, product_id, plan_id)
            elif prow and prow[0]:
                # Android-ID products collect the ID first; create_manual_order then
                # uses the configured provider (Bunty) and charges only after success.
                ask_android_id(call, product_id, plan_id)
            elif prow and prow[1] and get_external_api_provider(prow[2]) == "bunty" and prow[3]:
                create_manual_order(call.from_user.id, call.message.chat.id, call.from_user.first_name,
                                     call.from_user.username, product_id, plan_id, "")
            else:
                process_purchase(call.from_user.id, call.message.chat.id, call.from_user.first_name,
                                  call.from_user.username, product_id, plan_id,
                                  msg_id=call.message.message_id, call_id=call.id)
    elif data.startswith("watchvid_"):
        product_id = int(data.split("_")[1])
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT video_file_id, video_caption, video_caption_entities FROM products WHERE id=?", (product_id,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if row and row[0]:
            delete_active_demo_video(chat_id)
            vid_caption = render_text_with_custom_emoji(row[1], row[2]) if row[1] else "🎬 Demo video"
            vid_msg = bot.send_video(chat_id, row[0], caption=vid_caption, parse_mode="HTML")
            active_demo_video[chat_id] = vid_msg.message_id
        else:
            bot.answer_callback_query(call.id, "No video available.")
    elif data.startswith("notifyme_"):
        product_id = int(data.split("_")[1])
        now_subscribed = toggle_notify_subscription(product_id, user_id)
        if now_subscribed:
            bot.answer_callback_query(call.id, "🔔 Done! Ye product available hote hi aapko turant message mil jaayega.", show_alert=True)
        else:
            bot.answer_callback_query(call.id, "🔕 Notify hata diya gaya.", show_alert=True)
        show_product_plans(call, product_id)
    elif data.startswith("notifyskip_") and user_id == ADMIN_USER_ID:
        prod_id = int(data.split("_")[1])
        user_states.pop(user_id, None)
        send_notify_broadcast(prod_id, chat_id)
    # Services (demo)
    elif data.startswith("svc_"):
        service_name = data.replace("svc_", "").replace("_", " ").title()
        markup = InlineKeyboardMarkup()
        markup.add(
            InlineKeyboardButton(" Buy Now - ₹499", callback_data="demo_buy", style="success", icon_custom_emoji_id=btn_emo("demo_buy")),
            InlineKeyboardButton(" Back", callback_data="shop_now", style="primary", icon_custom_emoji_id=btn_emo("shop_now"))
        )
        bot.edit_message_text(f" <b>{service_name}</b>\n\nPrice: ₹499\nDelivery: Instant\n\nClick buy to purchase:", chat_id, msg_id, parse_mode="HTML", reply_markup=markup)
    elif data == "demo_buy":
        bot.edit_message_text(" <b>Demo Purchase Successful!</b>\nKey: DEMO-KEY-123", chat_id, msg_id, parse_mode="HTML")

    # ----- Admin Panel -----
    elif data == "admin_panel" and user_id == ADMIN_USER_ID:
        bot.edit_message_text(" <b>Admin Panel</b>", chat_id, msg_id, parse_mode="HTML", reply_markup=get_admin_panel())
    elif data.startswith("admin_"):
        handle_admin_actions(call, data)
    elif data.startswith("editprod_"):
        prod_id = int(data.split("_")[1])
        bot.edit_message_text("📝 Send new product name:", chat_id, msg_id)
        bot.register_next_step_handler(call.message, lambda m: edit_product_name(m, prod_id))
    elif data.startswith("delprod_"):
        if user_id != ADMIN_USER_ID:
            bot.answer_callback_query(call.id, "Admin only.", show_alert=True)
            return
        try:
            prod_id = int(data.split("_", 1)[1])
        except (TypeError, ValueError):
            bot.answer_callback_query(call.id, "Invalid product.", show_alert=True)
            return

        def _write(conn):
            cur = conn.cursor()
            cur.execute("SELECT id FROM products WHERE id=?", (prod_id,))
            if not cur.fetchone():
                cur.close()
                return 0
            # Keep historical keys/orders. Only the live product row is removed.
            cur.execute("DELETE FROM products WHERE id=?", (prod_id,))
            changed = cur.rowcount
            cur.close()
            return changed

        try:
            changed = sqlite_write_with_retry(_write)
        except Exception as e:
            print(f"[DB DELETE PRODUCT] {e}")
            bot.answer_callback_query(call.id, "❌ Database save failed. Try again.", show_alert=True)
            return

        if not changed:
            bot.answer_callback_query(call.id, "⚠️ Product already deleted / not found.", show_alert=True)
            return

        bot.answer_callback_query(call.id, "✅ Product deleted.")
        bot.edit_message_text("✅ <b>Product deleted successfully.</b>", chat_id, msg_id, parse_mode="HTML")
    elif data.startswith("delplan_page_"):
        if user_id != ADMIN_USER_ID:
            bot.answer_callback_query(call.id, "Admin only.", show_alert=True)
            return
        try:
            page = int(data.rsplit("_", 1)[1])
        except (TypeError, ValueError):
            page = 0
        show_delete_plan_page(chat_id, msg_id, page)
        try:
            bot.answer_callback_query(call.id)
        except Exception:
            pass
    elif data.startswith("delplan_"):
        if user_id != ADMIN_USER_ID:
            bot.answer_callback_query(call.id, "Admin only.", show_alert=True)
            return
        try:
            plan_id = int(data.split("_", 1)[1])
        except (TypeError, ValueError):
            bot.answer_callback_query(call.id, "Invalid plan.", show_alert=True)
            return

        def _write(conn):
            cur = conn.cursor()
            cur.execute("SELECT id FROM plans WHERE id=?", (plan_id,))
            if not cur.fetchone():
                cur.close()
                return 0
            cur.execute("DELETE FROM plans WHERE id=?", (plan_id,))
            changed = cur.rowcount
            cur.close()
            return changed

        try:
            changed = sqlite_write_with_retry(_write)
        except Exception as e:
            print(f"[DB DELETE PLAN] {e}")
            bot.answer_callback_query(call.id, "❌ Database save failed. Try again.", show_alert=True)
            return

        if not changed:
            bot.answer_callback_query(call.id, "Plan already deleted / not found.", show_alert=True)
            show_delete_plan_page(chat_id, msg_id, 0)
            return

        bot.answer_callback_query(call.id, "Plan deleted.")
        bot.edit_message_text(
            "✅ <b>Plan deleted successfully.</b>", chat_id, msg_id,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton("🗑️ Delete Another Plan", callback_data="admin_del_plan",
                                     style="danger", icon_custom_emoji_id=btn_emo("admin_del_plan")),
                InlineKeyboardButton("◀️ Back", callback_data="admin_cat_product",
                                     style="primary", icon_custom_emoji_id=btn_emo("admin_cat_product"))
            )
        )
    elif data.startswith("addkeys_prod_"):
        prod_id = int(data.split("_")[2])
        user_states[user_id] = {"action": "add_keys_select_plan", "prod_id": prod_id}
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT id, days, price, duration_unit FROM plans WHERE product_id=? ORDER BY (CASE WHEN duration_unit='hours' THEN days ELSE days*24 END)", (prod_id,))
        plans = cursor.fetchall()
        cursor.close()
        conn.close()
        if not plans:
            bot.edit_message_text("⚠️ No plans. Add a plan first.", chat_id, msg_id)
            return
        markup = InlineKeyboardMarkup()
        for pl in plans:
            markup.add(InlineKeyboardButton(f" {format_duration(pl[1], pl[3])} - ₹{pl[2]}", callback_data=f"addkeys_plan_{pl[0]}", style="success", icon_custom_emoji_id=btn_emo("addkeys_plan")))
        markup.add(InlineKeyboardButton(" Cancel", callback_data="admin_panel", style="danger", icon_custom_emoji_id=btn_emo("admin_panel")))
        bot.edit_message_text("🔑 Select plan:", chat_id, msg_id, reply_markup=markup)
    elif data.startswith("addkeys_plan_"):
        plan_id = int(data.split("_")[2])
        user_states[user_id] = {"action": "add_keys_final", "prod_id": user_states.get(user_id, {}).get("prod_id"), "plan_id": plan_id}
        bot.edit_message_text("📝 Send keys (one per line):", chat_id, msg_id)
        bot.register_next_step_handler(call.message, receive_keys)
    elif data.startswith("approve_pay_") and user_id == ADMIN_USER_ID:
        pay_id = int(data.split("_")[2])
        approve_payment(call, pay_id)
    elif data.startswith("amtdigit_"):
        state = user_states.get(user_id, {})
        if state.get("action") != "custom_amount":
            bot.answer_callback_query(call.id)
            return
        digit = data.split("_", 1)[1]
        digits = state.get("digits", "")
        if len(digits) < 5:
            digits += digit
        state["digits"] = digits
        user_states[user_id] = state
        bot.edit_message_text(amount_keypad_text(digits, state.get("gateway", "zapupi")), chat_id, msg_id, parse_mode="HTML", reply_markup=build_amount_keypad(digits))
        bot.answer_callback_query(call.id)
    elif data == "amtdelete":
        state = user_states.get(user_id, {})
        if state.get("action") != "custom_amount":
            bot.answer_callback_query(call.id)
            return
        digits = state.get("digits", "")[:-1]
        state["digits"] = digits
        user_states[user_id] = state
        bot.edit_message_text(amount_keypad_text(digits, state.get("gateway", "zapupi")), chat_id, msg_id, parse_mode="HTML", reply_markup=build_amount_keypad(digits))
        bot.answer_callback_query(call.id)
    elif data == "amtback":
        user_states.pop(user_id, None)
        display_name = call.from_user.first_name or "user"
        bot.edit_message_text(get_main_menu_text(display_name, user_id), chat_id, msg_id, parse_mode="HTML", reply_markup=get_main_menu(user_id))
    elif data == "amtconfirm":
        state = user_states.get(user_id, {})
        if state.get("action") != "custom_amount":
            bot.answer_callback_query(call.id)
            return
        digits = state.get("digits", "")
        if not digits:
            bot.answer_callback_query(call.id, "❌ Pehle amount type karo.", show_alert=True)
            return
        amount = int(digits)
        if amount < 5:
            bot.answer_callback_query(call.id, "❌ Minimum ₹5", show_alert=True)
            return
        if amount > 5000:
            bot.answer_callback_query(call.id, "❌ Maximum ₹5,000 per request. Contact admin for larger amounts.", show_alert=True)
            return
        gateway = state.get("gateway", "zapupi")
        user_states.pop(user_id, None)
        bot.answer_callback_query(call.id)
        try:
            bot.edit_message_text(f"{emo('qr_pay')} <b>Generating your payment link for ₹{amount}...</b>", chat_id, msg_id, parse_mode="HTML")
        except:
            pass
        process_deposit_amount(chat_id, user_id, amount, gateway)
    elif data.startswith("zapcheck_"):
        pay_id = int(data.split("_")[1])
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT order_id, status, gateway, fampay_order_id FROM pending_payments WHERE id=? AND user_id=?", (pay_id, user_id))
        prow = cursor.fetchone()
        cursor.close()
        conn.close()
        if not prow:
            bot.answer_callback_query(call.id, "❌ Order not found.", show_alert=True)
            return
        order_id, status, gateway, fampay_order_id = prow[0], prow[1], (prow[2] or "zapupi"), prow[3]
        if status == "success":
            bot.answer_callback_query(call.id, "✅ Payment already credited!", show_alert=True)
            return
        if status in ("cancelled", "expired", "failed"):
            bot.answer_callback_query(call.id, f"⚠️ Ye payment {status} hai. Dobara Add Balance se try karo.", show_alert=True)
            return
        if gateway == "fampay":
            fdata = check_fampay_order_status(fampay_order_id or order_id)
            # Same note as the reconcile thread: check_fampay_order_status() only
            # returns data when the payment is already confirmed, and that data
            # has no inner "status" field -- so just check it's truthy.
            if fdata:
                credit_pending_payment(pay_id, txn_id=fdata.get("transaction_id") or fdata.get("txn_id"), utr=fdata.get("utr"))
                bot.answer_callback_query(call.id, "✅ Payment confirmed! Balance added.", show_alert=True)
            else:
                bot.answer_callback_query(call.id, "⏳ Payment Nhi hua Pay Karke Check Status Per Click kro Balance Ad Hojyga.", show_alert=True)
        else:
            zdata = check_zapupi_order_status(order_id)
            if zdata and str(zdata.get("status", "")).lower() == "success":
                credit_pending_payment(pay_id, txn_id=zdata.get("txn_id"), utr=zdata.get("utr"))
                bot.answer_callback_query(call.id, "✅ Payment confirmed! Balance added.", show_alert=True)
            else:
                bot.answer_callback_query(call.id, "⏳ Payment Nhi hua Pay Karke Check Status Per Click kro Balance Ad Hojyga.", show_alert=True)
    elif data.startswith("paycancel_"):
        pay_id = int(data.split("_")[1])
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM pending_payments WHERE id=? AND user_id=?", (pay_id, user_id))
        row = cursor.fetchone()
        if row and row[0] == "pending":
            cursor.execute("UPDATE pending_payments SET status='cancelled' WHERE id=?", (pay_id,))
            conn.commit()
            cursor.close()
            conn.close()
            bot.answer_callback_query(call.id, "❌ Payment cancelled.")
            try:
                bot.edit_message_text("❌ <b>Payment cancel kar diya gaya.</b>\n❌ <b>Payment has been cancelled.</b>", chat_id, msg_id, parse_mode="HTML")
            except:
                bot.send_message(chat_id, "❌ <b>Payment cancel kar diya gaya.</b>\n❌ <b>Payment has been cancelled.</b>", parse_mode="HTML")
        else:
            cursor.close()
            conn.close()
            bot.answer_callback_query(call.id, "⚠️ Ye payment ab cancel nahi ho sakti.", show_alert=True)
    elif data.startswith("reject_pay_") and user_id == ADMIN_USER_ID:
        pay_id = int(data.split("_")[2])
        reject_payment(call, pay_id)
    elif data.startswith("sendkey_") and user_id == ADMIN_USER_ID:
        order_id = int(data.split("_")[1])
        prompt_send_key(call, order_id)
    elif data.startswith("rejectorder_") and user_id == ADMIN_USER_ID:
        order_id = int(data.split("_")[1])
        reject_manual_order(call, order_id)
    elif data.startswith("prodpick_") and user_id == ADMIN_USER_ID:
        parts = data.split("_")
        action = parts[1]
        prod_id = int(parts[2])
        if action == "hub":
            show_product_edit_hub(chat_id, msg_id, prod_id)
        elif action == "plans":
            show_product_plans_editor(chat_id, msg_id, prod_id, back_target="admin_cat_product")
        elif action == "toggle":
            conn = get_db()
            cursor = conn.cursor()
            cursor.execute("SELECT name, working_status FROM products WHERE id=?", (prod_id,))
            row = cursor.fetchone()
            new_status = "inactive" if (row[1] or "active") == "active" else "active"
            cursor.execute("UPDATE products SET working_status=? WHERE id=?", (new_status, prod_id))
            conn.commit()
            cursor.close()
            conn.close()
            label = "🟢 Active" if new_status == "active" else " Inactive (hidden from shop)"
            bot.edit_message_text(f"✅ {row[0]} is now: {label}", chat_id, msg_id)
        elif action == "setvideo":
            user_states[user_id] = {"action": "awaiting_prod_video", "prod_id": prod_id}
            bot.edit_message_text("🎬 Now send the demo video for this product:", chat_id, msg_id)
        else:
            field_map = {"setchannel": ("channel_link", "🔗 Send the channel link for this product (e.g. https://t.me/yourchannel):"),
                         "setdesc": ("description", "📝 Send the description text for this product:"),
                         "setdevice": ("device_type", "📱 Send the device type (e.g. Android, iOS, Both):")}
            field, prompt = field_map[action]
            user_states[user_id] = {"action": "awaiting_prod_field", "field": field, "prod_id": prod_id}
            bot.edit_message_text(prompt, chat_id, msg_id)
            bot.register_next_step_handler(call.message, save_product_field)
    elif data.startswith("hubfield_") and user_id == ADMIN_USER_ID:
        parts = data.split("_")
        prod_id = int(parts[-1])
        field = "_".join(parts[1:-1])
        prompts = {
            "name": "✏️ Send the new product name — custom Premium emoji bhi is text ke saath jahan chaho wahin daal sakte ho:",
            "emoji": "🎨 Send a single emoji for this product (e.g. 💎🔥🎮):",
            "compat_status": "🔧 Send the compatibility status (e.g. Root Only / Non-Root Only / Root + Non Root) — custom Premium emoji bhi bhej sakte ho:",
            "description": "📝 Send the new description (custom Premium emoji bhi bhej sakte ho, wo bhi save hoga):",
            "device_type": "📱 Send the device type (e.g. Android, iOS, Both) — custom Premium emoji bhi bhej sakte ho:",
            "duration_label": "🗓️ Send the text to show above the duration buttons (default: 'Select Duration:') — custom Premium emoji bhi bhej sakte ho:",
            "video_caption": "📝 Send the caption to show below the demo video (default: '🎬 Demo video') — custom Premium emoji bhi bhej sakte ho:",
            "channel_link": "🔗 Send the channel link (e.g. https://t.me/yourchannel):",
            "reseller_product_id": "🔌 Send the Bunty Reseller Product ID (bantibhaiya.to ka product_id). Existing Bunty API format bilkul same rahega.\\n\\nKhaali/blank bhejo agar Bunty PID clear karna hai:",
            "keyspanels_variant_id": "🟣 KeysPanelShop Variant IDs <b>per-plan</b> set hote hain. Product-level Variant ID use nahi hota. <b>Manage KeysPanel Variants</b> kholo aur 1 Hour/3 Hours/1 Credit/etc. ke saamne exact Variant ID set karo. Example: 1 Hour → 101, 3 Hours → 102, 1 Credit → 201.",
            "maintenance_emoji": "🔴 Send a single emoji to show next to this product's name in the shop list when maintenance mode is ON (default: 🔴):",
            "maintenance_desc": "📝 Send the description users will see when they open this product while it's under maintenance (custom Premium emoji bhi bhej sakte ho):",
            "premium_emoji_id": "✨ Is product ke Buy/Select button par jo Premium emoji icon dikhana hai uski <b>custom emoji ID</b> bhejo (ya wo emoji khud bhej do/forward kar do, main ID nikal lunga).\n\nKhaali/blank bhejo default emoji par wapas jaane ke liye:",
            "plan_emoji_id": " Is product ke Duration/Plan buttons (1 Hour, 3 Hours, etc.) par jo Premium emoji icon dikhana hai uski <b>custom emoji ID</b> bhejo (ya wo emoji khud bhej do/forward kar do, main ID nikal lunga). Ye Product Emoji se alag hai.\n\nKhaali/blank bhejo default emoji par wapas jaane ke liye:",
        }
        # Show the CURRENT value in a tap-to-copy code block for text/emoji fields, so
        # the admin can copy it, paste it back, and just add/move an emoji instead of
        # retyping everything from scratch. (Telegram gives bots no way to open a
        # pre-filled edit box, so this is the closest practical equivalent.)
        current_note = ""
        if field in ("name", "compat_status", "description", "device_type", "duration_label", "video_caption", "channel_link", "reseller_product_id", "maintenance_emoji", "maintenance_desc", "keyspanels_variant_id"):
            conn = get_db()
            cursor = conn.cursor()
            cursor.execute(f"SELECT {field} FROM products WHERE id=?", (prod_id,))
            row = cursor.fetchone()
            cursor.close()
            conn.close()
            if row and row[0]:
                raw_val = row[0]
                # Telegram messages have a hard 4096-char limit. A long description
                # (multi-line fancy text with lots of emoji) plus the prompt text could
                # cross that limit and make edit_message_text silently fail -- so the
                # preview is capped, guaranteeing the prompt always sends successfully.
                preview_val = raw_val if len(raw_val) <= 500 else raw_val[:500] + "… (trimmed)"
                current_note = f"\n\nAbhi ka value (copy karne ke liye tap karo):\n<code>{html.escape(preview_val)}</code>"
                if len(raw_val) > 500:
                    current_note += "\n\n⚠️ Value lamba hai, sirf shuruaati hissa dikhaya gaya hai."
        user_states[user_id] = {"action": "awaiting_prod_field_hub", "field": field, "prod_id": prod_id}
        cancel_markup = InlineKeyboardMarkup()
        cancel_markup.add(InlineKeyboardButton("❌ Cancel", callback_data=f"prodpick_hub_{prod_id}", style="danger", icon_custom_emoji_id=btn_emo("prodpick_hub")))
        bot.edit_message_text(prompts.get(field, "Send new value:") + current_note, chat_id, msg_id, parse_mode="HTML", reply_markup=cancel_markup)
        bot.register_next_step_handler(call.message, save_product_field_hub)
    elif data.startswith("hubtoggle_") and user_id == ADMIN_USER_ID:
        prod_id = int(data.split("_")[1])
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT working_status FROM products WHERE id=?", (prod_id,))
        row = cursor.fetchone()
        new_status = "inactive" if (row[0] or "active") == "active" else "active"
        cursor.execute("UPDATE products SET working_status=? WHERE id=?", (new_status, prod_id))
        conn.commit()
        cursor.close()
        conn.close()
        show_product_edit_hub(chat_id, msg_id, prod_id)
    elif data.startswith("hubmainttoggle_") and user_id == ADMIN_USER_ID:
        prod_id = int(data.split("_")[1])
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT maintenance_enabled FROM products WHERE id=?", (prod_id,))
        row = cursor.fetchone()
        new_val = 0 if (row and row[0]) else 1
        cursor.execute("UPDATE products SET maintenance_enabled=? WHERE id=?", (new_val, prod_id))
        conn.commit()
        cursor.close()
        conn.close()
        show_product_edit_hub(chat_id, msg_id, prod_id)
        prompt_notify_broadcast_if_needed(chat_id, prod_id, new_val)
    elif data.startswith("mainttoggle_") and user_id == ADMIN_USER_ID:
        prod_id = int(data.split("_")[1])
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT maintenance_enabled FROM products WHERE id=?", (prod_id,))
        row = cursor.fetchone()
        new_val = 0 if (row and row[0]) else 1
        cursor.execute("UPDATE products SET maintenance_enabled=? WHERE id=?", (new_val, prod_id))
        conn.commit()
        cursor.close()
        conn.close()
        show_maintenance_toggle_list(chat_id, msg_id)
        prompt_notify_broadcast_if_needed(chat_id, prod_id, new_val)
    elif data.startswith("hubaidtoggle_") and user_id == ADMIN_USER_ID:
        prod_id = int(data.split("_")[1])
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT requires_android_id FROM products WHERE id=?", (prod_id,))
        row = cursor.fetchone()
        new_val = 0 if (row and row[0]) else 1
        cursor.execute("UPDATE products SET requires_android_id=? WHERE id=?", (new_val, prod_id))
        conn.commit()
        cursor.close()
        conn.close()
        show_product_edit_hub(chat_id, msg_id, prod_id)
    elif data.startswith("hubapiprovider_") and user_id == ADMIN_USER_ID:
        prod_id = int(data.split("_")[1])
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT external_api_provider FROM products WHERE id=?", (prod_id,))
        row = cursor.fetchone()
        current = get_external_api_provider(row[0] if row else "bunty")
        new_provider = "keyspanels" if current == "bunty" else "bunty"
        cursor.execute("UPDATE products SET external_api_provider=? WHERE id=?", (new_provider, prod_id))
        conn.commit()
        cursor.close()
        conn.close()
        show_product_edit_hub(chat_id, msg_id, prod_id)
    elif data.startswith("kpvariants_") and user_id == ADMIN_USER_ID:
        prod_id = int(data.split("_")[1])
        show_keyspanels_variant_manager(chat_id, msg_id, prod_id)
    elif data.startswith("kpvariant_") and user_id == ADMIN_USER_ID:
        parts = data.split("_"); plan_id = int(parts[1]); prod_id = int(parts[2])
        conn = get_db(); cur = conn.cursor()
        cur.execute("SELECT days, duration_unit, keyspanels_variant_id FROM plans WHERE id=? AND product_id=?", (plan_id, prod_id))
        row = cur.fetchone(); cur.close(); conn.close()
        if not row:
            bot.answer_callback_query(call.id, "❌ Plan not found.", show_alert=True)
        else:
            current = str(row[2]).strip() if row[2] else "Not set"
            user_states[user_id] = {"action":"awaiting_keyspanels_plan_variant", "kp_plan_id":plan_id, "prod_id":prod_id}
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("❌ Cancel", callback_data=f"kpvariants_{prod_id}", style="danger", icon_custom_emoji_id=btn_emo("cancel_product_detail")))
            bot.edit_message_text(f"🟣 <b>KeysPanel Variant ID</b>\n\nPlan: <b>{html.escape(format_duration(row[0], row[1]))}</b>\nCurrent: <code>{html.escape(current)}</code>\n\nNumeric Variant ID bhejo (example: <code>101</code>).\nClear ke liye <code>clear</code> bhejo.", chat_id, msg_id, parse_mode="HTML", reply_markup=markup)
            bot.register_next_step_handler(call.message, save_keyspanels_plan_variant)
    elif data.startswith("hubextapitoggle_") and user_id == ADMIN_USER_ID:
        prod_id = int(data.split("_")[1])
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT external_api_enabled FROM products WHERE id=?", (prod_id,))
        row = cursor.fetchone()
        new_val = 0 if (row and row[0]) else 1
        cursor.execute("UPDATE products SET external_api_enabled=? WHERE id=?", (new_val, prod_id))
        conn.commit()
        cursor.close()
        conn.close()
        show_product_edit_hub(chat_id, msg_id, prod_id)
    elif data.startswith("hubreorder_") and user_id == ADMIN_USER_ID:
        prod_id = int(data.split("_")[1])
        show_reorder_screen(chat_id, msg_id, prod_id)
    elif data.startswith("reorderup_") and user_id == ADMIN_USER_ID:
        prod_id = int(data.split("_")[1])
        move_product(prod_id, "up")
        show_reorder_screen(chat_id, msg_id, prod_id)
    elif data.startswith("reorderdown_") and user_id == ADMIN_USER_ID:
        prod_id = int(data.split("_")[1])
        move_product(prod_id, "down")
        show_reorder_screen(chat_id, msg_id, prod_id)
    elif data.startswith("hubvideo_") and user_id == ADMIN_USER_ID:
        prod_id = int(data.split("_")[1])
        user_states[user_id] = {"action": "awaiting_prod_video", "prod_id": prod_id}
        bot.edit_message_text("🎬 Now send the demo video for this product:", chat_id, msg_id)
    elif data.startswith("hubplans_") and user_id == ADMIN_USER_ID:
        prod_id = int(data.split("_")[1])
        show_product_plans_editor(chat_id, msg_id, prod_id, back_target=f"prodpick_hub_{prod_id}")
    elif data.startswith("editplanprice_") and user_id == ADMIN_USER_ID:
        parts = data.split("_")
        plan_id = int(parts[1])
        prod_id = int(parts[2])
        user_states[user_id] = {"action": "awaiting_plan_price", "plan_id": plan_id, "prod_id": prod_id}
        bot.edit_message_text("💰 Send the new price (numbers only, e.g. 299):", chat_id, msg_id)
        bot.register_next_step_handler(call.message, save_plan_price)
    elif data.startswith("emojicat_") and user_id == ADMIN_USER_ID:
        category = data[len("emojicat_"):]
        show_emoji_slots(chat_id, msg_id, category)
        try: bot.answer_callback_query(call.id)
        except: pass
        return
    elif data.startswith("emojislot_") and user_id == ADMIN_USER_ID:
        slot_key = data[len("emojislot_"):]
        info = EMOJI_SLOTS.get(slot_key)
        if not info:
            bot.answer_callback_query(call.id, "❌ Unknown slot.", show_alert=True)
            return
        default, label, category = info
        current = get_setting(f"emoji_slot_{slot_key}")
        status_line = f"Current: custom emoji set (ID: <code>{html.escape(current)}</code>)" if current else f"Current: default {default}"
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("♻️ Reset to Default", callback_data=f"emojireset_{slot_key}", style="success", icon_custom_emoji_id=btn_emo("emojireset")))
        markup.add(InlineKeyboardButton(" Back", callback_data=f"emojicat_{category}", style="primary", icon_custom_emoji_id=btn_emo("emojicat")))
        bot.edit_message_text(
            f"🎨 <b>{html.escape(label)}</b>\n📂 Category: {html.escape(category)}\n\n{status_line}\n\n"
            f"👇 Ab ek custom emoji is text ke saath bhejo (ya kisi message ko forward karo jisme wo custom emoji ho), "
            f"ya uski numeric custom_emoji_id seedhe type karke bhejo.\n\n"
            f"Reset karne ke liye upar wala button dabao.",
            chat_id, msg_id, parse_mode="HTML", reply_markup=markup
        )
        bot.register_next_step_handler(call.message, save_emoji_slot, slot_key)
        return
    elif data.startswith("emojireset_") and user_id == ADMIN_USER_ID:
        slot_key = data[len("emojireset_"):]
        if slot_key == "link_leaderboard":
            slot_key = "btn_link_leaderboard"
        info = EMOJI_SLOTS.get(slot_key)
        if info:
            set_setting(f"emoji_slot_{slot_key}", "")
            bot.answer_callback_query(call.id, "♻️ Reset to default.")
            show_emoji_slots(chat_id, msg_id, info[2])
        else:
            bot.answer_callback_query(call.id, "❌ Unknown slot.", show_alert=True)
    elif data.startswith("resellerprice_prod_") and user_id == ADMIN_USER_ID:
        prod_id = int(data.split("_")[2])
        show_reseller_price_plans(chat_id, msg_id, prod_id)
    elif data.startswith("resellerprice_plan_") and user_id == ADMIN_USER_ID:
        parts = data.split("_")
        plan_id = int(parts[2])
        prod_id = int(parts[3])
        user_states[user_id] = {"action": "awaiting_reseller_price", "plan_id": plan_id, "prod_id": prod_id}
        cancel_markup = InlineKeyboardMarkup()
        cancel_markup.add(InlineKeyboardButton("❌ Cancel", callback_data=f"resellerprice_prod_{prod_id}", style="danger", icon_custom_emoji_id=btn_emo("resellerprice_prod")))
        bot.edit_message_text(
            "💰 Is plan ke liye RESELLER price bhejo (numbers only, jaise 299).\n\n"
            "Reseller price hatani ho (wapas normal price par laana ho) to <code>clear</code> bhejo.",
            chat_id, msg_id, parse_mode="HTML", reply_markup=cancel_markup
        )
        bot.register_next_step_handler(call.message, save_reseller_price)
    else:
        # Unknown/old callback data should never show the misleading
        # Unknown/old callback data is kept harmless and silent.
        try:
            bot.answer_callback_query(call.id)
        except Exception:
            pass

def show_product_plans_editor(chat_id, msg_id, prod_id, back_target):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id, days, price, duration_unit FROM plans WHERE product_id=? ORDER BY (CASE WHEN duration_unit='hours' THEN days ELSE days*24 END)", (prod_id,))
    plans = cursor.fetchall()
    cursor.close()
    conn.close()
    markup = InlineKeyboardMarkup(row_width=1)
    for pl in plans:
        markup.add(InlineKeyboardButton(f"✏️ {format_duration(pl[1], pl[3])} - ₹{pl[2]}", callback_data=f"editplanprice_{pl[0]}_{prod_id}", style="success", icon_custom_emoji_id=btn_emo("editplanprice")))
    markup.add(InlineKeyboardButton(" Back", callback_data=back_target, style="primary", icon_custom_emoji_id=btn_emo("link_back")))
    bot.edit_message_text("💰 Select a plan to edit its price:" if plans else "⚠️ No plans yet for this product.", chat_id, msg_id, reply_markup=markup)

def move_product(prod_id, direction):
    """Swap this product's sort_order with its immediate neighbor (up or down)
    in the product list, so the admin can freely reorder products."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id, sort_order FROM products ORDER BY sort_order, id")
    all_products = cursor.fetchall()
    ids = [p[0] for p in all_products]
    if prod_id not in ids:
        cursor.close()
        conn.close()
        return
    idx = ids.index(prod_id)
    swap_idx = idx - 1 if direction == "up" else idx + 1
    if 0 <= swap_idx < len(all_products):
        this_order = all_products[idx][1]
        other_id, other_order = all_products[swap_idx]
        cursor.execute("UPDATE products SET sort_order=? WHERE id=?", (other_order, prod_id))
        cursor.execute("UPDATE products SET sort_order=? WHERE id=?", (this_order, other_id))
        conn.commit()
    cursor.close()
    conn.close()

def show_reorder_screen(chat_id, msg_id, prod_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, name_entities FROM products ORDER BY sort_order, id")
    all_products = cursor.fetchall()
    cursor.close()
    conn.close()
    ids = [p[0] for p in all_products]
    this_product = next((p for p in all_products if p[0] == prod_id), None)
    if not this_product:
        bot.edit_message_text("❌ Product not found.", chat_id, msg_id)
        return
    idx = ids.index(prod_id)
    position_text = f"Position: {idx + 1} / {len(all_products)}"
    text = (
        f"⬆️⬇️ <b>Reorder Product</b>\n\n"
        f"{strip_custom_emoji(this_product[1], this_product[2])}\n"
        f"{position_text}\n\n"
        f"Upar/niche move karne ke liye button dabao. Customer ko Shop list mein ye product "
        f"isi order mein dikhega."
    )
    markup = InlineKeyboardMarkup(row_width=2)
    row = []
    if idx > 0:
        row.append(InlineKeyboardButton("⬆️ Move Up", callback_data=f"reorderup_{prod_id}", style="success", icon_custom_emoji_id=btn_emo("reorderup")))
    if idx < len(all_products) - 1:
        row.append(InlineKeyboardButton("⬇️ Move Down", callback_data=f"reorderdown_{prod_id}", style="success", icon_custom_emoji_id=btn_emo("reorderdown")))
    if row:
        markup.add(*row)
    markup.add(InlineKeyboardButton(" Back", callback_data=f"prodpick_hub_{prod_id}", style="primary", icon_custom_emoji_id=btn_emo("prodpick_hub")))
    bot.edit_message_text(text, chat_id, msg_id, parse_mode="HTML", reply_markup=markup)

def get_keyspanels_plan_mappings(prod_id):
    conn = get_db(); cur = conn.cursor()
    cur.execute("SELECT id, days, duration_unit, price, keyspanels_variant_id FROM plans WHERE product_id=? ORDER BY (CASE WHEN duration_unit='hours' THEN days ELSE days*24 END), id", (prod_id,))
    rows = cur.fetchall(); cur.close(); conn.close()
    return rows

def show_keyspanels_variant_manager(chat_id, msg_id, prod_id):
    plans = get_keyspanels_plan_mappings(prod_id)
    markup = InlineKeyboardMarkup(row_width=1)
    lines = ["🟣 <b>KeysPanelShop Variant Mapping</b>", "", "Har duration/plan ka apna Variant ID set karo:"]
    if not plans:
        lines.append("⚠️ Is product mein koi plan nahi hai.")
    else:
        for pl in plans:
            variant = str(pl[4]).strip() if pl[4] else "❌ Not set"
            lines.append(f"• <b>{html.escape(format_duration(pl[1], pl[2]))}</b> → <code>{html.escape(variant)}</code>")
            markup.add(InlineKeyboardButton(f"🆔 {format_duration(pl[1], pl[2])} → {variant}", callback_data=f"kpvariant_{pl[0]}_{prod_id}", style="success", icon_custom_emoji_id=btn_emo("hubfield_reseller_product_id")))
    lines += ["", "Example: 1 Hour = 101, 3 Hours = 102, 6 Hours = 103.", "Bunty PID isse bilkul alag rahega."]
    markup.add(InlineKeyboardButton("⬅️ Back to Product", callback_data=f"prodpick_hub_{prod_id}", style="primary", icon_custom_emoji_id=btn_emo("prodpick_hub")))
    bot.edit_message_text("\n".join(lines), chat_id, msg_id, parse_mode="HTML", reply_markup=markup)

def save_keyspanels_plan_variant(message):
    if _check_cancel(message):
        return
    uid = message.from_user.id; state = user_states.get(uid, {})
    plan_id = state.get("kp_plan_id"); prod_id = state.get("prod_id")
    value = (message.text or "").strip()
    if not plan_id or not prod_id:
        return
    if value.lower() in ("clear", "remove", "none", "-1"):
        value = None
    elif not value.isdigit():
        bot.reply_to(message, "❌ Variant ID numeric hona chahiye. Example: 101")
        bot.register_next_step_handler(message, save_keyspanels_plan_variant)
        return

    def _write(conn):
        cur = conn.cursor()
        cur.execute("UPDATE plans SET keyspanels_variant_id=? WHERE id=? AND product_id=?",
                    (value, int(plan_id), int(prod_id)))
        changed = cur.rowcount
        cur.close()
        return changed

    try:
        changed = sqlite_write_with_retry(_write)
    except Exception as e:
        bot.reply_to(message, f"❌ Variant ID save nahi hua: {e}")
        return
    if not changed:
        user_states.pop(uid, None)
        bot.reply_to(message, "❌ Ye plan/product match nahi hua. Manage KeysPanel Variants se sahi plan dobara select karo.")
        return
    user_states.pop(uid, None)
    shown = value if value else "cleared"
    bot.reply_to(message, f"✅ KeysPanel Variant ID saved: {shown}")
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🟣 Manage KeysPanel Variants", callback_data=f"kpvariants_{prod_id}", style="success", icon_custom_emoji_id=btn_emo("hubextapitoggle")))
    markup.add(InlineKeyboardButton("⬅️ Product", callback_data=f"prodpick_hub_{prod_id}", style="primary", icon_custom_emoji_id=btn_emo("prodpick_hub")))
    bot.send_message(message.chat.id, "Done! Ab selected plan isi Variant ID se order karega.", reply_markup=markup)

def show_product_edit_hub(chat_id, msg_id, prod_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""SELECT name, emoji, description, device_type, channel_link, working_status,
                              compat_status, compat_status_entities, description_entities, name_entities,
                              device_type_entities, duration_label, duration_label_entities,
                              video_caption, video_caption_entities, requires_android_id, reseller_product_id,
                              external_api_enabled, external_api_provider, keyspanels_variant_id, maintenance_enabled, maintenance_emoji,
                              maintenance_desc, maintenance_desc_entities, premium_emoji_id, plan_emoji_id
                       FROM products WHERE id=?""", (prod_id,))
    p = cursor.fetchone()
    cursor.close()
    conn.close()
    if not p:
        bot.edit_message_text("❌ Product not found.", chat_id, msg_id)
        return
    status_label = "🟢 Active" if (p[5] or "active") == "active" else "🔴 Inactive"
    compat_html = render_text_with_custom_emoji(p[6], p[7]) if p[6] else "—"
    desc_html = render_text_with_custom_emoji(p[2], p[8]) if p[2] else "—"
    name_html = render_text_with_custom_emoji(p[0], p[9])
    device_html = render_text_with_custom_emoji(p[3], p[10]) if p[3] else "—"
    duration_label_html = render_text_with_custom_emoji(p[11], p[12]) if p[11] else "Select Duration: (default)"
    video_caption_html = render_text_with_custom_emoji(p[13], p[14]) if p[13] else "🎬 Demo video (default)"
    android_id_label = "🟢 ON (Android ID maangega, key manual style se milegi)" if p[15] else "🔴 OFF (instant delivery, jaise pehle)"
    provider = get_external_api_provider(p[18])
    if p[17]:
        if provider == "keyspanels":
            kp_plans = get_keyspanels_plan_mappings(prod_id)
            set_count = sum(1 for pl in kp_plans if pl[4] and str(pl[4]).strip())
            external_api_label = f"🟣 ON (KeysPanelShop: {set_count}/{len(kp_plans)} plan variants set)" if kp_plans else "🟣 ON (⚠️ No plans)"
        else:
            external_api_label = f"🟢 ON (Bunty PID: {html.escape(p[16])})" if p[16] else "🟢 ON (⚠️ Bunty PID abhi set nahi hai)"
    else:
        external_api_label = "🔴 OFF"
    maintenance_label = "🟢 ON (shop mein dot dikhega, kholne par description aayega)" if p[20] else "⚪ OFF"
    maintenance_emoji_val = p[21] or "🔴"
    maintenance_desc_html = render_text_with_custom_emoji(p[22], p[23]) if p[22] else "—"
    premium_emoji_label = f"<code>{html.escape(p[24])}</code> ✅" if p[24] else "Not set (default emoji use ho rahi hai)"
    plan_emoji_label = f"<code>{html.escape(p[25])}</code> ✅" if p[25] else "Not set (default emoji use ho rahi hai)"
    text = (f"✏️ <b>Edit Product</b>\n\n"
            f"Title: <b>{name_html}</b>\n"
            f"Compatibility: {compat_html}\n"
            f"Description: {desc_html}\n"
            f"Device: {device_html}\n"
            f"Duration Label: {duration_label_html}\n"
            f"Channel Link: {html.escape(p[4]) if p[4] else '—'}\n"
            f"Video Caption: {video_caption_html}\n"
            f"Status: {status_label}\n"
            f"Android ID Flow: {android_id_label}\n"
            f"External API (auto key-gen): {external_api_label}\n"
            f"🛠️ Maintenance Mode: {maintenance_label}\n"
            f"🛠️ Maintenance Emoji: {maintenance_emoji_val}\n"
            f"🛠️ Maintenance Description: {maintenance_desc_html}\n"
            f"✨ Product Button Emoji: {premium_emoji_label}\n"
            f" Plan/Duration Button Emoji: {plan_emoji_label}\n\n"
            f"Neeche jo bhi field change karni hai uska button dabao. Text bhejte waqt ek custom "
            f"Premium emoji bhi saath mein daal sakte ho, wo bhi save ho jaayega.")
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("Name", callback_data=f"hubfield_name_{prod_id}", style="success", icon_custom_emoji_id=btn_emo("hubfield_name")),
        InlineKeyboardButton("🔧 Compatibility (Root/Non-Root)", callback_data=f"hubfield_compat_status_{prod_id}", style="success", icon_custom_emoji_id=btn_emo("hubfield_compat_status")),
        InlineKeyboardButton("Description", callback_data=f"hubfield_description_{prod_id}", style="success", icon_custom_emoji_id=btn_emo("hubfield_description")),
        InlineKeyboardButton("Device Type", callback_data=f"hubfield_device_type_{prod_id}", style="success", icon_custom_emoji_id=btn_emo("hubfield_device_type")),
        InlineKeyboardButton("🗓️ Duration Label", callback_data=f"hubfield_duration_label_{prod_id}", style="success", icon_custom_emoji_id=btn_emo("hubfield_duration_label")),
        InlineKeyboardButton("🔗 Channel Link", callback_data=f"hubfield_channel_link_{prod_id}", style="success", icon_custom_emoji_id=btn_emo("hubfield_channel_link")),
        InlineKeyboardButton("🎬 Video", callback_data=f"hubvideo_{prod_id}", style="success", icon_custom_emoji_id=btn_emo("hubvideo")),
        InlineKeyboardButton("📝 Video Caption", callback_data=f"hubfield_video_caption_{prod_id}", style="success", icon_custom_emoji_id=btn_emo("hubfield_video_caption")),
        InlineKeyboardButton("💰 Edit Plan Prices", callback_data=f"hubplans_{prod_id}", style="success", icon_custom_emoji_id=btn_emo("hubplans")),
        InlineKeyboardButton("🔁 Toggle Active/Inactive", callback_data=f"hubtoggle_{prod_id}", style="success", icon_custom_emoji_id=btn_emo("hubtoggle")),
        InlineKeyboardButton("📱 Toggle Android ID Flow", callback_data=f"hubaidtoggle_{prod_id}", style="success", icon_custom_emoji_id=btn_emo("hubaidtoggle")),
        InlineKeyboardButton("🔌 Toggle External API", callback_data=f"hubextapitoggle_{prod_id}", style="success", icon_custom_emoji_id=btn_emo("hubextapitoggle")),
        InlineKeyboardButton("🔀 Select API: Bunty / KeysPanel", callback_data=f"hubapiprovider_{prod_id}", style="success", icon_custom_emoji_id=btn_emo("hubextapitoggle")),
        InlineKeyboardButton("🆔 Set Bunty PID", callback_data=f"hubfield_reseller_product_id_{prod_id}", style="success", icon_custom_emoji_id=btn_emo("hubfield_reseller_product_id")),
        InlineKeyboardButton("🟣 Manage KeysPanel Variants", callback_data=f"kpvariants_{prod_id}", style="success", icon_custom_emoji_id=btn_emo("hubfield_reseller_product_id")),
        InlineKeyboardButton("⬆️⬇️ Reorder", callback_data=f"hubreorder_{prod_id}", style="success", icon_custom_emoji_id=btn_emo("hubreorder")),
        InlineKeyboardButton("🛠️ Toggle Maintenance ON/OFF", callback_data=f"hubmainttoggle_{prod_id}", style="success", icon_custom_emoji_id=btn_emo("hubmainttoggle")),
        InlineKeyboardButton("🔴 Set Maintenance Emoji", callback_data=f"hubfield_maintenance_emoji_{prod_id}", style="success", icon_custom_emoji_id=btn_emo("hubfield_maintenance_emoji")),
        InlineKeyboardButton("📝 Set Maintenance Description", callback_data=f"hubfield_maintenance_desc_{prod_id}", style="success", icon_custom_emoji_id=btn_emo("hubfield_maintenance_desc")),
        InlineKeyboardButton("✨ Set Product Emoji", callback_data=f"hubfield_premium_emoji_id_{prod_id}", style="success", icon_custom_emoji_id=get_product_emoji(p[24])),
        InlineKeyboardButton(" Set Plan Button Emoji", callback_data=f"hubfield_plan_emoji_id_{prod_id}", style="success", icon_custom_emoji_id=get_product_emoji(p[25])),
    )
    markup.add(InlineKeyboardButton(" Back", callback_data="admin_cat_product", style="primary", icon_custom_emoji_id=btn_emo("admin_cat_product")))
    bot.edit_message_text(text, chat_id, msg_id, parse_mode="HTML", reply_markup=markup)

def save_product_field_hub(message):
    if _check_cancel(message):
        return
    user_id = message.from_user.id
    state = user_states.get(user_id, {})
    field = state.get("field")
    prod_id = state.get("prod_id")
    if not field or not prod_id:
        return
    value = (message.text or "").strip()
    if field in ("emoji", "maintenance_emoji") and len(value) > 4:
        bot.reply_to(message, "❌ Please send just one emoji.")
        return
    if field in ("premium_emoji_id", "plan_emoji_id"):
        custom_id = None
        entities = message.entities or message.caption_entities
        if entities:
            for ent in entities:
                if getattr(ent, "type", None) == "custom_emoji" and getattr(ent, "custom_emoji_id", None):
                    custom_id = ent.custom_emoji_id
                    break
        if not custom_id and value.isdigit():
            custom_id = value
        if not custom_id and value != "":
            bot.reply_to(message, "❌ Ek valid custom emoji ID nahi mili. Emoji khud bhejo/forward karo, ya uski numeric ID paste karo. Ya khaali message bhejo reset karne ke liye.")
            bot.register_next_step_handler(message, save_product_field_hub)
            return
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(f"UPDATE products SET {field}=? WHERE id=?", (custom_id or None, prod_id))
        conn.commit()
        cursor.close()
        conn.close()
        user_states.pop(user_id, None)
        label = "Product emoji" if field == "premium_emoji_id" else "Plan button emoji"
        bot.reply_to(message, f"✅ {label} updated!" if custom_id else f"✅ {label} reset to default!")
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("✏️ Continue Editing This Product", callback_data=f"prodpick_hub_{prod_id}", style="success", icon_custom_emoji_id=btn_emo("prodpick_hub")))
        bot.send_message(message.chat.id, "Tap below to continue editing:", reply_markup=markup)
        return
    conn = get_db()
    cursor = conn.cursor()
    if field in ("description", "compat_status", "name", "device_type", "duration_label", "video_caption", "maintenance_desc"):
        entities_json = extract_custom_emoji_json(message)
        cursor.execute(f"UPDATE products SET {field}=?, {field}_entities=? WHERE id=?", (value, entities_json, prod_id))
    else:
        cursor.execute(f"UPDATE products SET {field}=? WHERE id=?", (value, prod_id))
    conn.commit()
    cursor.close()
    conn.close()
    user_states.pop(user_id, None)
    bot.reply_to(message, f"✅ Updated!")
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("✏️ Continue Editing This Product", callback_data=f"prodpick_hub_{prod_id}", style="success", icon_custom_emoji_id=btn_emo("prodpick_hub")))
    bot.send_message(message.chat.id, "Tap below to continue editing:", reply_markup=markup)

def save_plan_price(message):
    if _check_cancel(message):
        return
    user_id = message.from_user.id
    state = user_states.get(user_id, {})
    plan_id = state.get("plan_id")
    prod_id = state.get("prod_id")
    if not plan_id:
        return
    try:
        new_price = int(message.text.strip())
    except:
        bot.reply_to(message, "❌ Invalid price. Numbers only.")
        return
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("UPDATE plans SET price=? WHERE id=?", (new_price, plan_id))
    conn.commit()
    cursor.close()
    conn.close()
    user_states.pop(user_id, None)
    bot.reply_to(message, f"✅ Price updated to ₹{new_price}!")
    if prod_id:
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton(" Back to Product", callback_data=f"prodpick_hub_{prod_id}", style="primary", icon_custom_emoji_id=btn_emo("prodpick_hub")))
        bot.send_message(message.chat.id, "Done!", reply_markup=markup)

def save_product_field(message):
    if _check_cancel(message):
        return
    user_id = message.from_user.id
    state = user_states.get(user_id, {})
    field = state.get("field")
    prod_id = state.get("prod_id")
    if not field or not prod_id:
        return
    value = message.text.strip()
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(f"UPDATE products SET {field}=? WHERE id=?", (value, prod_id))
    conn.commit()
    cursor.close()
    conn.close()
    user_states.pop(user_id, None)
    bot.reply_to(message, f"✅ Updated! {field.replace('_',' ').title()} saved for the product.")

# ======================= SHOP FUNCTIONS =======================
def send_insufficient_balance_message(chat_id, price, balance, back_callback, msg_id=None):
    """Shows a proper in-chat 'Balance kam hai!' message (with Add Balance +
    Back buttons) instead of a popup alert -- matches the flow the person
    wants: tapping Add Balance goes straight into the existing single-gateway
    (ZapUPI) top-up flow, nothing extra added."""
    text = (
        f"{emo('insuff_title')} <b>Balance kam hai!</b>\n\n"
        f"{emo('insuff_need')} Chahiye: ₹{price}{usd_hint(price)}\n"
        f"{emo('insuff_have')} Aapke paas: ₹{balance}{usd_hint(balance)}"
    )
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton(" Add Balance", callback_data="add_balance", style="success", icon_custom_emoji_id=btn_emo("add_balance")),
        InlineKeyboardButton(" Back", callback_data=back_callback, style="primary", icon_custom_emoji_id=btn_emo("link_back")),
    )
    if msg_id:
        try:
            bot.edit_message_text(text, chat_id, msg_id, parse_mode="HTML", reply_markup=markup)
            return
        except:
            pass
    bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)

def show_become_reseller_screen(call, user_id):
    """Shows the self-serve 'Become a Reseller' screen: admin-written terms,
    the price, the user's current balance, and a Buy button. Admin controls
    everything (on/off, price, terms text) from the Reseller Management panel."""
    chat_id = call.message.chat.id
    msg_id = call.message.message_id
    if get_setting("reseller_buy_enabled") != "1":
        bot.answer_callback_query(call.id, "⚠️ Reseller program abhi available nahi hai.", show_alert=True)
        return
    price_raw = get_setting("reseller_buy_price")
    try:
        price = int(price_raw)
    except (TypeError, ValueError):
        bot.answer_callback_query(call.id, "⚠️ Reseller price set nahi hai. Admin se contact karo.", show_alert=True)
        return
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT balance, is_reseller, reseller_banned FROM users WHERE id=?", (user_id,))
    user = cursor.fetchone()
    cursor.close()
    conn.close()
    if user and user[1] and not user[2]:
        bot.answer_callback_query(call.id, "✅ Aap already Reseller hain!", show_alert=True)
        return
    balance = user[0] if user else 0
    terms = get_setting("reseller_buy_terms") or "Reseller banne ke baad aapko har product par special reseller price milega."
    text = (
        f"{emo('reseller_become_title')} <b>Become a Reseller</b>\n\n"
        f"{terms}\n\n"
        f"{emo('reseller_price')} <b>Price:</b> ₹{price}\n"
        f"{emo('reseller_balance')} <b>Aapka Balance:</b> ₹{balance}"
    )
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(
        InlineKeyboardButton(f" Buy Reseller - ₹{price}", callback_data="reseller_buy_confirm", style="success", icon_custom_emoji_id=btn_emo("reseller_buy_confirm")),
        InlineKeyboardButton(" Back", callback_data="back_main", style="primary", icon_custom_emoji_id=btn_emo("back_main")),
    )
    bot.edit_message_text(text, chat_id, msg_id, parse_mode="HTML", reply_markup=markup)

def process_reseller_buy(call, user_id):
    """Deducts the admin-set reseller price from the user's balance and
    activates reseller status. If balance is short, shows the same
    Add-Balance prompt used everywhere else in the shop."""
    chat_id = call.message.chat.id
    msg_id = call.message.message_id
    if get_setting("reseller_buy_enabled") != "1":
        bot.answer_callback_query(call.id, "⚠️ Reseller program abhi available nahi hai.", show_alert=True)
        return
    price_raw = get_setting("reseller_buy_price")
    try:
        price = int(price_raw)
    except (TypeError, ValueError):
        bot.answer_callback_query(call.id, "⚠️ Reseller price set nahi hai. Admin se contact karo.", show_alert=True)
        return
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT balance, is_reseller, reseller_banned FROM users WHERE id=?", (user_id,))
    user = cursor.fetchone()
    if user and user[1] and not user[2]:
        cursor.close()
        conn.close()
        bot.answer_callback_query(call.id, "✅ Aap already Reseller hain!", show_alert=True)
        return
    if not user or user[0] < price:
        cursor.close()
        conn.close()
        send_insufficient_balance_message(chat_id, price, user[0] if user else 0, "become_reseller", msg_id=msg_id)
        return
    new_bal = user[0] - price
    cursor.execute("UPDATE users SET balance=?, is_reseller=1, reseller_banned=0 WHERE id=?", (new_bal, user_id))
    cursor.execute("INSERT INTO transactions (user_id, amount, type, details) VALUES (?,?,'purchase',?)",
                   (user_id, price, "Reseller access purchased"))
    conn.commit()
    cursor.close()
    conn.close()
    success_msg = get_setting("reseller_buy_success_msg") or "Ab aapko har product par special reseller price milega."
    text = (
        f"{emo('reseller_activated_title')} <b>Reseller Activated!</b>\n\n"
        f"{success_msg}\n\n"
        f"{emo('reseller_new_balance')} <b>Naya Balance:</b> ₹{new_bal}"
    )
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton(" Back to Menu", callback_data="back_main", style="primary", icon_custom_emoji_id=btn_emo("back_main")))
    bot.edit_message_text(text, chat_id, msg_id, parse_mode="HTML", reply_markup=markup)
    try:
        bot.send_message(
            ADMIN_USER_ID,
            f"🏷️ <b>New Reseller!</b>\n🆔 <code>{user_id}</code>\n👤 @{call.from_user.username or 'N/A'}\n💰 Paid: ₹{price}",
            parse_mode="HTML"
        )
    except:
        pass

def show_products(call):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, emoji, name_entities, maintenance_enabled, maintenance_emoji, premium_emoji_id FROM products WHERE COALESCE(working_status,'active')='active' ORDER BY sort_order, id")
    products = cursor.fetchall()
    cursor.close()
    conn.close()
    if not products:
        bot.edit_message_text("❌ No products.", call.message.chat.id, call.message.message_id)
        return
    markup = InlineKeyboardMarkup(row_width=1)
    for p in products:
        label = strip_custom_emoji(p[1], p[3])
        is_maint = bool(p[4])
        if is_maint:
            # Under maintenance: red button + maintenance emoji at the END of the
            # name, but the product's own icon (button emoji) stays exactly as
            # set for that product -- it should NOT disappear just because
            # maintenance is ON.
            label = f"{label} {p[5] or '🔴'}"
            btn_style = "danger"
            btn_icon = get_product_emoji(p[6])
        else:
            btn_style = "success"
            btn_icon = get_product_emoji(p[6])
        markup.add(InlineKeyboardButton(label, callback_data=f"product_{p[0]}", style=btn_style, icon_custom_emoji_id=btn_icon))
    markup.add(InlineKeyboardButton(" Back", callback_data="back_main", style="primary", icon_custom_emoji_id=btn_emo("back_main")))
    bot.edit_message_text("🛒 <b>Apna Product Chunlo! 👇</b>", call.message.chat.id, call.message.message_id, parse_mode="HTML", reply_markup=markup)

def show_product_plans(call, product_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id, days, price, duration_unit, reseller_price FROM plans WHERE product_id=? ORDER BY (CASE WHEN duration_unit='hours' THEN days ELSE days*24 END)", (product_id,))
    plans = cursor.fetchall()
    cursor.execute("""SELECT name, video_file_id, emoji, compat_status, compat_status_entities, name_entities,
                              duration_label, duration_label_entities, description, description_entities,
                              maintenance_enabled, maintenance_emoji, maintenance_desc, maintenance_desc_entities,
                              premium_emoji_id, plan_emoji_id
                       FROM products WHERE id=?""", (product_id,))
    product = cursor.fetchone()
    cursor.close()
    conn.close()
    # Fixes the "Something went wrong. Try again." popup: this used to only check
    # `plans`, so tapping a product button whose product row no longer exists
    # (deleted, or a stale button from an old/cached menu) crashed with a
    # TypeError further down when we tried to read product[1], product[0], etc.
    if not product:
        bot.edit_message_text(
            "⚠️ Ye product ab available nahi hai (delete ho chuka hai). Shop list refresh karne ke liye Back dabao.",
            call.message.chat.id, call.message.message_id,
            reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton(" Back", callback_data="shop_now", style="primary", icon_custom_emoji_id=btn_emo("shop_now")))
        )
        return
    # Maintenance mode: instead of the plan list, show only the admin-written
    # maintenance description, with no Buy/plan buttons.
    if product[10]:
        m_emoji = product[11] or "🔴"
        name_html = render_text_with_custom_emoji(product[0], product[5])
        text = f"{m_emoji} <b>{name_html}</b>\n\n"
        if product[12]:
            text += render_text_with_custom_emoji(product[12], product[13])
        else:
            text += "⚠️ Ye product abhi maintenance mein hai."
        viewer_id = call.from_user.id
        already_subscribed = is_notify_subscribed(product_id, viewer_id)
        markup = InlineKeyboardMarkup(row_width=1)
        if already_subscribed:
            markup.add(InlineKeyboardButton("🔔 Notified! (tap to cancel)", callback_data=f"notifyme_{product_id}", style="success", icon_custom_emoji_id=btn_emo("notifyme")))
        else:
            markup.add(InlineKeyboardButton("🔔 Notify Me", callback_data=f"notifyme_{product_id}", style="success", icon_custom_emoji_id=btn_emo("notifyme")))
        markup.add(InlineKeyboardButton(" Back", callback_data="shop_now", style="primary", icon_custom_emoji_id=btn_emo("shop_now")))
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, parse_mode="HTML", reply_markup=markup)
        return
    if not plans:
        bot.edit_message_text(
            "⚠️ Is product ke liye abhi koi plan nahi hai.", call.message.chat.id, call.message.message_id,
            reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton(" Back", callback_data="shop_now", style="primary", icon_custom_emoji_id=btn_emo("shop_now")))
        )
        return
    viewer_id = call.from_user.id
    viewer_is_reseller = is_active_reseller(viewer_id)
    markup = InlineKeyboardMarkup(row_width=1)
    for pl in plans:
        plan_price = effective_price(viewer_id, pl[2], pl[4])
        label_icon = "🏷️" if (viewer_is_reseller and pl[4] is not None) else ""
        markup.add(InlineKeyboardButton(f"{label_icon} {format_duration(pl[1], pl[3])} - ₹{plan_price}{usd_hint(plan_price)}", callback_data=f"buy_{product_id}_{pl[0]}", style="success", icon_custom_emoji_id=get_product_emoji(product[15])))
    if product[1]:
        markup.add(InlineKeyboardButton(" Watch Demo", callback_data=f"watchvid_{product_id}", style="success", icon_custom_emoji_id=btn_emo("watchvid")))
    markup.add(InlineKeyboardButton(" Back", callback_data="shop_now", style="primary", icon_custom_emoji_id=btn_emo("shop_now")))
    (name, _video, icon, compat_status, compat_status_entities, name_entities,
     duration_label, duration_label_entities, description, description_entities,
     _maint_enabled, _maint_emoji, _maint_desc, _maint_desc_entities, _premium_emoji_id, _plan_emoji_id) = product
    # Title, compatibility line, then the full description (with any Premium custom
    # emoji the admin embedded in it), then the duration list at the bottom.
    text = f"<b>{render_text_with_custom_emoji(name, name_entities)}</b>\n"
    if compat_status:
        text += f"{render_text_with_custom_emoji(compat_status, compat_status_entities)}\n"
    if description:
        text += f"\n{render_text_with_custom_emoji(description, description_entities)}\n"
    if viewer_is_reseller:
        text += f"\n🏷️ <b>Reseller Pricing Active</b>\n"
    duration_text = render_text_with_custom_emoji(duration_label, duration_label_entities) if duration_label else "Select Duration:"
    text += f"\n{duration_text}"
    bot.edit_message_text(text, call.message.chat.id, call.message.message_id, parse_mode="HTML", reply_markup=markup)

def ask_android_id(call, product_id, plan_id):
    """Step 1 of buying (Android-ID-required products): verify balance is OK, then
    ask the user to send their Android ID / Mod ID. These products don't use the
    pre-added key stock at all -- the key is unique per Android ID and typed by the
    admin by hand, so there's no stock check here, only a balance check."""
    user_id = call.from_user.id
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT balance FROM users WHERE id=?", (user_id,))
    user = cursor.fetchone()
    cursor.execute("SELECT price, reseller_price FROM plans WHERE id=?", (plan_id,))
    plan = cursor.fetchone()
    cursor.close()
    conn.close()
    if not plan:
        bot.answer_callback_query(call.id, "❌ Plan not found.", show_alert=True)
        return
    price = effective_price(user_id, plan[0], plan[1])
    if not user or user[0] < price:
        try:
            bot.answer_callback_query(call.id)
        except:
            pass
        send_insufficient_balance_message(call.message.chat.id, price, user[0] if user else 0, f"product_{product_id}", msg_id=call.message.message_id)
        return
    user_states[user_id] = {"action": "awaiting_android_id", "product_id": product_id, "plan_id": plan_id}
    cancel_markup = InlineKeyboardMarkup()
    cancel_markup.add(InlineKeyboardButton("❌ Cancel", callback_data=f"product_{product_id}", style="danger", icon_custom_emoji_id=btn_emo("cancel_product_detail")))
    bot.edit_message_text(
        "📱 <b>Apna Android ID / Mod ID bhejo:</b>\n\n"
        "Yeh ID aapko apni app ke andar milegi. ID bhejte hi aapka order confirm ho jaayega "
        "aur thodi der mein aapko key mil jaayegi.",
        call.message.chat.id, call.message.message_id, parse_mode="HTML", reply_markup=cancel_markup
    )
    bot.register_next_step_handler(call.message, receive_android_id)

def receive_android_id(message):
    if _check_cancel(message):
        return
    user_id = message.from_user.id
    state = user_states.get(user_id, {})
    if state.get("action") != "awaiting_android_id":
        return
    product_id = state.get("product_id")
    plan_id = state.get("plan_id")
    android_id = message.text.strip() if message.text else ""
    if not android_id:
        bot.reply_to(message, "❌ Invalid ID. Please send your Android ID / Mod ID as text:")
        bot.register_next_step_handler(message, receive_android_id)
        return
    user_states.pop(user_id, None)
    create_manual_order(user_id, message.chat.id, message.from_user.first_name, message.from_user.username,
                         product_id, plan_id, android_id)

def fetch_key_from_reseller(reseller_product_id, duration_label, android_id):
    """Buy one key from the Bunty reseller API.

    The provider contract is POST form data: api_key, action=buy, product_id,
    duration and android_id, plus the x-master-key header.

    Important: hours already work with the normal ``N Hours`` spelling. For
    DAY plans, some Bunty panels use Day/Days/Day's inconsistently. Therefore
    an equivalent spelling is tried only after an explicit OUT OF STOCK reply.
    No duration value is ever changed.
    """
    pid = str(reseller_product_id or "").strip()
    requested_duration = str(duration_label or "").strip()
    android = str(android_id or "").strip()
    if not pid:
        return False, "Reseller API: Product PID is empty"
    if not requested_duration:
        return False, "Reseller API: Duration is empty"
    url = get_reseller_api_url().strip()
    if not url:
        return False, "Reseller API endpoint is empty"

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json, text/plain, */*",
        "x-master-key": get_reseller_master_key(),
        "User-Agent": "VetlamOfficial-Bot/1.0",
    }

    candidates = reseller_duration_candidates(requested_duration)
    last_error = "Reseller API request failed"

    for duration_index, duration in enumerate(candidates):
        # Retry transient gateway/network failures for the SAME duration.
        for attempt in range(1, 4):
            data = {
                "api_key": get_reseller_api_key(),
                "action": "buy",
                "product_id": pid,
                "duration": duration,
                "android_id": android,
            }
            try:
                req = urllib.request.Request(
                    url,
                    data=urllib.parse.urlencode(data).encode("utf-8"),
                    headers=headers,
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    raw = resp.read().decode("utf-8", errors="ignore")
                    http_status = getattr(resp, "status", 200)
            except urllib.error.HTTPError as e:
                try:
                    body = e.read().decode("utf-8", errors="ignore")[:700]
                except Exception:
                    body = ""
                last_error = f"Reseller API HTTP {e.code}: {body or e.reason}"
                if e.code in (502, 503, 504) and attempt < 3:
                    time.sleep(1.5 * attempt)
                    continue
                return False, last_error
            except urllib.error.URLError as e:
                last_error = f"Reseller API connection failed: {e.reason}"
                if attempt < 3:
                    time.sleep(1.5 * attempt)
                    continue
                return False, last_error
            except (TimeoutError, socket.timeout) as e:
                last_error = f"Reseller API request timed out: {e}"
                if attempt < 3:
                    time.sleep(1.5 * attempt)
                    continue
                return False, last_error
            except Exception as e:
                last_error = f"Reseller API request failed: {e}"
                if attempt < 3:
                    time.sleep(1.5 * attempt)
                    continue
                return False, last_error

            try:
                parsed = json.loads(raw)
            except Exception:
                last_error = f"Reseller API HTTP {http_status} returned non-JSON: {raw[:500]}"
                if http_status in (502, 503, 504) and attempt < 3:
                    time.sleep(1.5 * attempt)
                    continue
                return False, last_error

            if str(parsed.get("status", "")).lower() != "success":
                msg = str(parsed.get("msg") or parsed.get("error") or parsed.get("message") or "Unknown reseller API error")
                low = msg.lower()
                is_stock = any(x in low for x in (
                    "out of stock", "stock unavailable", "no stock", "insufficient stock"
                ))
                if is_stock:
                    # Only try the next equivalent DAY spelling. Never switch
                    # to another numeric duration or another product.
                    last_error = f"Reseller API OUT OF STOCK: {msg} (duration={duration})"
                    break
                return False, f"Reseller API error: {msg} (duration={duration})"

            key_val = parsed.get("key") or parsed.get("license_key")
            if not key_val and isinstance(parsed.get("data"), dict):
                key_val = parsed["data"].get("key") or parsed["data"].get("license_key")
            if not key_val:
                return False, f"Reseller API reported success but no key was returned: {raw[:500]}"
            return True, str(key_val)

        # Current duration was explicitly out of stock. If another equivalent
        # spelling exists, try it. Otherwise report the provider's response.
        if duration_index + 1 < len(candidates):
            continue
        return False, last_error

    return False, last_error

def fetch_key_from_keyspanels(variant_id, quantity=1):
    """KeysPanelShop reseller_api.php contract.

    The selected plan's numeric Variant ID is the only product identifier sent
    to KeysPanelShop. Duration/Android ID are never sent. Network/timeout failures
    are retried once with the same idempotency key.
    """
    variant = str(variant_id or "").strip()
    if not variant or not variant.isdigit():
        return False, "KeysPanelShop Variant ID must be numeric"
    try:
        qty = max(1, min(10, int(quantity)))
    except Exception:
        qty = 1
    master_key = get_keyspanels_master_key().strip()
    if not master_key:
        return False, "KeysPanelShop Master Key is not configured"
    url = get_keyspanels_request_url()
    if not url:
        return False, "KeysPanelShop API endpoint is empty"
    idempotency_key = "vetlam_" + variant + "_" + uuid.uuid4().hex
    data = {"action":"buy", "variant_id":int(variant), "quantity":qty, "idempotency_key":idempotency_key}
    headers = {"Content-Type":"application/x-www-form-urlencoded", "Accept":"application/json",
               "x-master-key":master_key, "User-Agent":"VetlamOfficial-Bot/1.0"}
    last_error = ""
    for attempt in range(1,3):
        try:
            req = urllib.request.Request(url, data=urllib.parse.urlencode(data).encode("utf-8"), headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=65) as resp:
                raw = resp.read().decode("utf-8", errors="ignore")
                http_status = getattr(resp,"status",200)
        except urllib.error.HTTPError as e:
            try: body=e.read().decode("utf-8", errors="ignore")[:700]
            except Exception: body=""
            return False, f"KeysPanelShop HTTP {e.code}: {body or e.reason}"
        except urllib.error.URLError as e:
            last_error=f"KeysPanelShop connection failed: {e.reason}"
            if attempt==1: continue
            return False,last_error
        except TimeoutError as e:
            last_error=f"KeysPanelShop request timed out: {e}"
            if attempt==1: continue
            return False,last_error
        except Exception as e:
            last_error=f"KeysPanelShop request failed: {e}"
            if attempt==1: continue
            return False,last_error
        if not raw:
            last_error="KeysPanelShop returned an empty response"
            if attempt==1: continue
            return False,last_error
        try:
            parsed=json.loads(raw)
        except Exception:
            last_error=f"KeysPanelShop HTTP {http_status} returned non-JSON: {raw[:500]}"
            if attempt==1: continue
            return False,last_error
        if parsed.get("ok") is not True:
            msg=parsed.get("error") or parsed.get("message") or parsed.get("msg") or "Unknown KeysPanelShop error"
            shortfall=parsed.get("shortfall")
            if shortfall is not None: msg=f"{msg} (need ₹{shortfall} more)"
            return False,f"KeysPanelShop error: {msg}"
        key_val=parsed.get("key")
        keys=parsed.get("keys")
        if not key_val and isinstance(keys,list) and keys: key_val=keys[0]
        if not key_val and isinstance(keys,dict): key_val=keys.get("key") or keys.get("license_key")
        if not key_val:
            return False,f"KeysPanelShop returned ok=true but no key was returned: {raw[:500]}"
        return True,{"key":str(key_val),"order_id":str(parsed.get("order_id") or ""),"expires_at":str(parsed.get("expires_at") or "")}
    return False,last_error or "KeysPanelShop request failed"


def _create_keyspanels_order_impl(user_id, chat_id, first_name, username, product_id, plan_id):
    """KeysPanelShop path. API is called before wallet deduction.

    The public wrapper below serializes purchases per user so a double-tap cannot
    create two provider orders before the first wallet deduction completes.
    """
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT balance FROM users WHERE id=?", (user_id,))
    user = cursor.fetchone()
    cursor.execute("SELECT price, days, duration_unit, reseller_price FROM plans WHERE id=?", (plan_id,))
    plan = cursor.fetchone()
    cursor.execute("SELECT name, channel_link, keyspanels_variant_id, external_api_enabled, external_api_provider FROM products WHERE id=?", (product_id,))
    product = cursor.fetchone()
    plan_variant_row = cursor.execute("SELECT keyspanels_variant_id FROM plans WHERE id=? AND product_id=?", (plan_id, product_id)).fetchone()
    plan_variant_id = str(plan_variant_row[0]).strip() if plan_variant_row and plan_variant_row[0] else ""
    cursor.close()
    conn.close()

    if not plan or not product:
        bot.send_message(chat_id, "❌ Product/Plan not found. Please try again.")
        return
    price = effective_price(user_id, plan[0], plan[3])
    if not user or user[0] < price:
        send_insufficient_balance_message(chat_id, price, user[0] if user else 0, f"product_{product_id}")
        return
    if not product[3] or get_external_api_provider(product[4]) != "keyspanels" or not plan_variant_id:
        bot.send_message(chat_id, "❌ Is duration ka KeysPanelShop Variant ID set nahi hai. Admin → Edit Product → Manage KeysPanel Variants mein is plan ka Variant ID set karo.")
        return

    anim_msg = start_key_generation_animation(chat_id, product[0])
    ok, result = fetch_key_from_keyspanels(plan_variant_id, quantity=1)
    finish_key_generation_animation(chat_id, anim_msg, product[0])

    if not ok:
        try:
            bot.send_message(ADMIN_USER_ID,
                f"⚠️ KeysPanelShop auto-delivery failed. Wallet charge nahi kiya gaya.\\n"
                f"Product: {html.escape(product[0])}\\nPlan: {html.escape(format_duration(plan[1], plan[2]))}\\nVariant ID: <code>{html.escape(str(plan_variant_id))}</code>\\n"
                f"Reason: {html.escape(str(result))}", parse_mode="HTML")
        except Exception:
            pass
        bot.send_message(chat_id, "❌ Abhi key generate nahi ho paayi, isliye aapka balance deduct nahi hua. Thodi der baad dobara try karein.")
        return

    key_val = result["key"]
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT balance FROM users WHERE id=?", (user_id,))
    fresh_user = cursor.fetchone()
    if not fresh_user or fresh_user[0] < price:
        conn.rollback()
        cursor.close()
        conn.close()
        try:
            bot.send_message(ADMIN_USER_ID,
                f"⚠️ KeysPanelShop returned a key but user {user_id} no longer has enough balance.\\n"
                f"API Order: <code>{html.escape(result['order_id'])}</code>", parse_mode="HTML")
        except Exception:
            pass
        bot.send_message(chat_id, "❌ Balance change detected during order. Please contact support before retrying.")
        return

    new_bal = fresh_user[0] - price
    cursor.execute("UPDATE users SET balance=? WHERE id=?", (new_bal, user_id))
    cursor.execute("INSERT INTO transactions (user_id, amount, type, details) VALUES (?,?,'purchase',?)",
                   (user_id, price, f"Bought {product[0]} - {format_duration(plan[1], plan[2])} (KeysPanelShop API)"))
    referral_commission, referral_referrer = apply_referral_commission(cursor, user_id, price, product[0])
    cursor.execute("INSERT INTO manual_orders (user_id, product_id, plan_id, android_id, price, status, license_key) VALUES (?,?,?,?,?,'completed',?)",
                   (user_id, product_id, plan_id, "", price, key_val))
    first_purchase_bonus = apply_first_purchase_bonus(cursor, user_id)
    conn.commit()
    cursor.close()
    conn.close()

    if referral_commission and referral_referrer:
        try:
            bot.send_message(referral_referrer,
                f"💸 <b>Referral Commission</b>\\n\\nReferred user <code>{user_id}</code> purchased "
                f"<b>{html.escape(product[0])}</b>.\\nCommission: <b>₹{referral_commission}</b>", parse_mode="HTML")
        except Exception:
            pass

    deliver_key_to_user(chat_id, user_id, product[0], product[1],
                        format_duration(plan[1], plan[2]), price, key_val, "", plan)
    if first_purchase_bonus:
        try:
            bot.send_message(chat_id, f"🎁 <b>First Purchase Bonus</b>\\n₹{first_purchase_bonus} bonus added to your balance.", parse_mode="HTML")
        except Exception:
            pass
    # KeysPanelShop admin delivery notification — same clean layout as the
    # existing Bunty/Reseller auto-delivery notification, with the provider
    # name changed to KeysPanelShop.
    try:
        kp_username = f"@{username}" if username else "N/A"
        bot.send_message(ADMIN_USER_ID,
            f"🟢 <b>AUTO-DELIVERED ORDER!</b>\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"👤 Name: {html.escape(first_name or 'N/A')}\n"
            f"🆔 User ID: {user_id}\n"
            f"👤 Username: {html.escape(kp_username)}\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"🎮 Product: {html.escape(product[0])}\n"
            f"⏱️ Duration: {html.escape(format_duration(plan[1], plan[2]))}\n"
            f"💰 Amount: ₹{price}\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"🔑 Key: <code>{html.escape(key_val)}</code>\n"
            f"🟣 Provider: <b>KeysPanelShop</b>\n"
            f"🧾 API Order: <code>{html.escape(result['order_id'] or 'N/A')}</code>\n"
            f"📅 Expires: <code>{html.escape(result['expires_at'] or 'N/A')}</code>\n"
            f"━━━━━━━━━━━━━━━━━",
            parse_mode="HTML")
    except Exception:
        pass

def create_keyspanels_order(user_id, chat_id, first_name, username, product_id, plan_id):
    lock = _get_purchase_lock(user_id)
    if not lock.acquire(blocking=False):
        bot.send_message(chat_id, "⏳ Aapka previous order abhi process ho raha hai. Please 1 baar wait karein — dobara Buy dabane ki zaroorat nahi hai.")
        return
    try:
        return _create_keyspanels_order_impl(user_id, chat_id, first_name, username, product_id, plan_id)
    finally:
        lock.release()


def create_manual_order(user_id, chat_id, first_name, username, product_id, plan_id, android_id):
    """Create/deliver a Bunty order safely. Provider is called BEFORE charging.
    API failure or OUT OF STOCK therefore never consumes wallet balance.
    """
    conn=get_db(); cur=conn.cursor()
    cur.execute("SELECT balance FROM users WHERE id=?", (user_id,)); user=cur.fetchone()
    cur.execute("SELECT price, days, duration_unit, reseller_price FROM plans WHERE id=?", (plan_id,)); plan=cur.fetchone()
    cur.execute("SELECT name, channel_link, reseller_product_id, external_api_enabled, external_api_provider FROM products WHERE id=?", (product_id,)); product=cur.fetchone()
    cur.close(); conn.close()
    if not plan or not product:
        bot.send_message(chat_id, "❌ Product/Plan not found. Please try again."); return
    price=effective_price(user_id, plan[0], plan[3])
    if not user or user[0] < price:
        send_insufficient_balance_message(chat_id, price, user[0] if user else 0, f"product_{product_id}"); return

    prod_name, channel_link = product[0], product[1]
    pid=str(product[2] or "").strip()
    enabled=bool(product[3])
    provider=get_external_api_provider(product[4])
    duration_label=format_duration(plan[1], plan[2])
    reseller_duration=format_duration_for_reseller(plan[1], plan[2])

    if provider != "bunty" or not enabled or not pid:
        bot.send_message(chat_id, "❌ Is product ka Bunty Reseller API configuration complete nahi hai. Admin mein External API ON, Provider Bunty aur valid PID check karo.")
        return
    if not reseller_duration:
        bot.send_message(chat_id, "❌ Is plan ki duration Bunty API format mein valid nahi hai. Admin mein plan duration check karo.")
        return

    anim_msg=start_key_generation_animation(chat_id, prod_name)
    ok,result=fetch_key_from_reseller(pid, reseller_duration, android_id)
    finish_key_generation_animation(chat_id, anim_msg, prod_name)
    if not ok:
        reason=str(result)
        try:
            bot.send_message(ADMIN_USER_ID,
                f"⚠️ <b>Reseller auto-delivery failed</b>\n\n"
                f"👤 User: <code>{user_id}</code>\n🎮 Product: {html.escape(prod_name)}\n"
                f"🆔 PID: <code>{html.escape(pid)}</code>\n"
                f"⏱️ Duration sent: <code>{html.escape(reseller_duration)}</code>\n"
                f"📱 Android ID: <code>{html.escape(str(android_id or ''))}</code>\n"
                f"❗ Reason: {html.escape(reason)}\n\n💡 Wallet charge nahi kiya gaya.", parse_mode="HTML")
        except Exception: pass
        if "OUT OF STOCK" in reason.upper():
            bot.send_message(chat_id, "❌ Is duration ka stock reseller panel par available nahi hai. Aapka balance deduct nahi hua.")
        else:
            bot.send_message(chat_id, "❌ Key generate nahi ho paayi. Aapka balance deduct nahi hua. Thodi der baad dobara try karein.")
        return

    # Provider returned a key. Re-read balance and charge only now.
    conn=get_db(); cur=conn.cursor()
    try:
        cur.execute("SELECT balance FROM users WHERE id=?", (user_id,)); latest=cur.fetchone()
        if not latest or latest[0] < price:
            conn.rollback(); bot.send_message(chat_id, "❌ Balance order ke dauran change ho gaya. Key reserve ho chuki hai; support se contact karein."); return
        new_bal=latest[0]-price
        cur.execute("UPDATE users SET balance=? WHERE id=?", (new_bal,user_id))
        cur.execute("INSERT INTO transactions (user_id, amount, type, details) VALUES (?,?,'purchase',?)", (user_id,price,f"Bought {prod_name} - {duration_label}" + (" (Android ID)" if android_id else "")))
        referral_commission, referral_referrer=apply_referral_commission(cur,user_id,price,prod_name)
        cur.execute("INSERT INTO manual_orders (user_id, product_id, plan_id, android_id, price, status, license_key) VALUES (?,?,?,?,?,'completed',?)", (user_id,product_id,plan_id,android_id,price,str(result)))
        first_purchase_bonus=apply_first_purchase_bonus(cur,user_id); new_bal+=first_purchase_bonus
        conn.commit()
    except Exception:
        conn.rollback(); raise
    finally:
        cur.close(); conn.close()

    if referral_commission and referral_referrer:
        try: bot.send_message(referral_referrer, f"💸 <b>Referral Commission</b>\n\nReferred user <code>{user_id}</code> purchased <b>{html.escape(prod_name)}</b>.\nCommission: <b>₹{referral_commission}</b>", parse_mode="HTML")
        except Exception: pass
    deliver_key_to_user(chat_id,user_id,prod_name,channel_link,duration_label,price,str(result),android_id,plan)
    try:
        bot.send_message(ADMIN_USER_ID, f"✅ <b>Auto-delivered via Bunty Reseller API</b>\n\n👤 User: {user_id}\n🎮 Product: {html.escape(prod_name)}\n⏱️ Duration: {duration_label}\n💰 Amount: ₹{price}\n🔑 Key: <code>{html.escape(str(result))}</code>", parse_mode="HTML")
    except Exception: pass


def deliver_key_to_user(chat_id, user_id, prod_name, channel_link, duration_label, price, key_text, android_id, plan):
    """Sends the final formatted key message to the user, with the
    channel-join button if one is configured. Shared by both the reseller
    auto-delivery path and could be reused elsewhere."""
    seconds = plan[1] * (3600 if (plan[2] or "days").lower() == "hours" else 86400)
    expires_dt = datetime.now() + timedelta(seconds=seconds)
    expires_str = expires_dt.strftime("%Y-%m-%d %I:%M %p")
    android_id_block = f"{emo('deliver_androidid')} Android ID: <code>{html.escape(android_id)}</code>\n\n" if android_id else ""
    caption = (
        f"{emo('deliver_success')} <b>{html.escape(prod_name)} Key Generated Successfully!</b>\n\n"
        f"{emo('deliver_product')} Product: <b>{html.escape(prod_name)}</b>\n"
        f"{emo('deliver_type')} Type: {emo('deliver_regular')} Regular\n"
        f"{emo('deliver_duration')} Duration: {duration_label}\n"
        f"{emo('deliver_price')} Price: ₹{price}\n\n"
        f"{emo('deliver_key')} Your Key: <code>{html.escape(key_text)}</code>\n\n"
        f"{emo('deliver_expires')} Expires: {expires_str}\n\n"
        f"{android_id_block}"
        f"{emo('deliver_enjoy')} ENJOY YOUR {html.escape(prod_name).upper()} {emo('deliver_product')}"
    )
    update_link = channel_link or get_setting("update_channel_link")
    markup = None
    if update_link:
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton(f"📢 Join {prod_name} Updates", url=update_link, style="success", icon_custom_emoji_id=btn_emo("link_join_updates")))
    bot.send_message(chat_id, caption, parse_mode="HTML", reply_markup=markup)

def prompt_send_key(call, order_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT status FROM manual_orders WHERE id=?", (order_id,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    if not row:
        bot.answer_callback_query(call.id, "❌ Order not found.", show_alert=True)
        return
    if row[0] != "pending":
        bot.answer_callback_query(call.id, f"⚠️ Ye order already {row[0]} hai.", show_alert=True)
        return
    user_states[ADMIN_USER_ID] = {"action": "awaiting_manual_key", "order_id": order_id}
    cancel_markup = InlineKeyboardMarkup()
    cancel_markup.add(InlineKeyboardButton("❌ Cancel", callback_data="admin_panel", style="danger", icon_custom_emoji_id=btn_emo("admin_panel")))
    bot.send_message(call.message.chat.id,
        "✍️ <b>Poora message type karo</b> jo is user ko bhejna hai (key + jo bhi text chaho, "
        "jitna bhi lamba ho, sab chalega):",
        parse_mode="HTML", reply_markup=cancel_markup)
    bot.register_next_step_handler(call.message, save_manual_key)

def start_key_generation_animation(chat_id, prod_name=None):
    """Sends a 'Generating your key...' progress bar and steps it partway (up
    to 85%) with small pauses in between, then returns the message so the
    caller can keep doing real work (reseller API call, DB writes) while it's
    on screen. Call finish_key_generation_animation() afterwards to jump it to
    100% and clear it. Purely cosmetic -- the key itself is decided elsewhere;
    this just makes delivery feel like it's actively being fetched. Never
    raises -- if Telegram calls fail (e.g. blocked bot) this quietly returns
    None/keeps going, so it can never break an actual purchase."""
    label = f" — {html.escape(prod_name)}" if prod_name else ""
    try:
        msg = bot.send_message(chat_id, f"🔑 <b>Generating your key{label}...</b>\n\n[░░░░░░░░░░] 0%", parse_mode="HTML")
    except Exception:
        return None
    for pct in (15, 35, 55, 70, 85):
        time.sleep(0.35)
        filled = pct // 10
        bar = "▓" * filled + "░" * (10 - filled)
        try:
            bot.edit_message_text(f"🔑 <b>Generating your key{label}...</b>\n\n[{bar}] {pct}%", chat_id, msg.message_id, parse_mode="HTML")
        except Exception:
            pass
    return msg

def finish_key_generation_animation(chat_id, anim_msg, prod_name=None):
    """Jumps the progress bar to 100% for a beat, then deletes it so the real
    key message appears right after it in the chat."""
    if not anim_msg:
        return
    label = f" — {html.escape(prod_name)}" if prod_name else ""
    try:
        bot.edit_message_text(f"🔑 <b>Generating your key{label}...</b>\n\n[▓▓▓▓▓▓▓▓▓▓] 100% ✅", chat_id, anim_msg.message_id, parse_mode="HTML")
        time.sleep(0.5)
    except Exception:
        pass
    try:
        bot.delete_message(chat_id, anim_msg.message_id)
    except Exception:
        pass

def save_manual_key(message):
    if _check_cancel(message):
        return
    admin_id = message.from_user.id
    state = user_states.get(admin_id, {})
    if state.get("action") != "awaiting_manual_key":
        return
    order_id = state.get("order_id")
    key_text = message.text
    if not key_text:
        bot.reply_to(message, "❌ Empty message. Text bhejo jo user ko jaana hai:")
        bot.register_next_step_handler(message, save_manual_key)
        return
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, product_id, status FROM manual_orders WHERE id=?", (order_id,))
    row = cursor.fetchone()
    if not row:
        cursor.close()
        conn.close()
        bot.reply_to(message, "❌ Order not found.")
        return
    target_user_id, product_id, status = row[0], row[1], row[2]
    if status != "pending":
        cursor.close()
        conn.close()
        bot.reply_to(message, f"⚠️ Ye order already {status} hai.")
        return
    cursor.execute("SELECT name, channel_link FROM products WHERE id=?", (product_id,))
    prod_row = cursor.fetchone()
    cursor.execute("UPDATE manual_orders SET status='completed', license_key=? WHERE id=?", (key_text, order_id))
    first_purchase_bonus = apply_first_purchase_bonus(cursor, target_user_id)
    conn.commit()
    cursor.close()
    conn.close()
    user_states.pop(admin_id, None)

    join_markup = None
    if prod_row:
        prod_name, channel_link = prod_row[0], prod_row[1]
        update_link = channel_link or get_setting("update_channel_link")
        if update_link:
            join_markup = InlineKeyboardMarkup()
            join_markup.add(InlineKeyboardButton(f"📢 Join {prod_name} Updates", url=update_link, style="success", icon_custom_emoji_id=btn_emo("link_join_updates")))

    anim_msg = start_key_generation_animation(target_user_id, prod_row[0] if prod_row else None)
    finish_key_generation_animation(target_user_id, anim_msg, prod_row[0] if prod_row else None)
    try:
        bot.send_message(target_user_id, key_text, reply_markup=join_markup)
        if first_purchase_bonus:
            bot.send_message(target_user_id, f"🎁 First Purchase Bonus: ₹{first_purchase_bonus} added to your balance.")
        bot.reply_to(message, "✅ Key bhej di gayi user ko!")
    except Exception as e:
        bot.reply_to(message, f"❌ User ko message nahi bhej paya (shayad usne bot block kiya hai): {e}")

def reject_manual_order(call, order_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, price, status FROM manual_orders WHERE id=?", (order_id,))
    row = cursor.fetchone()
    if not row:
        cursor.close()
        conn.close()
        bot.answer_callback_query(call.id, "❌ Order not found.", show_alert=True)
        return
    target_user_id, price, status = row[0], row[1], row[2]
    if status != "pending":
        cursor.close()
        conn.close()
        bot.answer_callback_query(call.id, f"⚠️ Ye order already {status} hai.", show_alert=True)
        return
    cursor.execute("UPDATE users SET balance = balance + ? WHERE id = ?", (price, target_user_id))
    cursor.execute("INSERT INTO transactions (user_id, amount, type, details) VALUES (?,?,'refund',?)",
                   (target_user_id, price, f"Refund for rejected manual order #{order_id}"))
    cursor.execute("UPDATE manual_orders SET status='refunded' WHERE id=?", (order_id,))
    conn.commit()
    cursor.close()
    conn.close()
    try:
        bot.send_message(target_user_id, f"{emo('reject_title')} Aapka order cancel kar diya gaya hai. ₹{price} refund kar diya gaya hai aapke balance mein.", parse_mode="HTML")
    except:
        pass
    try:
        bot.edit_message_text(f"❌ <b>REJECTED & REFUNDED</b>\n\n👤 User: {target_user_id}\n💰 Amount: ₹{price}", call.message.chat.id, call.message.message_id, parse_mode="HTML")
    except:
        bot.answer_callback_query(call.id, "❌ Rejected & refunded")


def process_purchase(user_id, chat_id, first_name, username, product_id, plan_id, msg_id=None, call_id=None):
    """Instant stock-based delivery, used for every product EXCEPT the ones where
    the admin has turned the Android ID Flow ON (those go through create_manual_order
    instead, since their keys are hand-typed by the admin, not pre-stocked)."""

    def notify_error(text):
        # If we have a callback (button tap) to answer, show a popup alert --
        # never send a permanent chat message for a simple "insufficient balance"
        # / "out of stock" tap, or repeated taps pile up junk messages in the chat.
        if call_id:
            try:
                bot.answer_callback_query(call_id, text, show_alert=True)
                return
            except:
                pass
        bot.send_message(chat_id, text)

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT balance FROM users WHERE id=?", (user_id,))
    user = cursor.fetchone()
    cursor.execute("SELECT price, days, duration_unit, reseller_price FROM plans WHERE id=?", (plan_id,))
    plan = cursor.fetchone()
    cursor.execute("SELECT name, channel_link FROM products WHERE id=?", (product_id,))
    product = cursor.fetchone()
    if not plan or not product:
        cursor.close()
        conn.close()
        notify_error("❌ Product/Plan not found. Please try again.")
        return
    price = effective_price(user_id, plan[0], plan[3])

    if not user or user[0] < price:
        cursor.close()
        conn.close()
        if call_id:
            try:
                bot.answer_callback_query(call_id)
            except:
                pass
        send_insufficient_balance_message(chat_id, price, user[0] if user else 0, f"product_{product_id}", msg_id=msg_id)
        return
    cursor.execute("SELECT id, license_key FROM license_keys WHERE product_id=? AND plan_id=? AND used=0 LIMIT 1", (product_id, plan_id))
    key = cursor.fetchone()
    if not key:
        cursor.close()
        conn.close()
        notify_error("❌ Out of stock!")
        return
    new_bal = user[0] - price
    cursor.execute("UPDATE users SET balance=? WHERE id=?", (new_bal, user_id))
    cursor.execute("UPDATE license_keys SET used=1, used_by=?, used_at=CURRENT_TIMESTAMP WHERE id=?", (user_id, key[0]))
    cursor.execute("INSERT INTO transactions (user_id, amount, type, details) VALUES (?,?,'purchase',?)",
                   (user_id, price, f"Bought {product[0]} - {format_duration(plan[1], plan[2])}"))
    referral_commission, referral_referrer = apply_referral_commission(cursor, user_id, price, product[0])
    first_purchase_bonus = apply_first_purchase_bonus(cursor, user_id)
    new_bal += first_purchase_bonus
    conn.commit()
    cursor.close()
    conn.close()
    if referral_commission and referral_referrer:
        try: bot.send_message(referral_referrer, f"💸 <b>Referral Commission</b>\n\nReferred user <code>{user_id}</code> purchased <b>{html.escape(product[0])}</b>.\nCommission: <b>₹{referral_commission}</b>", parse_mode="HTML")
        except: pass

    prod_name = product[0]
    update_link = product[1] or get_setting("update_channel_link")
    purchase_markup = None
    if update_link:
        purchase_markup = InlineKeyboardMarkup()
        purchase_markup.add(InlineKeyboardButton(f"📢 Join {prod_name} Updates", url=update_link, style="success", icon_custom_emoji_id=btn_emo("link_join_updates")))

    anim_msg = start_key_generation_animation(chat_id, prod_name)
    finish_key_generation_animation(chat_id, anim_msg, prod_name)

    caption = (f"{emo('buy_success')} <b>Purchase Successful!</b>\n\n"
               f"{emo('buy_product')} {html.escape(prod_name)}\n"
               f"{emo('buy_key')} <code>{html.escape(key[1])}</code>\n"
               f"{emo('buy_balance')} New Balance: ₹{new_bal}"
               + (f"\n🎁 First Purchase Bonus: <b>₹{first_purchase_bonus}</b>" if first_purchase_bonus else ""))

    try:
        bot.send_photo(chat_id, SUCCESS_IMG, caption=caption, parse_mode="HTML", reply_markup=purchase_markup)
        if msg_id:
            bot.delete_message(chat_id, msg_id)
    except:
        if msg_id:
            bot.edit_message_text(caption, chat_id, msg_id, parse_mode="HTML", reply_markup=purchase_markup)
        else:
            bot.send_message(chat_id, caption, parse_mode="HTML", reply_markup=purchase_markup)

    display_name = first_name or "N/A"
    username_str = f"@{username}" if username else "N/A"
    time_str = now_ist().strftime("%d-%m-%Y %I:%M %p")
    order_notify = build_order_notify_text(display_name, user_id, username_str, prod_name, key[1], price, new_bal, time_str)
    bot.send_message(ADMIN_USER_ID, order_notify, parse_mode="HTML")
    try:
        bot.send_message(user_id, order_notify, parse_mode="HTML")
    except:
        pass


# ======================= REFERRAL + DAILY SPIN =======================
def _setting_bool(key, default=True):
    v=get_setting(key)
    if v is None or v == "": return default
    return str(v).lower() in ("1","true","yes","on","enabled")

def get_referral_reward():
    # Join bonus has been removed. Kept only for backward compatibility with old DBs.
    return 0

def referral_enabled():
    return _setting_bool("referral_enabled", True)

def referral_commission_enabled():
    return _setting_bool("referral_commission_enabled", True)

def get_referral_commission_percent():
    try: return max(0, min(100, int(get_setting("referral_commission_percent") or "5")))
    except: return 5

def get_referral_min_purchase():
    try: return max(0, int(get_setting("referral_min_purchase") or "0"))
    except: return 0

def get_spin_config():

    """Returns [(reward, probability), ...]. New format: 0:50,5:30,10:15,20:5.
    Old format 0,5,10,20,50 is supported and treated as equal probability."""
    raw=get_setting("spin_probabilities")
    if raw:
        vals=[]; total=0
        for part in raw.split(","):
            try:
                a,b=part.split(":",1); reward=max(0,int(a.strip())); prob=float(b.strip())
                if prob>0: vals.append((reward,prob)); total+=prob
            except: pass
        if vals and total>0:
            return [(r,p*100.0/total) for r,p in vals]
    vals=[]
    for x in (get_setting("spin_rewards") or "0,5,10,20,50").split(","):
        try: vals.append(max(0,int(x.strip())))
        except: pass
    vals=vals or [0]
    prob=100.0/len(vals)
    return [(v,prob) for v in vals]

def get_spin_rewards():
    return [r for r,_ in get_spin_config()]

def spin_enabled():
    return _setting_bool("spin_enabled", True)

def pick_spin_reward():
    cfg=get_spin_config()
    return random.choices([r for r,_ in cfg], weights=[p for _,p in cfg], k=1)[0]

def referral_link(uid):
    try: return f"https://t.me/{bot.get_me().username}?start=ref_{uid}"
    except: return f"https://t.me/?start=ref_{uid}"

def process_referral_start(uid,payload):
    # Referral link still records the referrer, but there is NO join bonus.
    if not referral_enabled() or not payload or not payload.startswith("ref_"): return
    try: referrer=int(payload.split("_",1)[1])
    except: return
    if referrer==uid: return
    conn=get_db(); cur=conn.cursor()
    cur.execute("SELECT referred_by FROM users WHERE id=?",(uid,)); row=cur.fetchone()
    cur.execute("SELECT id FROM users WHERE id=?",(referrer,)); exists=cur.fetchone()
    if not row or row[0] is not None or not exists:
        cur.close(); conn.close(); return
    cur.execute("UPDATE users SET referred_by=? WHERE id=?",(referrer,uid))
    conn.commit(); cur.close(); conn.close()

def apply_referral_commission(cur, buyer_uid, purchase_amount, product_name):
    """Credit one commission for a completed product purchase. Must be called
    inside the purchase DB transaction after the purchase is recorded."""
    if not referral_enabled() or not referral_commission_enabled(): return 0, None
    try: amount=int(purchase_amount)
    except: return 0, None
    if amount < get_referral_min_purchase(): return 0, None
    cur.execute("SELECT referred_by FROM users WHERE id=?",(buyer_uid,)); row=cur.fetchone()
    if not row or row[0] is None: return 0, None
    referrer=int(row[0]); pct=get_referral_commission_percent()
    commission=(amount*pct)//100
    if commission<=0: return 0, referrer
    cur.execute("UPDATE users SET balance=balance+?, referral_earnings=COALESCE(referral_earnings,0)+? WHERE id=?",(commission,commission,referrer))
    cur.execute("INSERT INTO transactions(user_id,amount,type,details) VALUES(?,?,?,?)",(referrer,commission,"referral_commission",f"{pct}% commission from referred user {buyer_uid} purchase: {product_name}"))
    return commission, referrer

def show_referral(call,uid):
    conn=get_db(); cur=conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users WHERE referred_by=?",(uid,)); invited=cur.fetchone()[0]
    cur.execute("SELECT COALESCE(referral_earnings,0) FROM users WHERE id=?",(uid,)); row=cur.fetchone(); earned=row[0] if row else 0
    cur.execute("SELECT COUNT(*), COALESCE(SUM(amount),0) FROM transactions WHERE user_id=? AND type='referral_commission'",(uid,)); comm_orders,comm_earned=cur.fetchone()
    cur.close(); conn.close()
    if referral_commission_enabled():
        commission_line=f"💸 Purchase commission: <b>{get_referral_commission_percent()}%</b>"
        min_line=f"📌 Minimum purchase: ₹{get_referral_min_purchase()}"
    else:
        commission_line="💸 Purchase commission: <b>OFF</b>"
        min_line=""
    text=(f"{emo('referral_title')} <b>Referral Program</b>\n\n{emo('referral_link')} <code>{html.escape(referral_link(uid))}</code>\n\n"
          f"{emo('referral_invited')} Invited: <b>{invited}</b>\n{emo('referral_bonus')} Join bonus: <b>₹{get_referral_reward()}</b>\n"
          f"{emo('referral_earnings')} Total referral earnings: <b>₹{earned}</b>\n{commission_line}\n"
          f"{emo('referral_commission')} Commission earned: <b>₹{comm_earned}</b> ({comm_orders} purchases)\n{min_line}")
    mk=InlineKeyboardMarkup().add(InlineKeyboardButton("◀️ Back",callback_data="back_main",style="primary",icon_custom_emoji_id=btn_emo("back_main")))
    bot.edit_message_text(text,call.message.chat.id,call.message.message_id,parse_mode="HTML",reply_markup=mk)

def show_spin(call,uid):
    if not spin_enabled():
        bot.answer_callback_query(call.id,"🎡 Daily Spin abhi disabled hai.",show_alert=True); return
    conn=get_db(); cur=conn.cursor(); cur.execute("SELECT last_spin_date FROM users WHERE id=?",(uid,)); row=cur.fetchone(); cur.close(); conn.close()
    if row and row[0]==ist_today_str():
        bot.answer_callback_query(call.id,"🎡 Aaj ka spin already use ho gaya.",show_alert=True); return
    cfg=get_spin_config(); rewards="\n".join(f"• ₹{r} — {p:.0f}% chance" for r,p in cfg)
    mk=InlineKeyboardMarkup(); mk.add(InlineKeyboardButton("🎡 SPIN NOW",callback_data="spin_now",style="success")); mk.add(InlineKeyboardButton("◀️ Back",callback_data="back_main",style="primary",icon_custom_emoji_id=btn_emo("back_main")))
    bot.edit_message_text(f"{emo('spin_title')} <b>Daily Spin</b>\n\n{emo('spin_reward')} <b>Possible rewards</b>\n{rewards}\n\n{emo('spin_timer')} 1 spin per day",call.message.chat.id,call.message.message_id,parse_mode="HTML",reply_markup=mk)

def perform_spin(call,uid):
    if not spin_enabled():
        bot.answer_callback_query(call.id,"Daily Spin disabled hai.",show_alert=True); return
    conn=get_db(); cur=conn.cursor(); cur.execute("SELECT last_spin_date FROM users WHERE id=?",(uid,)); row=cur.fetchone()
    if row and row[0]==ist_today_str():
        cur.close(); conn.close(); bot.answer_callback_query(call.id,"Aaj ka spin already use ho gaya.",show_alert=True); return
    reward=pick_spin_reward(); cur.execute("UPDATE users SET last_spin_date=?, spin_count=COALESCE(spin_count,0)+1 WHERE id=?",(ist_today_str(),uid))
    cur.execute("UPDATE users SET balance=balance+? WHERE id=?",(reward,uid))
    cur.execute("INSERT INTO transactions(user_id,amount,type,details) VALUES(?,?,?,?)",(uid,reward,"spin_reward",f"Daily spin reward (₹{reward})"))
    conn.commit(); cur.close(); conn.close()
    # The five frames are Premium-Emoji-Manager slots. A Telegram Premium
    # custom emoji can be animated, so admins can make the spin look animated
    # without changing the existing button/menu flow.
    frames = [
        f"{emo('spin_anim_1')} <b>Spinning...</b>",
        f"{emo('spin_anim_2')} {emo('spin_anim_1')} {emo('spin_anim_2')}",
        f"{emo('spin_anim_3')} {emo('spin_anim_4')} {emo('spin_anim_3')}",
        f"{emo('spin_anim_4')} <b>Almost there...</b> {emo('spin_anim_4')}",
        f"{emo('spin_anim_5')} <b>Result ready!</b> {emo('spin_anim_5')}",
    ]
    for frame in frames:
        try:
            bot.edit_message_text(frame,call.message.chat.id,call.message.message_id,parse_mode="HTML")
            time.sleep(.35)
        except Exception:
            # Telegram may reject an edit if the previous frame is identical or
            # the user/network is slow. The final result is still sent below.
            pass
    mk=InlineKeyboardMarkup().add(InlineKeyboardButton("◀️ Back",callback_data="back_main",style="primary",icon_custom_emoji_id=btn_emo("back_main")))
    bot.edit_message_text(f"{emo('spin_result')} <b>Spin Result</b>\n\nYou won <b>₹{reward}</b>!\n\n{emo('referral_earnings')} Reward wallet mein add ho gaya.",call.message.chat.id,call.message.message_id,parse_mode="HTML",reply_markup=mk)

# ======================= LEADERBOARD =======================
def leaderboard_enabled():
    return get_setting("leaderboard_enabled") != "0"

def leaderboard_show_amount():
    return get_setting("leaderboard_show_amount") == "1"

def _leaderboard_name(first_name, username):
    name=(first_name or "").strip()
    if name:
        return name
    uname=(username or "").strip().lstrip("@")
    return uname if uname else "User"

def _leaderboard_rows(limit=10):
    conn=get_db(); cur=conn.cursor()
    try:
        cur.execute("""
            SELECT u.id,u.first_name,u.username,COALESCE(SUM(t.amount),0) AS spent,COUNT(t.id) AS purchases
            FROM transactions t JOIN users u ON u.id=t.user_id
            WHERE t.type='purchase' AND t.amount>0
            GROUP BY u.id
            HAVING COALESCE(SUM(t.amount),0)>0
            ORDER BY spent DESC, purchases DESC, u.id ASC
            LIMIT ?
        """, (max(1,int(limit)),))
        return cur.fetchall()
    finally:
        cur.close(); conn.close()

def show_leaderboard(call):
    try:
        if not leaderboard_enabled():
            bot.answer_callback_query(call.id, "🏆 Leaderboard abhi disabled hai.", show_alert=True)
            return
        rows = _leaderboard_rows(10)
        title = str(get_setting("leaderboard_title") or "TOP SPENDERS").strip() or "TOP SPENDERS"
        lines = [f"{emo('leaderboard_title')} <b>{html.escape(title[:80])}</b>", ""]
        if not rows:
            lines.append(f"{emo('leaderboard_empty')} Abhi koi qualifying purchase nahi hai.")
        else:
            rank_icons = [emo('leaderboard_top1'), emo('leaderboard_top2'), emo('leaderboard_top3')]
            for idx, row in enumerate(rows, 1):
                icon = rank_icons[idx-1] if idx <= 3 else emo('leaderboard_row')
                name = html.escape(_leaderboard_name(row[1], row[2])[:80])
                if leaderboard_show_amount():
                    lines.append(f"{icon} <b>#{idx} {name}</b>  {emo('leaderboard_spent')} ₹{int(row[3] or 0)}")
                else:
                    lines.append(f"{icon} <b>#{idx} {name}</b>")
        text = "\n".join(lines)
        mk = InlineKeyboardMarkup().add(
            InlineKeyboardButton("Back", callback_data="back_main", style="primary", icon_custom_emoji_id=btn_emo("back_main"))
        )
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, parse_mode="HTML", reply_markup=mk)
    except Exception as e:
        traceback.print_exc()
        try: bot.answer_callback_query(call.id, "⚠️ Leaderboard load nahi ho paaya. Admin se check karvao.", show_alert=True)
        except Exception: pass

def _show_leaderboard_admin_settings(chat_id,msg_id):
    try:
        enabled_bool=leaderboard_enabled()
        amount_bool=leaderboard_show_amount()
        enabled="ON" if enabled_bool else "OFF"
        amount="ON" if amount_bool else "OFF"
        title=str(get_setting("leaderboard_title") or "TOP SPENDERS").strip() or "TOP SPENDERS"
        mk=InlineKeyboardMarkup(row_width=1)
        mk.add(InlineKeyboardButton(f"🏆 Leaderboard: {enabled}",callback_data="admin_leaderboard_toggle",style="success" if enabled_bool else "danger"))
        mk.add(InlineKeyboardButton(f"💰 Show Spend Amount: {amount}",callback_data="admin_leaderboard_amount_toggle",style="success" if amount_bool else "danger"))
        mk.add(InlineKeyboardButton("✏️ Set Leaderboard Title",callback_data="admin_leaderboard_title",style="success"))
        # Directly open the Leaderboard emoji category; no dependency on another
        # screen, so this button cannot fall into an undefined callback state.
        mk.add(InlineKeyboardButton("Customize Leaderboard Emojis",callback_data="admin_leaderboard_emoji_settings",style="success",icon_custom_emoji_id=btn_emo("admin_emoji_manager")))
        mk.add(InlineKeyboardButton("Main Menu Leaderboard Button Emoji",callback_data="admin_leaderboard_menu_emoji",style="success",icon_custom_emoji_id=btn_emo("btn_link_leaderboard")))
        mk.add(InlineKeyboardButton("◀️ Back",callback_data="admin_cat_settings",style="primary",icon_custom_emoji_id=btn_emo("admin_cat_settings")))
        text=(f"🏆 <b>Leaderboard Settings</b>\n\n"
              f"Status: <b>{html.escape(enabled)}</b>\n"
              f"Title: <b>{html.escape(title)}</b>\n"
              f"Spend amount: <b>{html.escape(amount)}</b>\n\n"
              "Ranking purchase transactions se banta hai. ₹0 spend wale users hide rahenge.\n"
              "Default emojis: 🥇 #1, 🥈 #2, 🥉 #3, 🏅 बाकी ranks.\n"
              "Custom emojis isi screen ke <b>Customize Leaderboard Emojis</b> se set karo.")
        bot.edit_message_text(text,chat_id,msg_id,parse_mode="HTML",reply_markup=mk)
    except Exception as e:
        print(f"Leaderboard settings error: {e}")
        traceback.print_exc()
        try: bot.send_message(ADMIN_USER_ID,f"⚠️ Leaderboard Settings error: <code>{html.escape(str(e)[:500])}</code>",parse_mode="HTML")
        except: pass

def save_leaderboard_title(message):
    if _check_cancel(message): return
    title=(message.text or "").strip()
    if not title:
        bot.reply_to(message,"❌ Empty title. Dobara bhejo:")
        bot.register_next_step_handler(message, save_leaderboard_title)
        return
    set_setting("leaderboard_title", title[:80])
    user_states.pop(message.from_user.id,None)
    bot.reply_to(message,"✅ Leaderboard title updated!")

# ======================= PROFILE & HISTORY =======================
def show_profile(call, user_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT username, balance, joined_at, banned, is_reseller, reseller_banned, first_name, referral_earnings FROM users WHERE id=?", (user_id,))
    user = cursor.fetchone()
    cursor.execute("SELECT COUNT(*), COALESCE(SUM(amount),0) FROM transactions WHERE user_id=? AND type='purchase'", (user_id,))
    order_count, spent = cursor.fetchone()
    cursor.close()
    conn.close()
    if user:
        status = f"{emo('profile_status_banned')} Banned" if user[3] else f"{emo('profile_status_active')} Active"
        account_type = f"{emo('profile_role_admin')} Admin" if user_id == ADMIN_USER_ID else f"{emo('profile_role_regular')} Regular"
        display_name = user[6] or call.from_user.first_name or "N/A"
        phone = "N/A"
        try:
            conn2=get_db(); cursor2=conn2.cursor()
            cursor2.execute("SELECT phone_number FROM users WHERE id=?",(user_id,))
            phone_row=cursor2.fetchone()
            cursor2.close(); conn2.close()
            phone=(phone_row[0] if phone_row and phone_row[0] else "N/A")
        except Exception:
            pass
        reseller_line = ""
        if user[4]:
            reseller_line = f"🏷️ Reseller Status: {emo('profile_reseller_banned') + ' Banned' if user[5] else emo('profile_reseller_active') + ' Active'}\n"
        joined=str(user[2] or "N/A")[:10]
        text = (
            f"{emo('profile_title_l')} ━━ YOUR PROFILE ━━ {emo('profile_title_r')}\n\n"
            f"{emo('profile_id')} User ID: {user_id}\n"
            f"{emo('profile_name')} Name: {html.escape(str(display_name))}\n"
            f"{emo('profile_phone')} Phone: {html.escape(str(phone))}\n"
            f"{emo('profile_account')} Account: {account_type} | {status}\n"
            f"{reseller_line}"
            f"\n{emo('profile_bal_l')} ━ Balance ━\n"
            f"{emo('profile_wallet')} Current: ₹{user[1]}\n\n"
            f"{emo('profile_stats')} ━ Statistics ━\n"
            f"{emo('profile_orders')} Orders: {order_count}\n"
            f"{emo('profile_spent')} Spent: ₹{spent}\n\n"
            f"{emo('profile_joined')} Joined: {html.escape(joined)}\n🎁 Referral Earned: ₹{user[7] or 0}"
        )
    else:
        text = "Profile not found."
    bot.edit_message_text(text, call.message.chat.id, call.message.message_id, parse_mode="HTML", reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton(" Back", callback_data="back_main", style="primary", icon_custom_emoji_id=btn_emo("back_main"))))
    try: bot.answer_callback_query(call.id)
    except: pass

def _compute_expiry_label(purchase_dt_str, plan_row):
    """Return expiry in India/Aligarh local time. SQLite CURRENT_TIMESTAMP is UTC."""
    if not plan_row or not purchase_dt_str:
        return "N/A"
    days, duration_unit = plan_row
    try:
        raw = str(purchase_dt_str).strip()
        purchase_dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if purchase_dt.tzinfo is None:
            purchase_dt = purchase_dt.replace(tzinfo=timezone.utc)
    except Exception:
        return "N/A"
    unit = (duration_unit or "days").strip().lower()
    if unit in ("credit", "credits", "token", "tokens"):
        return "No time expiry"
    seconds = (days or 0) * (3600 if unit == "hours" else 86400)
    if seconds <= 0:
        return "N/A"
    return format_ist_datetime(purchase_dt + timedelta(seconds=seconds), include_seconds=False)

def _order_category_name(product_name):
    """Stable human-friendly category used only for My Orders grouping.
    If a future products.category column is added, this helper can be replaced
    without changing the order-history UI. Current bot derives useful groups
    from product names so existing DBs work immediately.
    """
    raw = strip_custom_emoji(product_name or "", None)
    text = " ".join(raw.replace("/", " ").replace("-", " ").split()).strip()
    up = text.upper()
    if up.startswith("BALA"):
        return "Bala Mode"
    if up.startswith("DRIP"):
        return "Drip"
    if up.startswith("HG CHEATS"):
        return "HG Cheats"
    if up.startswith("XYZ CHEATS"):
        return "XYZ Cheats"
    if up.startswith("XREG"):
        return "XREG"
    if up.startswith("NINE X"):
        return "Nine X"
    if up.startswith("AIM HACK"):
        return "Aim Hack"
    if up.startswith("ABCD PANNEL"):
        return "ABCD Pannel"
    return text or "Other"

def _get_user_purchase_rows(user_id):
    conn=get_db(); cur=conn.cursor()
    cur.execute("""SELECT lk.id,p.id,p.name,lk.license_key,lk.used_at,pl.days,pl.duration_unit,pl.price,COALESCE(p.premium_emoji_id,'')
                   FROM license_keys lk JOIN products p ON p.id=lk.product_id
                   LEFT JOIN plans pl ON pl.id=lk.plan_id WHERE lk.used_by=?""",(user_id,))
    rows=list(cur.fetchall())
    cur.execute("""SELECT mo.id,p.id,p.name,mo.license_key,mo.created_at,pl.days,pl.duration_unit,mo.price,COALESCE(p.premium_emoji_id,'')
                   FROM manual_orders mo JOIN products p ON p.id=mo.product_id
                   LEFT JOIN plans pl ON pl.id=mo.plan_id
                   WHERE mo.user_id=? AND mo.status='completed' AND mo.license_key IS NOT NULL""",(user_id,))
    rows += list(cur.fetchall())
    cur.close(); conn.close()
    rows.sort(key=lambda r:r[3] or "", reverse=True)
    return rows

def show_history(call, user_id):
    """My Orders landing page: product-wise buttons only.
    Each button opens the exact product's purchases. No extra box emoji is
    printed because Telegram already renders the product's custom emoji as the
    button icon."""
    try:
        rows=_get_user_purchase_rows(user_id)
        if not rows:
            text=f"{emo('hist_title')} <b>MY ORDERS</b>\n\n📭 No purchase history yet."
            mk=InlineKeyboardMarkup().add(InlineKeyboardButton(" Back",callback_data="back_main",style="primary",icon_custom_emoji_id=btn_emo("back_main")))
            bot.edit_message_text(text,call.message.chat.id,call.message.message_id,parse_mode="HTML",reply_markup=mk)
            bot.answer_callback_query(call.id)
            return

        # Exact product grouping, not name-prefix grouping. This prevents two
        # different products with similar names from being mixed together.
        groups={}
        for r in rows:
            prod_id=int(r[1]); prod_name=r[2]; price=int(r[7] or 0)
            g=groups.setdefault(prod_id,{'name':prod_name,'count':0,'spent':0,'emoji':None})
            g['count']+=1; g['spent']+=price
            if not g['emoji']:
                g['emoji']=get_product_emoji(r[8] if len(r)>8 else None)

        total_spent=sum(int(r[7] or 0) for r in rows)
        text=(f"{emo('hist_title')} <b>MY ORDERS</b>\n\n"
              f"🛒 Total Purchases: <b>{len(rows)}</b>\n"
              f"💰 Total Spent: <b>₹{total_spent}</b>\n\n"
              f"👇 <b>Product select karo</b>")
        markup=InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton(" All Orders",callback_data="orderprod_all",style="success",icon_custom_emoji_id=btn_emo("history")))
        for prod_id,g in sorted(groups.items(), key=lambda kv:(-kv[1]['count'], kv[1]['name'].lower())):
            icon=g.get('emoji') or DEFAULT_BUTTON_EMOJI_ID
            # IMPORTANT: no hardcoded 📦 in text; custom product emoji is the button icon.
            markup.add(InlineKeyboardButton(
                f"{strip_custom_emoji(g['name'], None)} • {g['count']} Purchases • ₹{g['spent']}",
                callback_data=f"orderprod_{prod_id}",style="success",icon_custom_emoji_id=icon))
        markup.add(InlineKeyboardButton(" Back",callback_data="back_main",style="primary",icon_custom_emoji_id=btn_emo("back_main")))
        try:
            bot.edit_message_text(text,call.message.chat.id,call.message.message_id,parse_mode="HTML",reply_markup=markup)
        except Exception as edit_err:
            # Telegram 400: message is not modified means the requested screen
            # already has exactly the same text/buttons. It is harmless.
            if "message is not modified" not in str(edit_err).lower():
                raise
        bot.answer_callback_query(call.id)
    except Exception as e:
        print(f"My Orders error: {e}")
        traceback.print_exc()
        try: bot.answer_callback_query(call.id,"⚠️ My Orders load nahi ho paya. Dobara try karo.",show_alert=True)
        except: pass

def show_order_category(call, user_id, anchor):
    rows=_get_user_purchase_rows(user_id)
    if anchor == "all":
        selected=rows; title="All Orders"
    else:
        try: prod_id=int(anchor)
        except Exception: prod_id=0
        selected=[r for r in rows if int(r[1])==prod_id]
        if not selected:
            bot.answer_callback_query(call.id,"⚠️ Product orders nahi mile.",show_alert=True); return
        title=selected[0][2]

    out=[f"🛒 <b>{html.escape(strip_custom_emoji(title, None))}</b>",
         f"Total Purchases: <b>{len(selected)}</b>",
         f"Total Spent: <b>₹{sum(int(r[7] or 0) for r in selected)}</b>",""]
    for n,r in enumerate(selected,1):
        purchased=format_ist_datetime(r[4])
        expiry=_compute_expiry_label(r[4],(r[5],r[6]) if r[5] is not None else None)
        out.append(
            f"#{n} <b>{html.escape(strip_custom_emoji(r[2], None))}</b>\n"
            f"💰 Price: ₹{r[7] or 0}\n"
            f"🔑 Key: <code>{html.escape(r[3] or 'N/A')}</code>\n"
            f"📅 Purchased: <b>{purchased}</b>\n"
            f"⏳ Expires: <b>{expiry}</b>\n────────────")
    # Keep HTML tags intact when order history is long.  Cutting the raw HTML
    # string at character 3900 can leave <code>/<b> tags unclosed and make
    # Telegram reject the whole message with a 400 parse-entities error.
    if sum(len(x) + 1 for x in out) > 3900:
        safe=[]
        used=0
        for line in out:
            extra=len(line) + 1
            if used + extra > 3700:
                break
            safe.append(line)
            used += extra
        safe.append("… Older orders trimmed.")
        text="\n".join(safe)
    else:
        text="\n".join(out)
    mk=InlineKeyboardMarkup(row_width=1)
    mk.add(InlineKeyboardButton(" Back to Categories",callback_data="history",style="primary",icon_custom_emoji_id=btn_emo("history")))
    mk.add(InlineKeyboardButton(" Back to Menu",callback_data="back_main",style="primary",icon_custom_emoji_id=btn_emo("back_main")))
    bot.edit_message_text(text,call.message.chat.id,call.message.message_id,parse_mode="HTML",reply_markup=mk)
    try: bot.answer_callback_query(call.id)
    except: pass

# ======================= ADD MONEY =======================
def build_amount_keypad(digits):
    markup = InlineKeyboardMarkup(row_width=3)
    for row in (["1", "2", "3"], ["4", "5", "6"], ["7", "8", "9"]):
        markup.row(*[
            InlineKeyboardButton(d, callback_data=f"amtdigit_{d}", style="success", icon_custom_emoji_id=btn_emo("amount_digit"))
            for d in row
        ])
    markup.row(
        InlineKeyboardButton(" Delete", callback_data="amtdelete", style="danger", icon_custom_emoji_id=btn_emo("amount_delete")),
        InlineKeyboardButton("0", callback_data="amtdigit_0", style="success", icon_custom_emoji_id=btn_emo("amount_digit")),
        InlineKeyboardButton(" Confirm", callback_data="amtconfirm", style="success", icon_custom_emoji_id=btn_emo("amount_confirm")),
    )
    markup.row(InlineKeyboardButton(" Back", callback_data="amtback", style="primary", icon_custom_emoji_id=btn_emo("amount_back")))
    return markup

def amount_keypad_text(digits, gateway="zapupi"):
    amount = digits if digits else "0"
    if gateway == "fampay":
        gateway_label = f"{emo('addbal_method_fampay')} FamPay UPI"
    else:
        gateway_label = f"{emo('addbal_method_upi')} UPI (ZapUPI)"
    return (f"{emo('addbal_title')} <b>Custom Amount</b>\n\n"
            f"───────────\n"
            f"Method: <b>{gateway_label}</b>\n"
            f"{emo('addbal_limit')} Min: ₹10 | Max: ₹5,000\n"
            f"───────────\n\n"
            f"{emo('addbal_arrow')} Amount: <b>₹{amount}{usd_hint(amount)}</b>\n\n"
            f"{emo('addbal_arrow')} Amount type karo — buttons se:")

def show_add_balance_gateway_choice(call):
    """First screen after tapping ➕ Add Balance. Shows only whichever payment
    method(s) the admin has switched ON (Payment Gateway Settings): if just one
    is ON, skips straight to its amount keypad like before; if both are ON, shows
    a choice screen; if both are OFF, tells the user Add Balance is paused."""
    zap_on = is_zapupi_enabled()
    fam_on = is_fampay_enabled()
    if zap_on and fam_on:
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(
            InlineKeyboardButton(" UPI (ZapUPI)", callback_data="gateway_upi", style="success", icon_custom_emoji_id=btn_emo("addbal_choice_zapupi")),
            InlineKeyboardButton(" FamPay UPI", callback_data="gateway_fampay", style="success", icon_custom_emoji_id=btn_emo("addbal_choice_fampay")),
        )
        markup.add(InlineKeyboardButton(" Back", callback_data="back_main", style="primary", icon_custom_emoji_id=btn_emo("back_main")))
        text = (
            f"{emo('addbal_choice_title')} <b>ADD BALANCE</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"{emo('addbal_choice_upi_icon')} <b>UPI Pay</b> → Instant &amp; Super Fast\n\n"
            f"{emo('addbal_choice_fampay_icon')} <b>FamPay</b> → Fast Deposit\n"
            f"━━━━━━━━━━━━━━━\n\n"
            f"{emo('addbal_choice_arrow')} Payment Method Chuno {emo('addbal_choice_footer')}"
        )
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, parse_mode="HTML", reply_markup=markup)
    elif zap_on:
        ask_amount(call, gateway="zapupi")
    elif fam_on:
        ask_amount(call, gateway="fampay")
    else:
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton(" Back", callback_data="back_main", style="primary", icon_custom_emoji_id=btn_emo("back_main")))
        bot.edit_message_text(f"{emo('addbal_title')} ⚠️ <b>Add Balance abhi available nahi hai.</b>\nAdmin se contact karo.", call.message.chat.id, call.message.message_id, parse_mode="HTML", reply_markup=markup)

def ask_amount(call, gateway="zapupi"):
    user_id = call.from_user.id
    user_states[user_id] = {"action": "custom_amount", "digits": "", "gateway": gateway}
    bot.edit_message_text(amount_keypad_text("", gateway), call.message.chat.id, call.message.message_id,
                           parse_mode="HTML", reply_markup=build_amount_keypad(""))

def process_deposit_amount(chat_id, user_id, amount, gateway="zapupi"):
    order_id = generate_order_id()
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO pending_payments (user_id, order_id, amount, status, created_at, gateway) VALUES (?,?,?,'pending',?,?)",
                   (user_id, order_id, amount, int(time.time()), gateway))
    conn.commit()
    pay_id = cursor.lastrowid
    cursor.close()
    conn.close()

    is_fampay = (gateway == "fampay")
    gateway_name = "FamPay" if is_fampay else "ZapUPI"
    if is_fampay:
        ok, result, ref_id = create_fampay_order(order_id, amount, user_id)
    else:
        ok, result, ref_id = create_zapupi_order(order_id, amount, user_id)

    if not ok:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("UPDATE pending_payments SET status='failed' WHERE id=?", (pay_id,))
        conn.commit()
        cursor.close()
        conn.close()
        bot.send_message(chat_id, "❌ Payment link generate nahi ho paya. Thodi der baad try karo ya support se contact karo.")
        try:
            bot.send_message(ADMIN_USER_ID, f"⚠️ {gateway_name} create-order failed (order {order_id}): {html.escape(str(result))}", parse_mode="HTML")
        except:
            pass
        return

    conn = get_db()
    cursor = conn.cursor()
    if is_fampay:
        cursor.execute("UPDATE pending_payments SET fampay_order_id=? WHERE id=?", (ref_id, pay_id))
    else:
        cursor.execute("UPDATE pending_payments SET zapupi_txn_id=? WHERE id=?", (ref_id, pay_id))
    conn.commit()
    cursor.close()
    conn.close()

    pay_markup = InlineKeyboardMarkup(row_width=1)

    if is_fampay:
        qr_url = result.get("qr_url")
        upi_id = result.get("upi_id", get_fampay_upi_id())
        caption = (f"{emo('qr_pay')} <b>Pay ₹{amount}</b>\n\n"
                   f"{emo('qr_order')} Order ID: <code>{order_id}</code>\n"
                   f"🏦 UPI ID: <code>{upi_id}</code>\n\n"
                   f"{emo('qr_step1')} QR code scan karo ya UPI ID pe bhejo\n"
                   f"{emo('qr_step2')} Exact amount ₹{amount} bhejo\n"
                   f"{emo('qr_step3')} Payment hote hi <b>🔄 Check Status</b> dabao\n"
                   f"{emo('qr_step4')} Balance <b>automatically</b> add ho jaayega\n\n"
                   f"{emo('qr_warn')} <b>Yeh QR sirf 15 minute tak valid hai.</b>\n"
                   f"{emo('qr_warn')} <b>This QR is valid for only 15 minutes.</b>")
        photo_markup = InlineKeyboardMarkup(row_width=1)
        photo_markup.add(
            InlineKeyboardButton(" Check Status", callback_data=f"zapcheck_{pay_id}", style="success", icon_custom_emoji_id=btn_emo("zapcheck")),
            InlineKeyboardButton(" Cancel Payment", callback_data=f"paycancel_{pay_id}", style="danger", icon_custom_emoji_id=btn_emo("paycancel")),
        )
        try:
            sent = bot.send_photo(chat_id, qr_url, caption=caption, parse_mode="HTML", reply_markup=photo_markup)
        except Exception:
            # Telegram couldn't fetch/embed the QR image directly -- fall back to
            # a plain message with a link to it, so the flow never breaks.
            fallback_markup = InlineKeyboardMarkup(row_width=1)
            fallback_markup.add(InlineKeyboardButton(" View QR Code", url=qr_url, style="primary", icon_custom_emoji_id=btn_emo("link_pay_now")))
            fallback_markup.add(
                InlineKeyboardButton(" Check Status", callback_data=f"zapcheck_{pay_id}", style="success", icon_custom_emoji_id=btn_emo("zapcheck")),
                InlineKeyboardButton(" Cancel Payment", callback_data=f"paycancel_{pay_id}", style="danger", icon_custom_emoji_id=btn_emo("paycancel")),
            )
            bot.send_message(chat_id, caption, parse_mode="HTML", reply_markup=fallback_markup)
        threading.Thread(target=send_payment_reminders, args=(user_id, pay_id), daemon=True).start()
        return

    payment_url = result
    caption = (f"{emo('qr_pay')} <b>Pay ₹{amount}</b>\n\n"
               f"{emo('qr_order')} Order ID: <code>{order_id}</code>\n\n"
               f"{emo('qr_step1')} Neeche <b>💳 Pay Now</b> button dabao\n"
               f"{emo('qr_step2')} Apne kisi bhi UPI app se payment complete karo\n"
               f"{emo('qr_step3')} Payment hote hi balance <b>automatically</b> add ho jaayega — koi screenshot ya wait nahi karna\n\n"
               f"{emo('qr_warn')} <b>Yeh payment link sirf 15 minute tak valid hai.</b>\n"
               f"{emo('qr_warn')} <b>This payment link is valid for only 15 minutes.</b>")

    pay_markup = InlineKeyboardMarkup(row_width=1)
    pay_markup.add(InlineKeyboardButton(" Pay Now", url=payment_url, style="success", icon_custom_emoji_id=btn_emo("link_pay_now")))
    pay_markup.add(
        InlineKeyboardButton(" Check Status", callback_data=f"zapcheck_{pay_id}", style="success", icon_custom_emoji_id=btn_emo("zapcheck")),
        InlineKeyboardButton(" Cancel Payment", callback_data=f"paycancel_{pay_id}", style="danger", icon_custom_emoji_id=btn_emo("paycancel")),
    )

    bot.send_message(chat_id, caption, parse_mode="HTML", reply_markup=pay_markup)
    threading.Thread(target=send_payment_reminders, args=(user_id, pay_id), daemon=True).start()

def send_payment_reminders(user_id, pay_id):
    """Send reminder nudges at 5 min and 10 min if payment is still pending
    (the ZapUPI payment link stays valid for 15 minutes)."""
    try:
        time.sleep(300)  # 5 minutes
        if get_pending_payment_status(pay_id) != "pending":
            return
        bot.send_message(user_id,
            "⏰ <b>Reminder:</b> Aapka payment link jaldi expire hone wala hai!\n"
            "⏰ <b>Reminder:</b> Your payment link is about to expire soon!",
            parse_mode="HTML")

        time.sleep(300)  # 5 more minutes (total 10 min)
        if get_pending_payment_status(pay_id) != "pending":
            return
        bot.send_message(user_id,
            "🚨 <b>Jaldi karein! Payment turant complete karein, link expire hone wala hai.</b>\n"
            "🚨 <b>Hurry! Please complete your payment now, the link is about to expire.</b>",
            parse_mode="HTML")
    except Exception as e:
        print(f"Reminder thread error: {e}")

def get_pending_payment_status(pay_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT status FROM pending_payments WHERE id=?", (pay_id,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row[0] if row else None

def approve_payment(call, pay_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, amount, status, qr_chat_id, qr_message_id FROM pending_payments WHERE id=?", (pay_id,))
    row = cursor.fetchone()
    if not row:
        cursor.close()
        conn.close()
        bot.answer_callback_query(call.id, "❌ Payment record not found.", show_alert=True)
        return
    uid, amount, status, qr_chat_id, qr_message_id = row[0], row[1], row[2], row[3], row[4]
    if status == "success":
        cursor.close()
        conn.close()
        bot.answer_callback_query(call.id, "⚠️ Already approved.", show_alert=True)
        return
    cursor.execute("SELECT balance FROM users WHERE id=?", (uid,))
    urow = cursor.fetchone()
    new_bal = (urow[0] if urow else 0) + amount
    if not urow:
        cursor.execute("INSERT INTO users (id, balance) VALUES (?,?)", (uid, amount))
    else:
        cursor.execute("UPDATE users SET balance=? WHERE id=?", (new_bal, uid))
    cursor.execute("INSERT INTO transactions (user_id, amount, type) VALUES (?,?,'add_money')", (uid, amount))
    cursor.execute("UPDATE pending_payments SET status='success' WHERE id=?", (pay_id,))
    conn.commit()
    cursor.close()
    conn.close()
    # Delete the original QR code message from the user's chat now that it's approved
    if qr_chat_id and qr_message_id:
        try:
            bot.delete_message(qr_chat_id, qr_message_id)
        except:
            pass
    try:
        bot.edit_message_caption(f"✅ <b>APPROVED</b>\n\n👤 User: {uid}\n💰 Amount: ₹{amount}", call.message.chat.id, call.message.message_id, parse_mode="HTML")
    except:
        bot.answer_callback_query(call.id, "✅ Approved")
    # Detailed "NEW DEPOSIT!" notification to admin, JITU-style
    try:
        chat_info = bot.get_chat(uid)
        dep_name = html.escape(chat_info.first_name or "N/A")
        dep_username = f"@{chat_info.username}" if chat_info.username else "N/A"
    except:
        dep_name, dep_username = "N/A", "N/A"
    time_str = now_ist().strftime("%d-%m-%Y %I:%M %p")
    deposit_notify = (
        f"{emo('deposit_title')} <b>NEW DEPOSIT!</b>\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"{emo('deposit_name')} {dep_name}\n"
        f"{emo('deposit_userid')} {uid}\n"
        f"{emo('deposit_phone')} N/A\n"
        f"{emo('deposit_username')} {html.escape(dep_username)}\n"
        f"{emo('deposit_amount')} ₹{amount}\n"
        f"{emo('deposit_bonus')} Bonus: ₹0\n"
        f"{emo('deposit_balance')} Current Balance: ₹{new_bal}\n"
        f"{emo('deposit_time')} {time_str}\n"
        f"━━━━━━━━━━━━━━━━━"
    )
    try:
        bot.send_message(ADMIN_USER_ID, deposit_notify, parse_mode="HTML")
    except:
        pass
    try:
        bot.send_message(uid, deposit_notify, parse_mode="HTML")
    except:
        pass

def reject_payment(call, pay_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, amount, status FROM pending_payments WHERE id=?", (pay_id,))
    row = cursor.fetchone()
    if not row:
        cursor.close()
        conn.close()
        bot.answer_callback_query(call.id, "❌ Payment record not found.", show_alert=True)
        return
    uid, amount, status = row[0], row[1], row[2]
    if status == "success":
        cursor.close()
        conn.close()
        bot.answer_callback_query(call.id, "⚠️ Already approved, cannot reject.", show_alert=True)
        return
    cursor.execute("UPDATE pending_payments SET status='failed' WHERE id=?", (pay_id,))
    conn.commit()
    cursor.close()
    conn.close()
    try:
        bot.send_message(uid, f"❌ Your payment of ₹{amount} was rejected by admin. If you did pay, contact support with your screenshot.")
    except:
        pass
    try:
        bot.edit_message_caption(f"❌ <b>REJECTED</b>\n\n👤 User: {uid}\n💰 Amount: ₹{amount}", call.message.chat.id, call.message.message_id, parse_mode="HTML")
    except:
        bot.answer_callback_query(call.id, "❌ Rejected")

# ======================= ADMIN FUNCTIONS =======================
DELETE_PLAN_PAGE_SIZE = 10

def show_delete_plan_page(chat_id, msg_id, page=0):
    """Render a small/paginated Delete Plan keyboard.

    Telegram rejected the old screen with HTTP 400 "reply markup is too long"
    when a shop had many plans because every plan became a button in one
    keyboard. Keeping only 10 plan buttons per page keeps the markup safely
    below Telegram's limit while preserving access to every plan.
    """
    try:
        page = max(0, int(page))
    except (TypeError, ValueError):
        page = 0

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT p.name, pl.id, pl.days, pl.price, pl.duration_unit "
        "FROM plans pl JOIN products p ON pl.product_id=p.id "
        "ORDER BY p.sort_order, p.id, "
        "(CASE WHEN pl.duration_unit='hours' THEN pl.days ELSE pl.days*24 END), pl.id"
    )
    plans = cursor.fetchall()
    cursor.close()
    conn.close()

    if not plans:
        bot.edit_message_text(
            "No plans.", chat_id, msg_id,
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton("◀️ Cancel", callback_data="admin_cat_product",
                                     style="danger", icon_custom_emoji_id=btn_emo("admin_cat_product"))
            )
        )
        return

    total_pages = (len(plans) + DELETE_PLAN_PAGE_SIZE - 1) // DELETE_PLAN_PAGE_SIZE
    page = min(page, total_pages - 1)
    start = page * DELETE_PLAN_PAGE_SIZE
    page_rows = plans[start:start + DELETE_PLAN_PAGE_SIZE]

    markup = InlineKeyboardMarkup(row_width=1)
    for pl in page_rows:
        # Product names can be arbitrarily long. Keep button labels short so
        # even a single unusually long product name cannot bloat the markup.
        product_name = str(pl[0] or "Product")
        if len(product_name) > 36:
            product_name = product_name[:36] + "…"
        duration = format_duration(pl[2], pl[4])
        label = f"🗑️ {product_name} - {duration} ₹{pl[3]}"
        markup.add(InlineKeyboardButton(
            label, callback_data=f"delplan_{pl[1]}", style="danger",
            icon_custom_emoji_id=btn_emo("delplan")
        ))

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(
            "⬅️ Prev", callback_data=f"delplan_page_{page-1}",
            style="primary", icon_custom_emoji_id=btn_emo("admin_all_users")
        ))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(
            "Next ➡️", callback_data=f"delplan_page_{page+1}",
            style="primary", icon_custom_emoji_id=btn_emo("admin_all_users")
        ))
    if nav:
        markup.row(*nav)

    markup.add(InlineKeyboardButton(
        "❌ Cancel", callback_data="admin_cat_product", style="danger",
        icon_custom_emoji_id=btn_emo("admin_cat_product")
    ))
    bot.edit_message_text(
        f"🗑️ <b>Delete Plan</b>\n\nSelect a plan — Page {page + 1}/{total_pages}",
        chat_id, msg_id, parse_mode="HTML", reply_markup=markup
    )

def handle_admin_actions(call, data):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    msg_id = call.message.message_id
    if data == "admin_cat_product":
        bot.edit_message_text("📦 <b>Product Management</b>\n\nKya karna hai?", chat_id, msg_id, parse_mode="HTML", reply_markup=get_product_mgmt_menu())
        return
    if data == "admin_maintenance_list":
        show_maintenance_toggle_list(chat_id, msg_id)
        return
    if data == "admin_cat_keys":
        bot.edit_message_text("🔑 <b>Keys Management</b>\n\nKya karna hai?", chat_id, msg_id, parse_mode="HTML", reply_markup=get_keys_mgmt_menu())
        return
    if data == "admin_cat_user":
        bot.edit_message_text("👤 <b>User Management</b>\n\nKya karna hai?", chat_id, msg_id, parse_mode="HTML", reply_markup=get_user_mgmt_menu())
        return
    if data == "admin_cat_reseller":
        bot.edit_message_text("🏷️ <b>Reseller Management</b>\n\nKya karna hai?", chat_id, msg_id, parse_mode="HTML", reply_markup=get_reseller_mgmt_menu())
        return
    if data == "admin_reseller_toggle":
        bot.edit_message_text(
            "🔁 <b>Reseller ON/OFF</b>\n\n"
            "Jis user ko reseller banana/hatana hai uska numeric Telegram ID bhejo.\n\n"
            "• Agar wo abhi reseller NAHI hai → reseller BAN JAAYEGA.\n"
            "• Agar wo abhi reseller HAI → reseller status HAT JAAYEGA.",
            chat_id, msg_id, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Cancel", callback_data="admin_cat_reseller", style="danger", icon_custom_emoji_id=btn_emo("admin_cat_reseller")))
        )
        bot.register_next_step_handler(call.message, reseller_toggle_admin)
        return
    if data == "admin_reseller_setprice":
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, name_entities FROM products ORDER BY sort_order, id")
        prods = cursor.fetchall()
        cursor.close()
        conn.close()
        if not prods:
            bot.edit_message_text("No products yet. Add a product first.", chat_id, msg_id, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton(" Back", callback_data="admin_cat_reseller", style="primary", icon_custom_emoji_id=btn_emo("admin_cat_reseller"))))
            return
        markup = InlineKeyboardMarkup()
        for p in prods:
            markup.add(InlineKeyboardButton(f" {strip_custom_emoji(p[1], p[2])}", callback_data=f"resellerprice_prod_{p[0]}", style="success", icon_custom_emoji_id=btn_emo("resellerprice_prod")))
        markup.add(InlineKeyboardButton(" Cancel", callback_data="admin_cat_reseller", style="danger", icon_custom_emoji_id=btn_emo("admin_cat_reseller")))
        bot.edit_message_text("💰 Kis product ke plans ke liye reseller price set karni hai?", chat_id, msg_id, reply_markup=markup)
        return
    if data == "admin_reseller_list":
        show_reseller_list(chat_id, msg_id)
        return
    if data == "admin_reseller_ban":
        bot.edit_message_text(
            "🚫 <b>Ban Reseller</b>\n\nBan karne ke liye reseller ka numeric Telegram ID bhejo. "
            "Iske baad usko normal price dikhega, uska account/balance active rahega.",
            chat_id, msg_id, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Cancel", callback_data="admin_cat_reseller", style="danger", icon_custom_emoji_id=btn_emo("admin_cat_reseller")))
        )
        bot.register_next_step_handler(call.message, reseller_ban_admin)
        return
    if data == "admin_reseller_unban":
        bot.edit_message_text(
            "✅ <b>Unban Reseller</b>\n\nUnban karne ke liye reseller ka numeric Telegram ID bhejo.",
            chat_id, msg_id, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Cancel", callback_data="admin_cat_reseller", style="danger", icon_custom_emoji_id=btn_emo("admin_cat_reseller")))
        )
        bot.register_next_step_handler(call.message, reseller_unban_admin)
        return
    if data == "admin_reseller_buy_menu":
        show_reseller_buy_menu(chat_id, msg_id)
        return
    if data == "admin_reseller_buy_toggle":
        cur_enabled = get_setting("reseller_buy_enabled") == "1"
        price_set = get_setting("reseller_buy_price")
        if not cur_enabled and not price_set:
            bot.answer_callback_query(call.id, "⚠️ Pehle reseller price set karo.", show_alert=True)
        else:
            set_setting("reseller_buy_enabled", "0" if cur_enabled else "1")
        show_reseller_buy_menu(chat_id, msg_id)
        return
    if data == "admin_reseller_buy_setprice":
        bot.edit_message_text(
            "💰 Reseller banne ke liye price bhejo (jaise 400):",
            chat_id, msg_id,
            reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Cancel", callback_data="admin_reseller_buy_menu", style="danger", icon_custom_emoji_id=btn_emo("admin_reseller_buy_menu")))
        )
        bot.register_next_step_handler(call.message, save_reseller_buy_price)
        return
    if data == "admin_reseller_buy_setterms":
        bot.edit_message_text(
            "📝 Conditions/description text bhejo jo Buy button ke upar dikhega (HTML allowed, jaise <b>bold</b>):",
            chat_id, msg_id,
            reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Cancel", callback_data="admin_reseller_buy_menu", style="danger", icon_custom_emoji_id=btn_emo("admin_reseller_buy_menu")))
        )
        bot.register_next_step_handler(call.message, save_reseller_buy_terms)
        return
    if data == "admin_set_order_notify":
        current = get_setting("order_notify_template")
        status_line = "✅ Custom template set hai." if current else "⚪ Abhi default text use ho raha hai."
        bot.edit_message_text(
            "📝 <b>Set Order Notify Text</b>\n\n"
            f"{status_line}\n\n"
            "Naya purchase hone par admin + user ko jo 'NEW ORDER!' message jaata hai, uska poora text apni marji se type karo. "
            "Premium emoji bhi daal sakte ho (emoji keyboard se select karo ya forward karo agar Telegram Premium hai) — wo automatically preserve honge.\n\n"
            "Ye placeholders use kar sakte ho, purchase ke time asli values se replace ho jaayenge:\n"
            f"<code>{html.escape(ORDER_NOTIFY_PLACEHOLDERS)}</code>\n\n"
            "Default text par wapas jaane ke liye 'reset' bhejo.",
            chat_id, msg_id, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Cancel", callback_data="admin_cat_settings", style="danger", icon_custom_emoji_id=btn_emo("admin_cat_settings")))
        )
        bot.register_next_step_handler(call.message, save_order_notify_template)
        return
    if data == "admin_set_support_text":
        current = get_setting("support_text_template")
        status_line = "✅ Custom text set hai." if current else "⚪ Abhi default text use ho raha hai."
        bot.edit_message_text(
            "📝 <b>Set Support Text</b>\n\n"
            f"{status_line}\n\n"
            "'📞 Support' button dabane par jo poora message dikhta hai (Orders/Payments/Product questions/etc), uska text apni marji se type karo — HTML formatting allowed (jaise <b>bold</b>, <i>italic</i>).\n\n"
            "Premium emoji bhi daal sakte ho (emoji keyboard se select karo ya forward karo agar Telegram Premium hai) — wo automatically preserve honge.\n\n"
            "Default text par wapas jaane ke liye 'reset' bhejo.",
            chat_id, msg_id, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Cancel", callback_data="admin_cat_settings", style="danger", icon_custom_emoji_id=btn_emo("admin_cat_settings")))
        )
        bot.register_next_step_handler(call.message, save_support_text)
        return
    if data == "admin_set_highlights":
        current = get_setting("store_highlights_template")
        status_line = "✅ Custom text set hai." if current else "⚪ Abhi default text use ho raha hai."
        bot.edit_message_text(
            "📝 <b>Set Store Highlights Text</b>\n\n"
            f"{status_line}\n\n"
            "Main menu (/start) par jo '— STORE HIGHLIGHTS —' box dikhta hai, uska poora text apni marji se type karo — HTML formatting allowed (jaise <b>bold</b>).\n\n"
            "Premium emoji bhi daal sakte ho (emoji keyboard se select karo ya forward karo agar Telegram Premium hai) — wo automatically preserve honge.\n\n"
            "Default text par wapas jaane ke liye 'reset' bhejo.",
            chat_id, msg_id, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Cancel", callback_data="admin_cat_settings", style="danger", icon_custom_emoji_id=btn_emo("admin_cat_settings")))
        )
        bot.register_next_step_handler(call.message, save_store_highlights_text)
        return
    if data == "admin_reseller_buy_setsuccessmsg":
        bot.edit_message_text(
            "🎉 Purchase ke baad 'Reseller Activated' ke niche jo text dikhana hai wo bhejo (jaise DM/contact instructions):",
            chat_id, msg_id,
            reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Cancel", callback_data="admin_reseller_buy_menu", style="danger", icon_custom_emoji_id=btn_emo("admin_reseller_buy_menu")))
        )
        bot.register_next_step_handler(call.message, save_reseller_buy_success_msg)
        return
    if data == "admin_cat_settings":
        bot.edit_message_text("⚙️ <b>Settings</b>\n\nKya karna hai?", chat_id, msg_id, parse_mode="HTML", reply_markup=get_settings_menu())
        return
    if data == "admin_maintenance_toggle":
        new_state = "0" if is_maintenance_on() else "1"
        set_setting("maintenance_mode", new_state)
        if new_state == "1":
            status_text = "🛠 <b>Maintenance mode ON kar diya gaya hai.</b>\n\nAb sirf tum (admin) bot use kar paoge. Baaki sab users ko maintenance message dikhega jab tak tum ise OFF nahi karte."
        else:
            status_text = "✅ <b>Maintenance mode OFF kar diya gaya hai.</b>\n\nAb sab users wapas normally bot use kar paayenge."
        bot.edit_message_text(status_text, chat_id, msg_id, parse_mode="HTML", reply_markup=get_settings_menu())
        return
    if data == "admin_usd_toggle":
        new_state = "0" if is_usd_display_on() else "1"
        set_setting("usd_display_enabled", new_state)
        if new_state == "1":
            rate = get_usd_inr_rate()
            status_text = f"💲 <b>USD Display ON kar diya gaya hai.</b>\n\nAb Add Balance, plans/prices aur balance -- sabme ₹ ke saath live $ equivalent bhi dikhega.\n\n📊 Current rate: $1 ≈ ₹{rate:,.2f}"
        else:
            status_text = "✅ <b>USD Display OFF kar diya gaya hai.</b>\n\nAb sab jagah sirf normal ₹ (INR) dikhega, $ hat gaya hai."
        bot.edit_message_text(status_text, chat_id, msg_id, parse_mode="HTML", reply_markup=get_settings_menu())
        return
    if data == "admin_emoji_manager":
        show_emoji_categories(chat_id, msg_id)
        return
    if data == "admin_external_api_settings":
        bot.edit_message_text(get_external_api_settings_text(), chat_id, msg_id, parse_mode="HTML", reply_markup=get_external_api_settings_menu())
        return
    if data == "admin_set_external_api_url":
        bot.edit_message_text(
            "🌐 Naya API Endpoint URL bhejo (e.g. https://bantibhaiya.to/api/reseller_v1.php):",
            chat_id, msg_id, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Cancel", callback_data="admin_external_api_settings", style="danger", icon_custom_emoji_id=btn_emo("admin_external_api_settings")))
        )
        bot.register_next_step_handler(call.message, save_external_api_url)
        return
    if data == "admin_set_external_api_key":
        bot.edit_message_text(
            "🔑 Nayi API Key bhejo (bantibhaiya.to panel se copy karke paste karo):",
            chat_id, msg_id, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Cancel", callback_data="admin_external_api_settings", style="danger", icon_custom_emoji_id=btn_emo("admin_external_api_settings")))
        )
        bot.register_next_step_handler(call.message, save_external_api_key)
        return
    if data == "admin_set_external_api_masterkey":
        bot.edit_message_text(
            "🔒 Nayi Master Key bhejo (x-master-key header ke liye):",
            chat_id, msg_id, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Cancel", callback_data="admin_external_api_settings", style="danger", icon_custom_emoji_id=btn_emo("admin_external_api_settings")))
        )
        bot.register_next_step_handler(call.message, save_external_api_masterkey)
        return
    if data == "admin_set_keyspanels_url":
        bot.edit_message_text(
            "🟣 KeysPanelShop API Endpoint URL bhejo (default: https://keyspanelshop.shop/reseller_api.php):",
            chat_id, msg_id,
            reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Cancel", callback_data="admin_external_api_settings", style="danger", icon_custom_emoji_id=btn_emo("admin_external_api_settings")))
        )
        bot.register_next_step_handler(call.message, save_keyspanels_url)
        return
    if data == "admin_set_keyspanels_masterkey":
        bot.edit_message_text(
            "🔒 KeysPanelShop ka x-master-key bhejo:",
            chat_id, msg_id,
            reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Cancel", callback_data="admin_external_api_settings", style="danger", icon_custom_emoji_id=btn_emo("admin_external_api_settings")))
        )
        bot.register_next_step_handler(call.message, save_keyspanels_masterkey)
        return
    if data == "admin_product_share_links":
        if user_id != ADMIN_USER_ID:
            bot.answer_callback_query(call.id, "Admin only.", show_alert=True)
            return
        bot.edit_message_text(
            "🔗 <b>Product Share Links</b>\n\nProduct select karo:",
            chat_id, msg_id, parse_mode="HTML",
            reply_markup=get_product_share_links_menu()
        )
        bot.answer_callback_query(call.id)
        return

    m_share = re.match(r"admin_share_product_(\d+)$", str(data))
    if m_share:
        if user_id != ADMIN_USER_ID:
            bot.answer_callback_query(call.id, "Admin only.", show_alert=True)
            return
        pid = int(m_share.group(1))
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM products WHERE id=?", (pid,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if not row:
            bot.answer_callback_query(call.id, "Product nahi mila.", show_alert=True)
            return
        link = make_product_share_link(bot, pid)
        if not link:
            bot.answer_callback_query(call.id, "Bot username nahi mila.", show_alert=True)
            return
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("🔗 Open Product", url=link, style="success"))
        markup.add(InlineKeyboardButton("◀️ Back", callback_data="admin_product_share_links", style="primary"))
        bot.edit_message_text(
            f"🔗 <b>{html.escape(str(row[0]))}</b>\n\n"
            f"📎 <b>Share Link:</b>\n<code>{html.escape(link)}</code>\n\n"
            "Is link ko user ko bhejo — open karte hi isi product ke plans/prices khulenge.",
            chat_id, msg_id, parse_mode="HTML", reply_markup=markup
        )
        bot.answer_callback_query(call.id)
        return

    if data == "admin_payment_gateways":
        bot.edit_message_text(get_payment_gateway_settings_text(), chat_id, msg_id, parse_mode="HTML", reply_markup=get_payment_gateway_settings_menu())
        return
    if data == "admin_zapupi_toggle":
        set_setting("zapupi_enabled", "0" if is_zapupi_enabled() else "1")
        bot.edit_message_text(get_payment_gateway_settings_text(), chat_id, msg_id, parse_mode="HTML", reply_markup=get_payment_gateway_settings_menu())
        return
    if data == "admin_set_zapupi_key":
        bot.edit_message_text(
            "🔑 Naya ZapUPI <b>Zap Key</b> bhejo (pay.zapupi.com dashboard se copy karke):",
            chat_id, msg_id, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Cancel", callback_data="admin_payment_gateways", style="danger", icon_custom_emoji_id=btn_emo("admin_payment_gateways")))
        )
        bot.register_next_step_handler(call.message, save_zapupi_key)
        return
    if data == "admin_fampay_toggle":
        new_state = "0" if is_fampay_enabled() else "1"
        if new_state == "1" and (not get_fampay_api_key() or not get_fampay_upi_id()):
            bot.answer_callback_query(call.id, "⚠️ Pehle FamPay API Key aur UPI ID set karo, tab ON karo.", show_alert=True)
            return
        set_setting("fampay_enabled", new_state)
        bot.edit_message_text(get_payment_gateway_settings_text(), chat_id, msg_id, parse_mode="HTML", reply_markup=get_payment_gateway_settings_menu())
        return
    if data == "admin_set_fampay_key":
        bot.edit_message_text(
            "🔑 Naya <b>FamPay API Key</b> bhejo:",
            chat_id, msg_id, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Cancel", callback_data="admin_payment_gateways", style="danger", icon_custom_emoji_id=btn_emo("admin_payment_gateways")))
        )
        bot.register_next_step_handler(call.message, save_fampay_key)
        return
    if data == "admin_set_fampay_upi":
        bot.edit_message_text(
            "🏦 FamPay ka <b>UPI ID</b> bhejo (jispe payment aayega, e.g. example@okhdfcbank):",
            chat_id, msg_id, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Cancel", callback_data="admin_payment_gateways", style="danger", icon_custom_emoji_id=btn_emo("admin_payment_gateways")))
        )
        bot.register_next_step_handler(call.message, save_fampay_upi)
        return
    if data == "admin_fampay_debug":
        bot.edit_message_text(
            get_fampay_debug_text(), chat_id, msg_id, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton(" Back", callback_data="admin_payment_gateways", style="primary", icon_custom_emoji_id=btn_emo("admin_payment_gateways")))
        )
        return
    if data == "admin_announcement":
        markup = InlineKeyboardMarkup(row_width=2)
        markup.add(
            InlineKeyboardButton("🕐 1 Hour", callback_data="admin_announce_dur_1", style="success", icon_custom_emoji_id=btn_emo("announce_1h")),
            InlineKeyboardButton("🕑 2 Hours", callback_data="admin_announce_dur_2", style="success", icon_custom_emoji_id=btn_emo("announce_2h")),
        )
        markup.add(
            InlineKeyboardButton("🕒 3 Hours", callback_data="admin_announce_dur_3", style="success", icon_custom_emoji_id=btn_emo("announce_3h")),
            InlineKeyboardButton("♾️ Permanent", callback_data="admin_announce_dur_perm", style="success", icon_custom_emoji_id=btn_emo("announce_perm")),
        )
        markup.add(InlineKeyboardButton("❌ Cancel", callback_data="admin_panel", style="danger", icon_custom_emoji_id=btn_emo("admin_panel")))
        bot.edit_message_text(
            "📢 <b>Announcement</b>\n\n"
            "Pehle duration select karo — announcement kitni der baad automatically sabki chat se delete ho jaaye (ya kabhi delete na ho):",
            chat_id, msg_id, parse_mode="HTML", reply_markup=markup
        )
        return
    if data.startswith("admin_announce_dur_"):
        dur_key = data[len("admin_announce_dur_"):]
        duration_map = {"1": 3600, "2": 2*3600, "3": 3*3600, "perm": None}
        duration_seconds = duration_map.get(dur_key)
        duration_label = {"1": "1 ghante", "2": "2 ghante", "3": "3 ghante", "perm": "kabhi nahi (Permanent)"}[dur_key]
        cancel_markup = InlineKeyboardMarkup()
        cancel_markup.add(InlineKeyboardButton("❌ Cancel", callback_data="admin_panel", style="danger", icon_custom_emoji_id=btn_emo("admin_panel")))
        bot.edit_message_text(
            f"📢 <b>Announcement</b> — auto-delete: <b>{duration_label} baad</b>\n\n"
            "Ab jo bhi bhejoge — text, photo, video, voice, audio, ya koi bhi file/document (caption ke saath ya bina), "
            "premium emoji, bold/italic formatting, links — sab kuch <b>as-is preserve</b> hoke <b>sabhi users</b> ko bhej diya jaayega.",
            chat_id, msg_id, parse_mode="HTML", reply_markup=cancel_markup
        )
        bot.register_next_step_handler(call.message, send_announcement, duration_seconds)
        return
    if data == "admin_add_product":
        bot.edit_message_text("📝 Send product name — custom Premium emoji bhi is text ke saath daal sakte ho:", chat_id, msg_id, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Cancel", callback_data="admin_cat_product", style="danger", icon_custom_emoji_id=btn_emo("admin_cat_product"))))
        bot.register_next_step_handler(call.message, add_product)
    elif data == "admin_edit_product":
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, name_entities FROM products ORDER BY sort_order, id")
        prods = cursor.fetchall()
        cursor.close()
        conn.close()
        if not prods:
            bot.edit_message_text("No products.", chat_id, msg_id)
            return
        markup = InlineKeyboardMarkup()
        for p in prods:
            markup.add(InlineKeyboardButton(f"✏️ {strip_custom_emoji(p[1], p[2])}", callback_data=f"editprod_{p[0]}", style="success", icon_custom_emoji_id=btn_emo("editprod")))
        markup.add(InlineKeyboardButton(" Cancel", callback_data="admin_cat_product", style="danger", icon_custom_emoji_id=btn_emo("admin_cat_product")))
        bot.edit_message_text("Edit product:", chat_id, msg_id, reply_markup=markup)
    elif data == "admin_del_product":
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, name_entities FROM products ORDER BY sort_order, id")
        prods = cursor.fetchall()
        cursor.close()
        conn.close()
        if not prods:
            bot.edit_message_text("No products.", chat_id, msg_id)
            return
        markup = InlineKeyboardMarkup()
        for p in prods:
            markup.add(InlineKeyboardButton(f"🗑️ {strip_custom_emoji(p[1], p[2])}", callback_data=f"delprod_{p[0]}", style="danger", icon_custom_emoji_id=btn_emo("delprod")))
        markup.add(InlineKeyboardButton(" Cancel", callback_data="admin_cat_product", style="danger", icon_custom_emoji_id=btn_emo("admin_cat_product")))
        bot.edit_message_text("Delete product:", chat_id, msg_id, reply_markup=markup)
    elif data == "admin_add_plan":
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT id, name FROM products ORDER BY sort_order, id")
        prods = cursor.fetchall()
        cursor.close()
        conn.close()
        if not prods:
            bot.edit_message_text("⚠️ Koi product nahi hai. Pehle 'Add Product' se product banao.", chat_id, msg_id)
            return
        prod_list = "\n".join([f"🆔 {p[0]} — {html.escape(p[1])}" for p in prods])
        bot.edit_message_text(
            f"📦 <b>Tumhare Products:</b>\n{prod_list}\n\n"
            f"📅 Ab send karo: <code>product_id value price [unit]</code>\n"
            f"Unit optional hai — 'days' (default), 'hours', 'credits' ya 'tokens'.\n\n"
            f"Days wala example: <code>{prods[0][0]} 30 499</code>\n"
            f"Hours wala example: <code>{prods[0][0]} 1 60 hours</code>\n"
            f"Credits wala example: <code>{prods[0][0]} 1 70 credits</code>",
            chat_id, msg_id, parse_mode="HTML", reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Cancel", callback_data="admin_cat_product", style="danger", icon_custom_emoji_id=btn_emo("admin_cat_product")))
        )
        bot.register_next_step_handler(call.message, add_plan)
    elif data == "admin_del_plan":
        show_delete_plan_page(chat_id, msg_id, 0)
    elif data == "admin_add_keys":
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, name_entities FROM products ORDER BY sort_order, id")
        prods = cursor.fetchall()
        cursor.close()
        conn.close()
        if not prods:
            bot.edit_message_text("No products.", chat_id, msg_id)
            return
        markup = InlineKeyboardMarkup()
        for p in prods:
            markup.add(InlineKeyboardButton(f"🔑 {strip_custom_emoji(p[1], p[2])}", callback_data=f"addkeys_prod_{p[0]}", style="success", icon_custom_emoji_id=btn_emo("addkeys_prod")))
        markup.add(InlineKeyboardButton(" Cancel", callback_data="admin_cat_keys", style="danger", icon_custom_emoji_id=btn_emo("admin_cat_keys")))
        bot.edit_message_text("Select product:", chat_id, msg_id, reply_markup=markup)
    elif data == "admin_list_keys":
        list_all_keys(call)
    elif data == "admin_expired_keys":
        show_expired_used_keys(chat_id, msg_id)
    elif data == "admin_stock_overview":
        show_stock_overview(chat_id, msg_id)
    elif data.startswith("admin_all_users_"):
        page = int(data.split("_")[-1])
        show_all_users(chat_id, msg_id, page)
    elif data == "admin_del_key":
        bot.edit_message_text("🔑 Send key to delete:", chat_id, msg_id, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Cancel", callback_data="admin_cat_keys", style="danger", icon_custom_emoji_id=btn_emo("admin_cat_keys"))))
        bot.register_next_step_handler(call.message, delete_key)
    elif data == "admin_add_balance":
        bot.edit_message_text("💰 Send: <code>user_id amount</code>", chat_id, msg_id, parse_mode="HTML", reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Cancel", callback_data="admin_cat_user", style="danger", icon_custom_emoji_id=btn_emo("admin_cat_user"))))
        bot.register_next_step_handler(call.message, add_balance_admin)
    elif data == "admin_remove_balance":
        bot.edit_message_text("💰 Send: <code>user_id amount</code>", chat_id, msg_id, parse_mode="HTML", reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Cancel", callback_data="admin_cat_user", style="danger", icon_custom_emoji_id=btn_emo("admin_cat_user"))))
        bot.register_next_step_handler(call.message, remove_balance_admin)
    elif data == "admin_ban_user":
        bot.edit_message_text("🚫 Send user ID to ban:", chat_id, msg_id, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Cancel", callback_data="admin_cat_user", style="danger", icon_custom_emoji_id=btn_emo("admin_cat_user"))))
        bot.register_next_step_handler(call.message, ban_user_cmd)
    elif data == "admin_unban_user":
        bot.edit_message_text("✅ Send user ID to unban:", chat_id, msg_id, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Cancel", callback_data="admin_cat_user", style="danger", icon_custom_emoji_id=btn_emo("admin_cat_user"))))
        bot.register_next_step_handler(call.message, unban_user_cmd)
    elif data == "admin_leaderboard_settings" and call.from_user.id == ADMIN_USER_ID:
        try: bot.answer_callback_query(call.id)
        except Exception: pass
        _show_leaderboard_admin_settings(chat_id, msg_id)
    elif data == "admin_leaderboard_toggle" and user_id == ADMIN_USER_ID:
        set_setting("leaderboard_enabled", "0" if leaderboard_enabled() else "1")
        _show_leaderboard_admin_settings(chat_id, msg_id)
    elif data == "admin_leaderboard_amount_toggle" and user_id == ADMIN_USER_ID:
        set_setting("leaderboard_show_amount", "0" if leaderboard_show_amount() else "1")
        _show_leaderboard_admin_settings(chat_id, msg_id)
    elif data == "admin_leaderboard_menu_emoji" and user_id == ADMIN_USER_ID:
        current=get_setting("emoji_slot_link_leaderboard")
        bot.edit_message_text(f"✨ <b>Main Menu Leaderboard Button Emoji</b>\n\nCurrent: {'custom emoji set' if current else 'default 🏆'}\n\nPremium custom emoji bhejo/forward karo, ya numeric custom_emoji_id paste karo.",chat_id,msg_id,parse_mode="HTML",reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("♻️ Reset",callback_data="emojireset_btn_link_leaderboard",style="success")).add(InlineKeyboardButton("◀️ Back",callback_data="admin_leaderboard_settings",style="primary")))
        bot.register_next_step_handler(call.message,save_emoji_slot,"btn_link_leaderboard")
        return
    elif data == "admin_leaderboard_title" and user_id == ADMIN_USER_ID:
        bot.edit_message_text("🏆 <b>Leaderboard Title</b>\n\nNaya title bhejo. Example: <code>TOP SPENDERS</code>", chat_id, msg_id, parse_mode="HTML", reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("◀️ Cancel", callback_data="admin_leaderboard_settings", style="primary")))
        user_states[ADMIN_USER_ID] = {"action":"set_leaderboard_title"}
        bot.register_next_step_handler(call.message, save_leaderboard_title)
    elif data == "admin_referral_settings":
        enabled="ON" if referral_enabled() else "OFF"
        comm="ON" if referral_commission_enabled() else "OFF"
        mk=InlineKeyboardMarkup(row_width=1)
        mk.add(InlineKeyboardButton(f"🎁 Referral: {enabled}",callback_data="admin_ref_toggle",style="success" if referral_enabled() else "danger"))
        mk.add(InlineKeyboardButton(f"💸 Commission: {comm} ({get_referral_commission_percent()}%)",callback_data="admin_ref_comm_toggle",style="success" if referral_commission_enabled() else "danger"))
        mk.add(InlineKeyboardButton(f"📌 Min Purchase: ₹{get_referral_min_purchase()}",callback_data="admin_ref_min",style="success"))
        mk.add(InlineKeyboardButton("📈 Set Commission %",callback_data="admin_ref_pct",style="success"))
        mk.add(InlineKeyboardButton("◀️ Back",callback_data="admin_cat_settings",style="primary",icon_custom_emoji_id=btn_emo("admin_cat_settings")))
        bot.edit_message_text("🎁 <b>Referral Settings</b>\n\nJoin bonus removed. Referred users ki eligible purchases par commission milega. Buyer ko first completed product purchase par ₹2 bonus milega.",chat_id,msg_id,parse_mode="HTML",reply_markup=mk)
    elif data == "admin_ref_toggle":
        set_setting("referral_enabled","0" if referral_enabled() else "1")
        return _show_referral_admin_settings(chat_id,msg_id)
    elif data == "admin_ref_comm_toggle":
        set_setting("referral_commission_enabled","0" if referral_commission_enabled() else "1")
        return _show_referral_admin_settings(chat_id,msg_id)
    elif data == "admin_ref_pct":
        bot.edit_message_text(f"💸 <b>Purchase Commission</b>\n\nCurrent: {get_referral_commission_percent()}%\n\nSend percentage 0-100, e.g. <code>5</code>",chat_id,msg_id,parse_mode="HTML",reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("◀️ Back",callback_data="admin_referral_settings",style="primary")))
        user_states[ADMIN_USER_ID]={"action":"set_referral_commission_pct"}
        bot.register_next_step_handler(call.message, save_referral_commission_pct)
    elif data == "admin_ref_min":
        bot.edit_message_text(f"📌 <b>Minimum Purchase</b>\n\nCurrent: ₹{get_referral_min_purchase()}\n\nSend minimum purchase amount. 0 = no minimum.",chat_id,msg_id,parse_mode="HTML",reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("◀️ Back",callback_data="admin_referral_settings",style="primary")))
        user_states[ADMIN_USER_ID]={"action":"set_referral_min"}
        bot.register_next_step_handler(call.message, save_referral_min)
    elif data == "admin_spin_settings":
        cfg=", ".join(f"₹{r}={p:.0f}%" for r,p in get_spin_config())
        enabled="ON" if spin_enabled() else "OFF"
        mk=InlineKeyboardMarkup(row_width=1)
        mk.add(InlineKeyboardButton(f"🎡 Daily Spin: {enabled}",callback_data="admin_spin_toggle",style="success" if spin_enabled() else "danger"))
        mk.add(InlineKeyboardButton("🎯 Set Rewards + Probability",callback_data="admin_spin_prob",style="success"))
        mk.add(InlineKeyboardButton("🎨 Set Spin Animation Emojis",callback_data="admin_spin_animation_emojis",style="success",icon_custom_emoji_id=btn_emo("admin_spin_settings")))
        mk.add(InlineKeyboardButton("◀️ Back",callback_data="admin_cat_settings",style="primary",icon_custom_emoji_id=btn_emo("admin_cat_settings")))
        bot.edit_message_text(
            f"🎡 <b>Spin Settings</b>\n\nCurrent: {html.escape(cfg)}\n\n"
            f"Format: <code>0:50,5:30,10:15,20:5</code>\nTotal probability 100% hona chahiye.\n\n"
            f"🎨 <b>Spin Animation:</b> Neeche wale 5 frames ko Premium Emoji Manager se set kar sakte ho. "
            f"Animated Telegram Premium custom emoji use karoge to spin animation mein wahi emoji dikhegi.",
            chat_id,msg_id,parse_mode="HTML",reply_markup=mk
        )
    elif data == "admin_spin_animation_emojis" and call.from_user.id == ADMIN_USER_ID:
        # Use the callback actor directly; this avoids stale/undefined user_id
        # references from older deployed versions.
        show_emoji_slots(chat_id, msg_id, "Daily Spin Animation")
    elif data == "admin_spin_toggle":
        set_setting("spin_enabled","0" if spin_enabled() else "1")
        return _show_spin_admin_settings(chat_id,msg_id)
    elif data == "admin_spin_prob":
        cfg=", ".join(f"{r}:{p:.0f}" for r,p in get_spin_config())
        bot.edit_message_text(f"🎯 <b>Spin Rewards & Probability</b>\n\nCurrent: <code>{cfg}</code>\n\nExample: <code>0:50,5:30,10:15,20:5</code>\nProbability total exactly 100 hona chahiye.",chat_id,msg_id,parse_mode="HTML",reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("◀️ Back",callback_data="admin_spin_settings",style="primary")))
        user_states[ADMIN_USER_ID]={"action":"set_spin_prob"}
        bot.register_next_step_handler(call.message, save_spin_probabilities)
    elif data == "admin_stats":
        conn = get_db()
        cursor = conn.cursor()
        today_str = ist_today_str()
        month_str = now_ist().strftime("%Y-%m")

        cursor.execute("SELECT COUNT(*) FROM users")
        total_users = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM users WHERE last_active=?", (today_str,))
        active_today = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM license_keys")
        total_keys = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM license_keys WHERE used=0")
        keys_available = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*), COALESCE(SUM(amount),0) FROM transactions WHERE type='purchase'")
        total_orders, total_sales = cursor.fetchone()

        cursor.execute("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type='purchase' AND date(created_at)=?", (today_str,))
        today_sales = cursor.fetchone()[0] or 0

        cursor.execute("SELECT COALESCE(SUM(amount),0) FROM transactions WHERE type='purchase' AND strftime('%Y-%m', created_at)=?", (month_str,))
        month_sales = cursor.fetchone()[0] or 0

        cursor.execute("SELECT COUNT(*) FROM users WHERE banned=1")
        banned = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        stats_text = (
            f"📊 <b>Bot Stats</b>\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"👥 Total Users: {total_users}\n"
            f"🟢 Active Today: {active_today}\n"
            f"🚫 Banned Users: {banned}\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"🔑 Total Keys: {total_keys}\n"
            f"✅ Available Keys: {keys_available}\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"🛒 Total Orders: {total_orders}\n"
            f"💰 Today's Sale: ₹{today_sales}\n"
            f"📈 This Month's Sale: ₹{month_sales}\n"
            f"💎 All-Time Sale: ₹{total_sales}\n"
            f"━━━━━━━━━━━━━━━━━"
        )
        bot.edit_message_text(stats_text, chat_id, msg_id, parse_mode="HTML", reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton(" Back", callback_data="admin_cat_settings", style="primary", icon_custom_emoji_id=btn_emo("admin_cat_settings"))))
    elif data == "admin_pending_payments":
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT id, user_id, order_id, amount, status FROM pending_payments WHERE status='pending' ORDER BY id DESC LIMIT 20")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        if not rows:
            bot.edit_message_text("✅ No pending payments right now — ZapUPI auto-credits these, so this list should normally stay empty.", chat_id, msg_id, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton(" Back", callback_data="admin_panel", style="primary", icon_custom_emoji_id=btn_emo("admin_panel"))))
            return
        text = " <b>Pending Payments</b>\n\nYe normally ZapUPI khud auto-credit kar deta hai. Neeche wale sirf manual override ke liye hain (agar ZapUPI se koi issue ho):\n\n"
        markup = InlineKeyboardMarkup(row_width=2)
        for r in rows:
            text += f"🆔 <code>{r[2]}</code>\n👤 {r[1]} | ₹{r[3]}\n\n"
            markup.add(
                InlineKeyboardButton(f"✅ Force Approve {r[2]}", callback_data=f"approve_pay_{r[0]}", style="success", icon_custom_emoji_id=btn_emo("approve_pay")),
                InlineKeyboardButton(f"❌ Reject {r[2]}", callback_data=f"reject_pay_{r[0]}", style="danger", icon_custom_emoji_id=btn_emo("reject_pay"))
            )
        markup.add(InlineKeyboardButton(" Back", callback_data="admin_panel", style="primary", icon_custom_emoji_id=btn_emo("admin_panel")))
        bot.edit_message_text(text, chat_id, msg_id, parse_mode="HTML", reply_markup=markup)
    elif data == "admin_set_selling_proof":
        bot.edit_message_text("🏆 Send the Selling Proof channel link (e.g. https://t.me/yourchannel):", chat_id, msg_id, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Cancel", callback_data="admin_cat_settings", style="danger", icon_custom_emoji_id=btn_emo("admin_cat_settings"))))
        bot.register_next_step_handler(call.message, save_selling_proof_link)
    elif data == "admin_set_updatechannel":
        bot.edit_message_text("📢 Send the Update/Join channel link shown after every purchase (e.g. https://t.me/yourchannel):", chat_id, msg_id, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Cancel", callback_data="admin_cat_settings", style="danger", icon_custom_emoji_id=btn_emo("admin_cat_settings"))))
        bot.register_next_step_handler(call.message, save_updatechannel_link)
    elif data == "admin_set_tutorial_video":
        bot.edit_message_text("🎥 Send the Tutorial video link (e.g. https://youtu.be/... or https://t.me/...):", chat_id, msg_id, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Cancel", callback_data="admin_cat_settings", style="danger", icon_custom_emoji_id=btn_emo("admin_cat_settings"))))
        bot.register_next_step_handler(call.message, save_tutorial_video_link)
    elif data == "admin_custom_button_menu":
        cur_text = get_setting("custom_button_text") or "Not set"
        cur_link = get_setting("custom_button_link") or "Not set"
        cur_enabled = get_setting("custom_button_enabled") == "1"
        status = "🟢 ON (visible)" if cur_enabled else "🔴 OFF (hidden)"
        text = (f"🔘 <b>Custom Menu Button</b>\n\n"
                f"Text: {html.escape(cur_text)}\n"
                f"Link: {html.escape(cur_link)}\n"
                f"Status: {status}")
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(
            InlineKeyboardButton("✏️ Set Button Text", callback_data="admin_set_custom_btn_text", style="success", icon_custom_emoji_id=btn_emo("admin_set_custom_btn_text")),
            InlineKeyboardButton("🔗 Set Button Link", callback_data="admin_set_custom_btn_link", style="success", icon_custom_emoji_id=btn_emo("admin_set_custom_btn_link")),
            InlineKeyboardButton("🔁 Toggle ON/OFF", callback_data="admin_toggle_custom_btn", style="success", icon_custom_emoji_id=btn_emo("admin_toggle_custom_btn")),
            InlineKeyboardButton(" Back", callback_data="admin_cat_settings", style="primary", icon_custom_emoji_id=btn_emo("admin_cat_settings")),
        )
        bot.edit_message_text(text, chat_id, msg_id, parse_mode="HTML", reply_markup=markup)
    elif data == "admin_set_custom_btn_text":
        bot.edit_message_text("✏️ Send the button text (with emoji if you want, e.g. 🎁 Offers):", chat_id, msg_id)
        bot.register_next_step_handler(call.message, save_custom_btn_text)
    elif data == "admin_set_custom_btn_link":
        bot.edit_message_text("🔗 Send the link this button should open (e.g. https://t.me/yourchannel):", chat_id, msg_id)
        bot.register_next_step_handler(call.message, save_custom_btn_link)
    elif data == "admin_toggle_custom_btn":
        cur_enabled = get_setting("custom_button_enabled") == "1"
        cur_text = get_setting("custom_button_text")
        cur_link = get_setting("custom_button_link")
        if not cur_enabled and (not cur_text or not cur_link):
            bot.answer_callback_query(call.id, "⚠️ Pehle button text aur link set karo.", show_alert=True)
        else:
            set_setting("custom_button_enabled", "0" if cur_enabled else "1")
        # Re-show the menu with updated status
        cur_text2 = get_setting("custom_button_text") or "Not set"
        cur_link2 = get_setting("custom_button_link") or "Not set"
        cur_enabled2 = get_setting("custom_button_enabled") == "1"
        status2 = "🟢 ON (visible)" if cur_enabled2 else "🔴 OFF (hidden)"
        text2 = (f"🔘 <b>Custom Menu Button</b>\n\n"
                 f"Text: {html.escape(cur_text2)}\n"
                 f"Link: {html.escape(cur_link2)}\n"
                 f"Status: {status2}")
        markup2 = InlineKeyboardMarkup(row_width=1)
        markup2.add(
            InlineKeyboardButton("✏️ Set Button Text", callback_data="admin_set_custom_btn_text", style="success", icon_custom_emoji_id=btn_emo("admin_set_custom_btn_text")),
            InlineKeyboardButton("🔗 Set Button Link", callback_data="admin_set_custom_btn_link", style="success", icon_custom_emoji_id=btn_emo("admin_set_custom_btn_link")),
            InlineKeyboardButton("🔁 Toggle ON/OFF", callback_data="admin_toggle_custom_btn", style="success", icon_custom_emoji_id=btn_emo("admin_toggle_custom_btn")),
            InlineKeyboardButton(" Back", callback_data="admin_cat_settings", style="primary", icon_custom_emoji_id=btn_emo("admin_cat_settings")),
        )
        bot.edit_message_text(text2, chat_id, msg_id, parse_mode="HTML", reply_markup=markup2)
    elif data == "orderprod_all":
        show_order_category(call, call.from_user.id, "all")
        return
    elif data.startswith("orderprod_"):
        show_order_category(call, call.from_user.id, data[len("orderprod_"):])
        return
    elif data.startswith("ordercatname_"):
        # Legacy callback support for buttons generated by an older running message.
        try: category=urllib.parse.unquote(data[len("ordercatname_"):])
        except Exception: category=data[len("ordercatname_"):]
        rows=_get_user_purchase_rows(call.from_user.id)
        selected=[r for r in rows if _order_category_name(r[2])==category]
        if not selected:
            bot.answer_callback_query(call.id,"⚠️ Product orders nahi mile.",show_alert=True); return
        # Pick the first matching product ID; new buttons use exact product IDs.
        show_order_category(call, call.from_user.id, str(selected[0][1]))
        return
    elif data.startswith("ordercat_"):
        show_order_category(call, call.from_user.id, data[len("ordercat_"):])
        return
    elif data == "admin_user_details":
        bot.edit_message_text("🔍 Send User ID, @username, phone, or name to search:", chat_id, msg_id, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Cancel", callback_data="admin_cat_user", style="danger", icon_custom_emoji_id=btn_emo("admin_cat_user"))))
        bot.register_next_step_handler(call.message, show_user_details_admin)
    elif data == "admin_edit_product_all":
        show_product_picker(chat_id, msg_id, "hub", "✏️ Select product to edit (name/emoji/description/etc):")
    elif data == "admin_edit_product_plans":
        show_product_picker(chat_id, msg_id, "hub", "💰 Select product to edit (full menu will open, then tap 💰 Edit Plan Prices):")
    elif data == "admin_set_prod_channel":
        show_product_picker(chat_id, msg_id, "setchannel", "🔗 Select product to set its channel link:")
    elif data == "admin_set_prod_desc":
        show_product_picker(chat_id, msg_id, "setdesc", "📝 Select product to set its description:")
    elif data == "admin_set_prod_device":
        show_product_picker(chat_id, msg_id, "setdevice", "📱 Select product to set its device type:")
    elif data == "admin_toggle_prod_status":
        show_product_picker(chat_id, msg_id, "toggle", "🟢 Select product to toggle Active/Inactive:")
    elif data == "admin_set_prod_video":
        show_product_picker(chat_id, msg_id, "setvideo", "🎬 Select product to set its demo video:")

def show_product_picker(chat_id, msg_id, action_prefix, prompt):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, working_status, name_entities FROM products ORDER BY sort_order, id")
    prods = cursor.fetchall()
    cursor.close()
    conn.close()
    if not prods:
        bot.edit_message_text("No products yet. Add a product first.", chat_id, msg_id)
        return
    markup = InlineKeyboardMarkup()
    for p in prods:
        status_dot = "🟢" if (p[2] or "active") == "active" else "🔴"
        markup.add(InlineKeyboardButton(f"{status_dot} {strip_custom_emoji(p[1], p[3])}", callback_data=f"prodpick_{action_prefix}_{p[0]}", style="success", icon_custom_emoji_id=btn_emo("prodpick")))
    markup.add(InlineKeyboardButton(" Cancel", callback_data="admin_cat_product", style="danger", icon_custom_emoji_id=btn_emo("admin_cat_product")))
    bot.edit_message_text(prompt, chat_id, msg_id, reply_markup=markup)

def show_maintenance_toggle_list(chat_id, msg_id):
    """Lists every product with its maintenance status. Tapping a product toggles
    its maintenance mode straight away and refreshes this same list, so the admin
    can flip several products on/off quickly without re-navigating each time."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, maintenance_enabled, maintenance_emoji, name_entities FROM products ORDER BY sort_order, id")
    prods = cursor.fetchall()
    cursor.close()
    conn.close()
    if not prods:
        bot.edit_message_text("No products yet. Add a product first.", chat_id, msg_id,
                               reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton(" Back", callback_data="admin_cat_product", style="primary", icon_custom_emoji_id=btn_emo("admin_cat_product"))))
        return
    markup = InlineKeyboardMarkup()
    for p in prods:
        status_dot = (p[3] or "🔴") if p[2] else "⚪"
        markup.add(InlineKeyboardButton(f"{status_dot} {strip_custom_emoji(p[1], p[4])}", callback_data=f"mainttoggle_{p[0]}", style="success", icon_custom_emoji_id=btn_emo("mainttoggle")))
    markup.add(InlineKeyboardButton(" Back", callback_data="admin_cat_product", style="primary", icon_custom_emoji_id=btn_emo("admin_cat_product")))
    text = ("🛠️ <b>Maintenance Mode</b>\n\n"
            "Kisi bhi product par tap karke uska maintenance mode ON/OFF karo. "
            "ON hone par shop list mein us product ke naam ke aage dot/emoji dikhega, "
            "aur user use open karega to plans ki jagah uski maintenance description dikhegi.")
    bot.edit_message_text(text, chat_id, msg_id, parse_mode="HTML", reply_markup=markup)

# ======================= PREMIUM EMOJI MANAGER (admin UI) =======================
def show_emoji_categories(chat_id, msg_id):
    """Top level: list every screen/category that has emoji slots."""
    categories = []
    for key, (default, label, category) in EMOJI_SLOTS.items():
        if category not in categories:
            categories.append(category)
    markup = InlineKeyboardMarkup(row_width=1)
    for cat in categories:
        markup.add(InlineKeyboardButton(f"📂 {cat}", callback_data=f"emojicat_{cat}", style="success", icon_custom_emoji_id=btn_emo("emojicat")))
    markup.add(InlineKeyboardButton(" Back", callback_data="admin_cat_settings", style="primary", icon_custom_emoji_id=btn_emo("admin_cat_settings")))
    bot.edit_message_text(
        "🎨 <b>Premium Emoji Manager</b>\n\n"
        "Har screen ke corner-corner ka emoji yahan se apni marzi ka Telegram Premium "
        "custom emoji laga sakte ho. Neeche se ek screen/category chuno, phir jo emoji "
        "change karna hai us par tap karo.",
        chat_id, msg_id, parse_mode="HTML", reply_markup=markup
    )

def show_emoji_slots(chat_id, msg_id, category):
    """Show every emoji slot inside one category, with its current status.
    Category matching is case-insensitive so older callback strings cannot produce
    an empty/undefined-looking screen.
    """
    category = str(category or "").strip()
    markup = InlineKeyboardMarkup(row_width=1)
    matched = 0
    for slot_key, (default, label, cat) in EMOJI_SLOTS.items():
        if str(cat).strip().lower() != category.lower():
            continue
        matched += 1
        current = get_setting(f"emoji_slot_{slot_key}")
        dot = "✅" if current else "⚪"
        markup.add(InlineKeyboardButton(f"{dot} {default} — {label}", callback_data=f"emojislot_{slot_key}", style="success", icon_custom_emoji_id=btn_emo("emojislot")))
    markup.add(InlineKeyboardButton(" Back", callback_data="admin_emoji_manager", style="primary", icon_custom_emoji_id=btn_emo("admin_emoji_manager")))
    if matched == 0:
        # Never leave the admin on a dead/blank callback screen.
        bot.edit_message_text(
            f"⚠️ <b>{html.escape(category)} Emoji Settings</b>\n\n"
            f"Is category ke emoji slots current build mein nahi mile.\n"
            f"Premium Emoji Manager se category dobara kholo.",
            chat_id, msg_id, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("◀️ Back", callback_data="admin_leaderboard_settings", style="primary"))
        )
        return
    bot.edit_message_text(
        f"📂 <b>{html.escape(category)}</b>\n\n"
        f"✅ = custom emoji set • ⚪ = default emoji\n\nKis emoji ko change karna hai?",
        chat_id, msg_id, parse_mode="HTML", reply_markup=markup
    )

def save_emoji_slot(message, slot_key):
    if _check_cancel(message):
        return
    user_id = message.from_user.id
    info = EMOJI_SLOTS.get(slot_key)
    if not info:
        return
    default, label, category = info
    custom_id = None
    # Case 1: message contains an actual custom emoji entity (typed or forwarded)
    entities = message.entities or message.caption_entities
    if entities:
        for ent in entities:
            if getattr(ent, "type", None) == "custom_emoji" and getattr(ent, "custom_emoji_id", None):
                custom_id = ent.custom_emoji_id
                break
    # Case 2: admin pasted the numeric custom_emoji_id directly as text
    if not custom_id and message.text and message.text.strip().isdigit():
        custom_id = message.text.strip()
    if not custom_id:
        bot.reply_to(message, "❌ Custom emoji nahi mila. Ek custom emoji bhejo (ya forward karo), ya uski numeric ID paste karo. Dobara try karo:")
        bot.register_next_step_handler(message, save_emoji_slot, slot_key)
        return
    set_setting(f"emoji_slot_{slot_key}", str(custom_id))
    bot.reply_to(message, f"✅ <b>{html.escape(label)}</b> ka emoji save ho gaya.\n\nAb ye Leaderboard/Main Menu mein use hoga.", parse_mode="HTML")
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton(f"◀️ Back to {category}", callback_data=f"emojicat_{category}", style="primary", icon_custom_emoji_id=btn_emo("emojicat")))
    bot.send_message(message.chat.id, f"🎨 <b>{html.escape(category)}</b> — Emoji saved successfully.", parse_mode="HTML", reply_markup=markup)

def _find_admin_user(query):
    """Find a user already known to this bot.
    Supports Telegram ID, @username/username, phone digits, and name.
    A bot cannot fetch arbitrary Telegram users who have never interacted with it,
    so the search is deliberately DB-backed and never pretends otherwise.
    """
    q=(query or "").strip()
    conn=get_db(); cur=conn.cursor()
    row=None
    if q.isdigit():
        cur.execute("SELECT * FROM users WHERE id=?",(int(q),)); row=cur.fetchone()
    if not row and q:
        clean=q.lstrip("@").strip().lower()
        phone_clean=re.sub(r"\D+","",q)
        cur.execute("""
            SELECT * FROM users
            WHERE lower(COALESCE(username,''))=?
               OR lower(COALESCE(first_name,''))=?
               OR replace(replace(replace(COALESCE(phone_number,''),' ',''),'-',''),'+','')=?
            ORDER BY id DESC LIMIT 1
        """,(clean,clean,phone_clean))
        row=cur.fetchone()
    if not row and q:
        clean=q.lstrip("@").strip().lower()
        like=f"%{clean}%"
        cur.execute("""
            SELECT * FROM users
            WHERE lower(COALESCE(username,'')) LIKE ?
               OR lower(COALESCE(first_name,'')) LIKE ?
               OR replace(replace(replace(COALESCE(phone_number,''),' ',''),'-',''),'+','') LIKE ?
            ORDER BY last_active DESC, id DESC LIMIT 1
        """,(like,like,f"%{re.sub(r'\D+','',q)}%"))
        row=cur.fetchone()
    cur.close(); conn.close(); return row

def show_user_details_admin(message):
    if _check_cancel(message):
        return

    try:
        u = _find_admin_user(message.text)
    except Exception as e:
        traceback.print_exc()
        bot.reply_to(
            message,
            f"❌ User search error: <code>{html.escape(str(e)[:500])}</code>",
            parse_mode="HTML"
        )
        return

    if not u:
        bot.reply_to(
            message,
            "❌ User nahi mila.\n\n"
            "Telegram User ID, @username, phone, ya name bhejo.\n"
            "⚠️ Sirf wahi users milenge jinhone bot ko pehle /start karke database mein entry banayi hai."
        )
        return

    uid = u[0]
    conn = get_db()
    cur = conn.cursor()

    try:
        # -------------------- LIVE USER DATA --------------------
        cur.execute("""
            SELECT username, balance, joined_at, banned, is_reseller,
                   reseller_banned, first_name, referral_earnings,
                   phone_number
            FROM users WHERE id=?
        """, (uid,))
        user_row = cur.fetchone()

        # -------------------- COMPLETE TRANSACTION LEDGER --------------------
        # IMPORTANT: The old screen only showed add_money/admin adjustments in
        # the money-history section. Purchases were shown separately, so an
        # admin could not directly see where the user's balance was spent.
        # Here every recorded balance-changing transaction is shown together.
        cur.execute("""
            SELECT id, amount, type, details, created_at
            FROM transactions
            WHERE user_id=?
            ORDER BY id DESC
            LIMIT 300
        """, (uid,))
        ledger = list(cur.fetchall())

        # -------------------- PAYMENT / DEPOSIT RECORDS --------------------
        # This shows payment attempts as well as successful deposits, so a
        # pending/failed payment cannot be confused with money actually added.
        payment_rows = []
        try:
            cur.execute("""
                SELECT id, amount, status, gateway, order_id, created_at
                FROM pending_payments
                WHERE user_id=?
                ORDER BY id DESC
                LIMIT 100
            """, (uid,))
            payment_rows = list(cur.fetchall())
        except Exception:
            # Compatibility with an older DB schema without gateway/order_id.
            try:
                cur.execute("""
                    SELECT id, amount, status, created_at
                    FROM pending_payments
                    WHERE user_id=?
                    ORDER BY id DESC
                    LIMIT 100
                """, (uid,))
                payment_rows = [
                    (r[0], r[1], r[2], None, None, r[3])
                    for r in cur.fetchall()
                ]
            except Exception:
                payment_rows = []

        # -------------------- PURCHASE DETAILS --------------------
        cur.execute("""
            SELECT p.name, lk.license_key, lk.used_at, pl.price,
                   pl.days, pl.duration_unit
            FROM license_keys lk
            JOIN products p ON p.id=lk.product_id
            LEFT JOIN plans pl ON pl.id=lk.plan_id
            WHERE lk.used_by=?
        """, (uid,))
        purchases = list(cur.fetchall())

        cur.execute("""
            SELECT p.name, mo.license_key, mo.created_at, mo.price,
                   pl.days, pl.duration_unit
            FROM manual_orders mo
            JOIN products p ON p.id=mo.product_id
            LEFT JOIN plans pl ON pl.id=mo.plan_id
            WHERE mo.user_id=?
              AND mo.status='completed'
              AND mo.license_key IS NOT NULL
        """, (uid,))
        purchases += list(cur.fetchall())

        # -------------------- REFERRAL / REWARD TOTALS --------------------
        def tx_sum(tx_type):
            cur.execute(
                "SELECT COALESCE(SUM(amount),0) FROM transactions "
                "WHERE user_id=? AND type=?",
                (uid, tx_type)
            )
            return cur.fetchone()[0] or 0

        deposits = tx_sum("add_money")
        admin_added = tx_sum("admin_add_balance")
        admin_removed = tx_sum("admin_remove_balance")
        refunds = tx_sum("refund")
        referral_commission = tx_sum("referral_commission")
        legacy_referral_rewards = tx_sum("referral_reward")
        spin_rewards = tx_sum("spin_reward")
        first_purchase_bonus = tx_sum("first_purchase_bonus")
        purchase_spend = tx_sum("purchase")

        cur.execute(
            "SELECT COALESCE(referred_by,0) FROM users WHERE id=?",
            (uid,)
        )
        referred_by = cur.fetchone()[0] or 0

        cur.execute(
            "SELECT COUNT(*) FROM users WHERE referred_by=?",
            (uid,)
        )
        referred_count = cur.fetchone()[0] or 0

        cur.execute("""
            SELECT COALESCE(SUM(amount),0)
            FROM transactions
            WHERE user_id IN (SELECT id FROM users WHERE referred_by=?)
              AND type='purchase'
        """, (uid,))
        referred_spent = cur.fetchone()[0] or 0

    finally:
        cur.close()
        conn.close()

    current_balance = int((user_row[1] if user_row else 0) or 0)

    # Transaction amounts are stored as positive values. Direction is derived
    # from the transaction type because that is how this bot records them.
    debit_types = {"purchase", "admin_remove_balance"}
    credit_types = {
        "add_money",
        "admin_add_balance",
        "refund",
        "referral_reward",
        "referral_commission",
        "spin_reward",
        "first_purchase_bonus",
    }

    labels = {
        "add_money": "Gateway Deposit",
        "admin_add_balance": "Admin Added",
        "admin_remove_balance": "Admin Removed",
        "purchase": "Purchase / Spent",
        "refund": "Refund",
        "referral_reward": "Referral Reward",
        "referral_commission": "Referral Commission",
        "spin_reward": "Spin Reward",
        "first_purchase_bonus": "First Purchase Bonus",
    }

    signed_net = 0
    for _tid, amount, typ, _details, _dt in ledger:
        amount = int(amount or 0)
        if typ in debit_types:
            signed_net -= amount
        elif typ in credit_types:
            signed_net += amount

    # This is useful for auditing: if all balance-changing activity was
    # recorded, opening_balance + net_change = current_balance.
    inferred_opening = current_balance - signed_net

    name = (user_row[6] if user_row else None) or u[12] or "N/A"
    username = (user_row[0] if user_row else None) or u[1] or "N/A"
    phone = ((user_row[8] if user_row else None) or u[8] or "").strip() or "Not shared"
    joined = format_ist_datetime(user_row[2] if user_row else u[6])

    lines = [
        "🔍 <b>USER DETAILS</b>",
        "━━━━━━━━━━━━━━━━━━",
        f"🆔 <b>User ID</b>\n<code>{uid}</code>",
        f"👤 <b>Name</b>\n{html.escape(str(name))}",
        f"🔗 <b>Username</b>\n{('@' + html.escape(str(username))) if username != 'N/A' else 'N/A'}",
        f"📱 <b>Phone</b>\n{html.escape(str(phone))}",
        f"📅 <b>Joined</b>\n{joined}",
        f"⚡ <b>Status</b>\n{'🚫 Banned' if (user_row and user_row[3]) else '✅ Active'}",
        "",
        "💰 <b>BALANCE AUDIT</b>",
        "━━━━━━━━━━━━━━━━━━",
        f"💰 Current Balance: <b>₹{current_balance}</b>",
        f"📈 Recorded Net Change: <b>{'+' if signed_net >= 0 else ''}₹{signed_net}</b>",
        f"🧮 Inferred Opening Balance: <b>₹{inferred_opening}</b>",
        "",
        "💳 <b>MONEY SUMMARY</b>",
        "━━━━━━━━━━━━━━━━━━",
        f"💵 Gateway Deposits: <b>₹{deposits}</b>",
        f"👑 Admin Added: <b>₹{admin_added}</b>",
        f"↩️ Admin Removed: <b>₹{admin_removed}</b>",
        f"↩️ Refunds: <b>₹{refunds}</b>",
        f"🛒 Purchases / Spent: <b>₹{purchase_spend}</b>",
        f"🎁 Rewards/Bonuses: <b>₹{referral_commission + legacy_referral_rewards + spin_rewards + first_purchase_bonus}</b>",
        "",
        "🛒 <b>PURCHASE SUMMARY</b>",
        "━━━━━━━━━━━━━━━━━━",
        f"📦 Completed Purchase Records: <b>{len(purchases)}</b>",
        f"💸 Purchase Transactions: <b>₹{purchase_spend}</b>",
        "",
        "👥 <b>REFERRAL</b>",
        "━━━━━━━━━━━━━━━━━━",
        f"👥 Referred Users: <b>{referred_count}</b>",
        f"🧾 Referred Users Spend: <b>₹{referred_spent}</b>",
        f"💸 Commission Earned: <b>₹{referral_commission}</b>",
        f"🔗 Referred By: <b>{referred_by if referred_by else 'None'}</b>",
        f"🎁 Legacy Join Bonus: <b>₹{legacy_referral_rewards}</b>",
        f"🎡 Spin Rewards: <b>₹{spin_rewards}</b>",
    ]

    # -------------------- COMPLETE PAYMENT HISTORY --------------------
    lines += [
        "",
        "🏦 <b>DEPOSIT / PAYMENT HISTORY</b>",
        "━━━━━━━━━━━━━━━━━━",
    ]

    if payment_rows:
        for pid, amount, status, gateway, order_id, dt in payment_rows:
            status_text = str(status or "unknown").upper()
            gateway_text = str(gateway or "UPI").upper()
            order_text = str(order_id or "N/A")
            lines.append(
                f"🧾 #{pid} • <b>{status_text}</b>\n"
                f"💰 ₹{int(amount or 0)} • {html.escape(gateway_text)}\n"
                f"🆔 Order: <code>{html.escape(order_text)}</code>\n"
                f"📅 {format_ist_datetime(dt)}\n"
                "────────────"
            )
    else:
        lines.append("No payment records found.")

    # -------------------- FULL BALANCE LEDGER --------------------
    lines += [
        "",
        "📒 <b>COMPLETE BALANCE LEDGER</b>",
        "━━━━━━━━━━━━━━━━━━",
        "⬇️ = balance kam | ⬆️ = balance badha",
    ]

    if ledger:
        for tid, amount, typ, details, dt in ledger:
            amount = int(amount or 0)
            if typ in debit_types:
                sign = "−"
                arrow = "⬇️"
            elif typ in credit_types:
                sign = "+"
                arrow = "⬆️"
            else:
                sign = "?"
                arrow = "❔"

            label = labels.get(typ, str(typ or "Unknown").replace("_", " ").title())
            detail = html.escape(str(details or "").strip())
            if len(detail) > 220:
                detail = detail[:217] + "..."

            lines.append(
                f"{arrow} <b>#{tid} {html.escape(label)}</b> • "
                f"<b>{sign}₹{amount}</b>\n"
                f"📅 {format_ist_datetime(dt)}\n"
                f"📝 {detail or 'No details'}\n"
                "────────────"
            )
    else:
        lines.append("No transaction ledger records found.")

    # -------------------- PURCHASE HISTORY --------------------
    lines += [
        "",
        "🛒 <b>PURCHASE HISTORY</b>",
        "━━━━━━━━━━━━━━━━━━",
    ]

    purchases.sort(key=lambda x: x[2] or "", reverse=True)

    if purchases:
        for n, (pname, key, dt, price, days, duration_unit) in enumerate(purchases, 1):
            expiry = _compute_expiry_label(
                dt,
                (days, duration_unit) if days is not None else None
            )
            lines.append(
                f"#{n} <b>{html.escape(pname)}</b>\n"
                f"💰 Price: <b>₹{price or 0}</b>\n"
                f"📅 Purchased: <b>{format_ist_datetime(dt)}</b>\n"
                f"⏳ Expires: <b>{expiry}</b>\n"
                f"🔑 Key: <code>{html.escape(key or 'N/A')}</code>\n"
                "────────────"
            )
    else:
        lines.append("No completed purchases.")

    lines += [
        "",
        "ℹ️ <b>AUDIT NOTE</b>",
        "This screen now reads the transaction ledger directly. "
        "So purchases, deposits, admin changes, refunds and rewards are "
        "shown in one chronological balance trail.",
        f"📊 Showing latest <b>{len(ledger)}</b> transaction records "
        f"(maximum 300).",
    ]

    out_text = "\n".join(lines)

    markup = InlineKeyboardMarkup().add(
        InlineKeyboardButton(
            "Back to Admin",
            callback_data="admin_cat_user",
            style="primary",
            icon_custom_emoji_id=btn_emo("admin_cat_user")
        )
    )

    # Never cut HTML tags in half. Split only at newline boundaries.
    max_len = 3900
    chunks = []
    current = ""

    for line in out_text.split("\n"):
        candidate = line if not current else current + "\n" + line
        if len(candidate) <= max_len:
            current = candidate
        else:
            if current:
                chunks.append(current)

            if len(line) > max_len:
                # Extremely long details are already truncated above, but keep
                # this guard so malformed HTML can never be produced.
                plain_line = re.sub(r"<[^>]+>", "", line)
                chunks.append(plain_line[:max_len])
                current = ""
            else:
                current = line

    if current:
        chunks.append(current)

    try:
        for i, chunk in enumerate(chunks):
            bot.reply_to(
                message,
                chunk,
                parse_mode="HTML",
                reply_markup=markup if i == len(chunks) - 1 else None
            )
    except Exception:
        # Last-resort fallback: still show the audit data as plain text.
        plain = re.sub(r"<[^>]+>", "", out_text)
        if len(plain) > max_len:
            plain = plain[:max_len] + "\n\n[Older history continues in the ledger]"
        try:
            bot.reply_to(message, plain, reply_markup=markup)
        except Exception as e:
            print(f"User details send error: {e}")

def save_selling_proof_link(message):
    if _check_cancel(message):
        return
    link = message.text.strip()
    if not link.startswith("http"):
        bot.reply_to(message, "❌ Invalid link. Must start with http:// or https://")
        return
    set_setting("selling_proof_link", link)
    bot.reply_to(message, "✅ Selling Proof link updated! It now shows on the main menu.")

def save_updatechannel_link(message):
    if _check_cancel(message):
        return
    link = message.text.strip()
    if not link.startswith("http"):
        bot.reply_to(message, "❌ Invalid link. Must start with http:// or https://")
        return
    set_setting("update_channel_link", link)
    bot.reply_to(message, "✅ Update channel link updated! It will now show after every purchase.")

def save_tutorial_video_link(message):
    if _check_cancel(message):
        return
    link = message.text.strip()
    if not link.startswith("http"):
        bot.reply_to(message, "❌ Invalid link. Must start with http:// or https://")
        return
    set_setting("tutorial_video_link", link)
    bot.reply_to(message, "✅ Tutorial video link updated! It now shows on the Tutorial screen.")

def save_external_api_url(message):
    if _check_cancel(message):
        return
    val = message.text.strip()
    if not val.startswith("http"):
        bot.reply_to(message, "❌ Invalid URL. http:// ya https:// se start hona chahiye.")
        return
    set_setting("reseller_api_url", val)
    bot.reply_to(message, "✅ External API Endpoint URL update ho gaya!")

def save_external_api_key(message):
    if _check_cancel(message):
        return
    val = message.text.strip()
    if not val:
        bot.reply_to(message, "❌ Empty key allowed nahi. Dobara try karo.")
        return
    set_setting("reseller_api_key", val)
    bot.reply_to(message, "✅ External API Key update ho gayi!")

def save_external_api_masterkey(message):
    if _check_cancel(message):
        return
    val = message.text.strip()
    if not val:
        bot.reply_to(message, "❌ Empty key allowed nahi. Dobara try karo.")
        return
    set_setting("reseller_master_key", val)
    bot.reply_to(message, "✅ External API Master Key update ho gayi!")

def save_keyspanels_url(message):
    if _check_cancel(message):
        return
    val = (message.text or "").strip()
    if not val.startswith("http"):
        bot.reply_to(message, "❌ Invalid URL. http:// ya https:// se start hona chahiye.")
        return
    set_setting("keyspanels_api_url", val)
    bot.reply_to(message, "✅ KeysPanelShop API Endpoint URL update ho gaya!")

def save_keyspanels_masterkey(message):
    if _check_cancel(message):
        return
    val = (message.text or "").strip()
    if not val:
        bot.reply_to(message, "❌ Empty master key allowed nahi. Dobara try karo.")
        return
    set_setting("keyspanels_master_key", val)
    bot.reply_to(message, "✅ KeysPanelShop Master Key update ho gaya!")

def save_zapupi_key(message):
    if _check_cancel(message):
        return
    val = message.text.strip()
    if not val:
        bot.reply_to(message, "❌ Empty key allowed nahi. Dobara try karo.")
        return
    set_setting("zapupi_zap_key", val)
    bot.reply_to(message, "✅ ZapUPI Zap Key update ho gayi!")

def save_fampay_key(message):
    if _check_cancel(message):
        return
    val = message.text.strip()
    if not val:
        bot.reply_to(message, "❌ Empty key allowed nahi. Dobara try karo.")
        return
    set_setting("fampay_api_key", val)
    bot.reply_to(message, "✅ FamPay API Key update ho gayi!")

def save_fampay_upi(message):
    if _check_cancel(message):
        return
    val = message.text.strip()
    if '@' not in val:
        bot.reply_to(message, "❌ Invalid UPI ID! '@' hona chahiye. Example: example@okhdfcbank")
        return
    set_setting("fampay_upi_id", val)
    bot.reply_to(message, "✅ FamPay UPI ID update ho gayi!")

def save_custom_btn_text(message):
    if _check_cancel(message):
        return
    text = message.text.strip()
    if not text:
        bot.reply_to(message, "❌ Empty text not allowed.")
        return
    set_setting("custom_button_text", text)
    bot.reply_to(message, "✅ Button text saved! Don't forget to toggle it ON and set a link if you haven't.")

def save_custom_btn_link(message):
    if _check_cancel(message):
        return
    link = message.text.strip()
    if not link.startswith("http"):
        bot.reply_to(message, "❌ Invalid link. Must start with http:// or https://")
        return
    set_setting("custom_button_link", link)
    bot.reply_to(message, "✅ Button link saved! Don't forget to toggle it ON.")


def send_announcement(message, duration_seconds=5*3600):
    if _check_cancel(message):
        return
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM users WHERE COALESCE(banned,0)=0")
    all_user_ids = [row[0] for row in cursor.fetchall()]

    photo_file_id = message.photo[-1].file_id if message.photo else None
    video_file_id = message.video.file_id if message.video else None
    voice_file_id = message.voice.file_id if message.voice else None
    audio_file_id = message.audio.file_id if message.audio else None
    document_file_id = message.document.file_id if message.document else None
    has_media = photo_file_id or video_file_id or voice_file_id or audio_file_id or document_file_id

    # A media message's description lives in .caption (with .caption_entities for
    # formatting/premium emoji/links), a plain text message's in .text (with
    # .entities) -- using the wrong pair silently drops formatting or the caption.
    caption_text = message.caption if has_media else message.text
    caption_text = caption_text or ""
    caption_entities = (message.caption_entities if has_media else message.entities) or None

    sent_count = 0
    failed = 0
    delete_at = (int(time.time()) + duration_seconds) if duration_seconds else None
    for uid in all_user_ids:
        try:
            if video_file_id:
                m = bot.send_video(uid, video_file_id, caption=caption_text, caption_entities=caption_entities)
            elif photo_file_id:
                m = bot.send_photo(uid, photo_file_id, caption=caption_text, caption_entities=caption_entities)
            elif voice_file_id:
                m = bot.send_voice(uid, voice_file_id, caption=caption_text, caption_entities=caption_entities)
            elif audio_file_id:
                m = bot.send_audio(uid, audio_file_id, caption=caption_text, caption_entities=caption_entities)
            elif document_file_id:
                m = bot.send_document(uid, document_file_id, caption=caption_text, caption_entities=caption_entities)
            else:
                m = bot.send_message(uid, caption_text, entities=caption_entities)
            # Save to DB (not just memory) so deletion still happens even if the
            # bot process restarts before the timer is up. Permanent announcements
            # (duration_seconds=None) never get inserted, so they're never deleted.
            if delete_at is not None:
                cursor.execute("INSERT INTO scheduled_deletions (chat_id, message_id, delete_at) VALUES (?,?,?)",
                               (uid, m.message_id, delete_at))
            sent_count += 1
        except Exception:
            failed += 1
    conn.commit()
    cursor.close()
    conn.close()

    if delete_at is not None:
        delete_note = "⏳ Ye tay time baad sabki chat se automatically delete ho jaayega (bot restart hone par bhi ye yaad rahega)."
    else:
        delete_note = "♾️ Ye announcement permanent hai — kabhi automatically delete nahi hoga."
    bot.reply_to(message, f"✅ Announcement bhej diya gaya {sent_count} users ko"
                           f"{f' ({failed} ko nahi bheja ja saka)' if failed else ''}.\n"
                           f"{delete_note}")

def add_product(message):
    if _check_cancel(message):
        return
    name = (message.text or "").strip()
    if not name:
        bot.reply_to(message, "❌ Invalid name.")
        return

    entities_json = extract_custom_emoji_json(message)

    def _write(conn):
        cur = conn.cursor()
        cur.execute("SELECT COALESCE(MAX(sort_order),0) FROM products")
        next_order = int(cur.fetchone()[0] or 0) + 10
        cur.execute(
            "INSERT INTO products (name, name_entities, sort_order) VALUES (?,?,?)",
            (name, entities_json, next_order)
        )
        new_id = cur.lastrowid
        cur.close()
        return new_id

    try:
        product_id = sqlite_write_with_retry(_write)
    except Exception as e:
        bot.reply_to(message, f"❌ Product save nahi hua: {e}")
        return

    bot.reply_to(message, f"✅ Product '{name}' added.\n🆔 Product ID: {product_id}")



def add_plan(message):
    """Admin handler for Product Management -> Add Plan.

    Expected input:
        product_id value price [unit]

    Units supported by the existing plan/display system:
        days (default), hours, credits, tokens
    """
    if _check_cancel(message):
        return

    raw = (message.text or "").strip()
    parts = raw.split()

    if len(parts) not in (3, 4):
        bot.reply_to(
            message,
            "❌ Invalid format.\n\n"
            "Use: <code>product_id value price [unit]</code>\n"
            "Example: <code>23 1 25 hours</code>",
            parse_mode="HTML"
        )
        return

    try:
        product_id = int(parts[0])
        duration_value = int(parts[1])
        price = int(parts[2])
    except (TypeError, ValueError):
        bot.reply_to(
            message,
            "❌ Product ID, value aur price numbers hone chahiye.\n"
            "Example: <code>23 1 25 hours</code>",
            parse_mode="HTML"
        )
        return

    if product_id <= 0:
        bot.reply_to(message, "❌ Invalid Product ID.")
        return

    if duration_value <= 0:
        bot.reply_to(message, "❌ Plan value 0 se greater hona chahiye.")
        return

    if price < 0:
        bot.reply_to(message, "❌ Price negative nahi ho sakta.")
        return

    unit = (parts[3] if len(parts) == 4 else "days").strip().lower()
    unit_aliases = {
        "day": "days",
        "days": "days",
        "hour": "hours",
        "hours": "hours",
        "credit": "credits",
        "credits": "credits",
        "token": "tokens",
        "tokens": "tokens",
    }
    unit = unit_aliases.get(unit)

    if unit is None:
        bot.reply_to(
            message,
            "❌ Invalid unit.\n\n"
            "Allowed: <code>days</code>, <code>hours</code>, "
            "<code>credits</code>, <code>tokens</code>.",
            parse_mode="HTML"
        )
        return

    def _write(conn):
        cur = conn.cursor()

        # Verify the selected product exists before inserting the plan.
        cur.execute("SELECT id, name FROM products WHERE id=?", (product_id,))
        product = cur.fetchone()
        if not product:
            cur.close()
            return None, "product_not_found"

        cur.execute(
            "INSERT INTO plans (product_id, days, price, duration_unit) "
            "VALUES (?,?,?,?)",
            (product_id, duration_value, price, unit)
        )
        plan_id = cur.lastrowid
        product_name = product[1]
        cur.close()
        return (plan_id, product_name), None

    try:
        result, error_code = sqlite_write_with_retry(_write)
    except Exception as e:
        print(f"[DB ADD PLAN] {e}")
        bot.reply_to(message, "❌ Plan save nahi hua. Database error aaya hai. Dobara try karo.")
        return

    if error_code == "product_not_found" or not result:
        bot.reply_to(
            message,
            f"❌ Product ID <code>{product_id}</code> nahi mila.\n"
            "Pehle Product Management mein existing Product ID check karo.",
            parse_mode="HTML"
        )
        return

    plan_id, product_name = result
    duration_label = format_duration(duration_value, unit)

    bot.reply_to(
        message,
        f"✅ <b>Plan Added Successfully!</b>\n\n"
        f"📦 Product: <b>{html.escape(str(product_name))}</b>\n"
        f"🆔 Product ID: <code>{product_id}</code>\n"
        f"📅 Duration: <b>{html.escape(duration_label)}</b>\n"
        f"💰 Price: <b>₹{price}</b>\n"
        f"🔢 Plan ID: <code>{plan_id}</code>",
        parse_mode="HTML"
    )

def edit_product_name(message, prod_id):
    if _check_cancel(message):
        return
    name = (message.text or "").strip()
    if not name:
        bot.reply_to(message, "❌ Product name empty nahi ho sakta.")
        return

    def _write(conn):
        cur = conn.cursor()
        cur.execute(
            "UPDATE products SET name=?, name_entities=? WHERE id=?",
            (name, extract_custom_emoji_json(message), int(prod_id))
        )
        changed = cur.rowcount
        cur.close()
        return changed

    try:
        changed = sqlite_write_with_retry(_write)
    except Exception as e:
        bot.reply_to(message, f"❌ Product update nahi hua: {e}")
        return

    if not changed:
        bot.reply_to(message, "❌ Product nahi mila. Shop refresh karke dobara try karo.")
        return

    bot.reply_to(message, f"✅ Product updated to '{name}'.")


def add_balance_admin(message):
    if _check_cancel(message):
        return
    parts = message.text.strip().split()
    if len(parts) != 2:
        bot.reply_to(message, "Format: user_id amount")
        return
    uid, amt = parts
    amt = int(amt)
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET balance = balance + ? WHERE id = ?", (amt, uid))
    if cursor.rowcount == 0:
        cursor.execute("INSERT INTO users (id, balance) VALUES (?,?)", (uid, amt))
    cursor.execute("INSERT INTO transactions (user_id, amount, type, details) VALUES (?,?,?,?)", (uid, amt, "admin_add_balance", f"Admin {message.from_user.id} added balance"))
    conn.commit()
    cursor.close()
    conn.close()
    bot.reply_to(message, f"✅ Added ₹{amt} to {uid}")
    try:
        bot.send_message(int(uid), f"🎉 Admin added ₹{amt} to your balance!")
    except:
        pass

def remove_balance_admin(message):
    if _check_cancel(message):
        return
    parts = message.text.strip().split()
    if len(parts) != 2:
        bot.reply_to(message, "Format: user_id amount")
        return
    uid, amt = parts
    amt = int(amt)
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET balance = balance - ? WHERE id = ? AND balance >= ?", (amt, uid, amt))
    if cursor.rowcount == 0:
        bot.reply_to(message, "❌ User not found or insufficient balance.")
    else:
        cursor.execute("INSERT INTO transactions (user_id, amount, type, details) VALUES (?,?,?,?)", (uid, amt, "admin_remove_balance", f"Admin {message.from_user.id} removed balance"))
        conn.commit()
        bot.reply_to(message, f"✅ Removed ₹{amt} from {uid}")
        try:
            bot.send_message(int(uid), f"⚠️ Admin removed ₹{amt} from your balance.")
        except:
            pass
    cursor.close()
    conn.close()

def ban_user_cmd(message):
    if _check_cancel(message):
        return
    try:
        uid = int(message.text.strip())
        ban_user(uid)
        bot.reply_to(message, f"✅ User {uid} banned.")
        try:
            bot.send_message(uid, "⛔ You have been banned. Contact support.")
        except:
            pass
    except:
        bot.reply_to(message, "❌ Invalid ID.")

def unban_user_cmd(message):
    if _check_cancel(message):
        return
    try:
        uid = int(message.text.strip())
        unban_user(uid)
        bot.reply_to(message, f"✅ User {uid} unbanned.")
        try:
            bot.send_message(uid, "✅ You have been unbanned. Use /start again.")
        except:
            pass
    except:
        bot.reply_to(message, "❌ Invalid ID.")

def _show_referral_admin_settings(chat_id,msg_id):
    enabled="ON" if referral_enabled() else "OFF"; comm="ON" if referral_commission_enabled() else "OFF"
    mk=InlineKeyboardMarkup(row_width=1)
    mk.add(InlineKeyboardButton(f"🎁 Referral: {enabled}",callback_data="admin_ref_toggle",style="success" if referral_enabled() else "danger"))
    mk.add(InlineKeyboardButton(f"💰 Join Bonus: ₹{get_referral_reward()}",callback_data="admin_ref_join",style="success"))
    mk.add(InlineKeyboardButton(f"💸 Commission: {comm} ({get_referral_commission_percent()}%)",callback_data="admin_ref_comm_toggle",style="success" if referral_commission_enabled() else "danger"))
    mk.add(InlineKeyboardButton(f"📌 Min Purchase: ₹{get_referral_min_purchase()}",callback_data="admin_ref_min",style="success"))
    mk.add(InlineKeyboardButton("📈 Set Commission %",callback_data="admin_ref_pct",style="success"))
    mk.add(InlineKeyboardButton("◀️ Back",callback_data="admin_cat_settings",style="primary",icon_custom_emoji_id=btn_emo("admin_cat_settings")))
    bot.edit_message_text("🎁 <b>Referral Settings</b>",chat_id,msg_id,parse_mode="HTML",reply_markup=mk)

def _show_spin_admin_settings(chat_id,msg_id):
    cfg=", ".join(f"₹{r}={p:.0f}%" for r,p in get_spin_config())
    mk=InlineKeyboardMarkup(row_width=1)
    mk.add(InlineKeyboardButton(f"🎡 Daily Spin: {'ON' if spin_enabled() else 'OFF'}",callback_data="admin_spin_toggle",style="success" if spin_enabled() else "danger"))
    mk.add(InlineKeyboardButton("🎯 Set Rewards + Probability",callback_data="admin_spin_prob",style="success"))
    mk.add(InlineKeyboardButton("🎨 Set Spin Animation Emojis",callback_data="admin_spin_animation_emojis",style="success",icon_custom_emoji_id=btn_emo("admin_spin_settings")))
    mk.add(InlineKeyboardButton("◀️ Back",callback_data="admin_cat_settings",style="primary",icon_custom_emoji_id=btn_emo("admin_cat_settings")))
    bot.edit_message_text(
        f"🎡 <b>Spin Settings</b>\n\nCurrent: {html.escape(cfg)}\n\n"
        f"🎨 Spin animation ke 5 frames Premium Emoji Manager se customize kar sakte ho.",
        chat_id,msg_id,parse_mode="HTML",reply_markup=mk
    )

def save_referral_reward(message):
    if _check_cancel(message): return
    try: value=max(0,int(message.text.strip()))
    except: bot.reply_to(message,"❌ Number bhejo, e.g. 10"); return
    set_setting("referral_join_bonus",str(value)); set_setting("referral_reward",str(value))
    user_states.pop(message.from_user.id,None); bot.reply_to(message,f"✅ Referral join bonus set to ₹{value}.")

def save_referral_commission_pct(message):
    if _check_cancel(message): return
    try: value=int(message.text.strip())
    except: bot.reply_to(message,"❌ 0-100 number bhejo, e.g. 5"); return
    if value<0 or value>100: bot.reply_to(message,"❌ Commission 0 se 100% ke beech hona chahiye."); return
    set_setting("referral_commission_percent",str(value)); user_states.pop(message.from_user.id,None); bot.reply_to(message,f"✅ Purchase commission set to {value}%.")

def save_referral_min(message):
    if _check_cancel(message): return
    try: value=max(0,int(message.text.strip()))
    except: bot.reply_to(message,"❌ Number bhejo, e.g. 0"); return
    set_setting("referral_min_purchase",str(value)); user_states.pop(message.from_user.id,None); bot.reply_to(message,f"✅ Minimum purchase set to ₹{value}.")

def save_spin_probabilities(message):
    if _check_cancel(message): return
    raw=message.text.strip(); pairs=[]; total=0.0
    try:
        for part in raw.split(","):
            a,b=part.split(":",1); reward=int(a.strip()); prob=float(b.strip())
            if reward<0 or prob<0: raise ValueError
            pairs.append((reward,prob)); total+=prob
        if not pairs or abs(total-100.0)>0.01: raise ValueError
    except:
        bot.reply_to(message,"❌ Format: <code>0:50,5:30,10:15,20:5</code> — probability total exactly 100 hona chahiye.",parse_mode="HTML"); return
    set_setting("spin_probabilities",",".join(f"{r}:{p:g}" for r,p in pairs))
    set_setting("spin_rewards",",".join(str(r) for r,_ in pairs))
    user_states.pop(message.from_user.id,None); bot.reply_to(message,"✅ Spin rewards + probabilities updated.")

def save_spin_rewards(message):
    # Backward-compatible simple reward list; new probability UI is preferred.
    if _check_cancel(message): return
    try:
        vals=[int(x.strip()) for x in message.text.strip().split(",") if x.strip()]
        if not vals or any(v<0 for v in vals): raise ValueError
    except: bot.reply_to(message,"❌ Format: 0,5,10,20,50"); return
    set_setting("spin_rewards",",".join(map(str,vals))); set_setting("spin_probabilities",",".join(f"{v}:{100/len(vals):g}" for v in vals)); user_states.pop(message.from_user.id,None); bot.reply_to(message,"✅ Spin rewards updated with equal probability.")

def receive_keys(message):
    if _check_cancel(message):
        return
    user_id = message.from_user.id
    state = user_states.get(user_id, {})
    if state.get("action") != "add_keys_final":
        bot.reply_to(message, "Session expired. Start over.")
        return
    prod_id = state.get("prod_id")
    plan_id = state.get("plan_id")
    if not prod_id or not plan_id:
        bot.reply_to(message, "Error: missing data.")
        return
    keys = [k.strip() for k in message.text.split('\n') if k.strip()]
    if not keys:
        bot.reply_to(message, "No keys provided.")
        return
    conn = get_db()
    cursor = conn.cursor()
    inserted = 0
    for k in keys:
        try:
            cursor.execute("INSERT INTO license_keys (product_id, plan_id, license_key) VALUES (?,?,?)", (prod_id, plan_id, k))
            inserted += 1
        except sqlite3.IntegrityError:
            bot.send_message(message.chat.id, f"Duplicate: {k}")
    conn.commit()
    cursor.close()
    conn.close()
    bot.reply_to(message, f"✅ {inserted} keys added.")
    user_states.pop(user_id, None)

def delete_key(message):
    if _check_cancel(message):
        return
    key_str = message.text.strip()
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM license_keys WHERE license_key = ?", (key_str,))
    if cursor.rowcount:
        conn.commit()
        bot.reply_to(message, "✅ Key deleted.")
    else:
        bot.reply_to(message, "❌ Key not found.")
    cursor.close()
    conn.close()

def list_all_keys(call):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT lk.license_key, p.name, pl.days, lk.used, lk.used_by, pl.duration_unit
        FROM license_keys lk
        JOIN products p ON lk.product_id=p.id
        JOIN plans pl ON lk.plan_id=pl.id
        ORDER BY lk.id DESC LIMIT 30
    """)
    keys = cursor.fetchall()
    cursor.close()
    conn.close()
    if not keys:
        bot.edit_message_text("No keys found.", call.message.chat.id, call.message.message_id)
        return
    text = "🔑 <b>Keys</b>\n\n"
    for k in keys:
        status = "✅ Active" if not k[3] else "❌ Used"
        text += f"📦 {html.escape(k[1])} ({format_duration(k[2], k[5])})\n🔑 <code>{html.escape(k[0])}</code>\nStatus: {status}\n"
        if k[3]:
            text += f"👤 Used by: {k[4]}\n"
        text += "\n"
    bot.edit_message_text(text, call.message.chat.id, call.message.message_id, parse_mode="HTML")

def show_expired_used_keys(chat_id, msg_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT lk.license_key, p.name, pl.days, lk.used_by, lk.used_at, pl.duration_unit
        FROM license_keys lk
        JOIN products p ON lk.product_id=p.id
        JOIN plans pl ON lk.plan_id=pl.id
        WHERE lk.used=1
        ORDER BY lk.used_at DESC LIMIT 50
    """)
    keys = cursor.fetchall()
    cursor.close()
    conn.close()
    if not keys:
        bot.edit_message_text("✅ Koi used/expired key nahi hai abhi.", chat_id, msg_id, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton(" Back", callback_data="admin_cat_keys", style="primary", icon_custom_emoji_id=btn_emo("admin_cat_keys"))))
        return
    now = time.time()
    text = "⌛ <b>Used / Expired Keys</b> (latest 50)\n\n"
    for k in keys:
        key_name, prod_name, days, used_by, used_at, duration_unit = k
        is_expired = False
        unit = (duration_unit or "days").lower()
        if unit not in ("credit", "credits", "token", "tokens"):
            seconds_per_unit = 3600 if unit == "hours" else 86400
            try:
                used_ts = datetime.strptime(used_at, "%Y-%m-%d %H:%M:%S").timestamp() if used_at else None
                if used_ts and (now - used_ts) > (days * seconds_per_unit):
                    is_expired = True
            except:
                pass
        status = "🔴 Expired" if is_expired else "🟡 Used (Active)"
        text += f"📦 {html.escape(prod_name)} ({format_duration(days, duration_unit)})\n🔑 <code>{html.escape(key_name)}</code>\n👤 User: {used_by}\nStatus: {status}\n\n"
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton(" Back", callback_data="admin_cat_keys", style="primary", icon_custom_emoji_id=btn_emo("admin_cat_keys")))
    # Telegram messages have a length limit; trim if needed
    if len(text) > 4000:
        text = text[:3950] + "\n\n... (list trimmed)"
    bot.edit_message_text(text, chat_id, msg_id, parse_mode="HTML", reply_markup=markup)

def show_stock_overview(chat_id, msg_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT p.name, pl.days, pl.price, pl.id,
               SUM(CASE WHEN lk.used=0 THEN 1 ELSE 0 END) as available, pl.duration_unit
        FROM plans pl
        JOIN products p ON pl.product_id=p.id
        LEFT JOIN license_keys lk ON lk.plan_id=pl.id
        GROUP BY pl.id
        ORDER BY p.name, (CASE WHEN duration_unit='hours' THEN days ELSE days*24 END)
    """)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    if not rows:
        bot.edit_message_text("⚠️ Koi product/plan nahi hai abhi.", chat_id, msg_id, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton(" Back", callback_data="admin_cat_keys", style="primary", icon_custom_emoji_id=btn_emo("admin_cat_keys"))))
        return
    text = "🎮 <b>Stock Overview</b>\n\n"
    current_product = None
    for r in rows:
        prod_name, days, price, plan_id, available, duration_unit = r
        available = available or 0
        if prod_name != current_product:
            if current_product is not None:
                text += "\n"
            text += f"🎮 <b>{html.escape(prod_name)}</b>\n"
            current_product = prod_name
        text += f"┣ 💊 {format_duration(days, duration_unit).upper()}: {available} keys\n"
    text += "\n"
    if len(text) > 4000:
        text = text[:3950] + "\n\n... (list trimmed)"
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton(" Back", callback_data="admin_cat_keys", style="primary", icon_custom_emoji_id=btn_emo("admin_cat_keys")))
    bot.edit_message_text(text, chat_id, msg_id, parse_mode="HTML", reply_markup=markup)

def show_all_users(chat_id, msg_id, page=0):
    PAGE_SIZE = 15
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM users")
    total = cursor.fetchone()[0]
    cursor.execute("SELECT id, username, phone_number, balance FROM users ORDER BY id LIMIT ? OFFSET ?", (PAGE_SIZE, page * PAGE_SIZE))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    if not rows:
        bot.edit_message_text("👥 Koi users nahi hain abhi.", chat_id, msg_id, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton(" Back", callback_data="admin_cat_user", style="primary", icon_custom_emoji_id=btn_emo("admin_cat_user"))))
        return
    total_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
    text = f"👥 <b>All Users</b> — Total: {total}\nPage {page+1}/{total_pages}\n\n"
    for u in rows:
        uid, username, phone, balance = u
        text += f"🆔 <code>{uid}</code>\n👤 @{username or 'N/A'}\n📱 {phone or 'N/A'}\n💰 ₹{balance}\n\n"
    markup = InlineKeyboardMarkup(row_width=2)
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"admin_all_users_{page-1}", style="danger", icon_custom_emoji_id=btn_emo("admin_all_users")))
    if (page + 1) < total_pages:
        nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"admin_all_users_{page+1}", style="success", icon_custom_emoji_id=btn_emo("admin_all_users")))
    if nav_row:
        markup.add(*nav_row)
    markup.add(InlineKeyboardButton(" Back", callback_data="admin_cat_user", style="primary", icon_custom_emoji_id=btn_emo("admin_cat_user")))
    bot.edit_message_text(text, chat_id, msg_id, parse_mode="HTML", reply_markup=markup)

# ======================= RESELLER ADMIN FUNCTIONS =======================
def reseller_toggle_admin(message):
    if _check_cancel(message):
        return
    try:
        uid = int(message.text.strip())
    except:
        bot.reply_to(message, "❌ Invalid ID. Numeric Telegram ID bhejo.")
        return
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT is_reseller FROM users WHERE id=?", (uid,))
    row = cursor.fetchone()
    if not row:
        # Create the user row if it doesn't exist yet (e.g. admin granting
        # reseller access to someone who hasn't pressed /start yet).
        cursor.execute("INSERT INTO users (id, is_reseller) VALUES (?,1)", (uid,))
        new_status = 1
    else:
        new_status = 0 if row[0] else 1
        cursor.execute("UPDATE users SET is_reseller=?, reseller_banned=0 WHERE id=?", (new_status, uid))
    conn.commit()
    cursor.close()
    conn.close()
    if new_status:
        bot.reply_to(message, f"✅ User {uid} ab RESELLER hai.")
        try:
            bot.send_message(uid, "🏷️ <b>Congratulations!</b>\n\nAapko Reseller access de diya gaya hai — jahan bhi reseller price set hai, wahan ab aapko wahi special price milega.", parse_mode="HTML")
        except:
            pass
    else:
        bot.reply_to(message, f"✅ User {uid} ka RESELLER status hata diya gaya.")
        try:
            bot.send_message(uid, "ℹ️ Aapka reseller access hata diya gaya hai. Ab aapko normal price dikhega.")
        except:
            pass

def reseller_ban_admin(message):
    if _check_cancel(message):
        return
    try:
        uid = int(message.text.strip())
    except:
        bot.reply_to(message, "❌ Invalid ID. Numeric Telegram ID bhejo.")
        return
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT is_reseller FROM users WHERE id=?", (uid,))
    row = cursor.fetchone()
    if not row or not row[0]:
        cursor.close()
        conn.close()
        bot.reply_to(message, f"⚠️ User {uid} reseller nahi hai, ban nahi kar sakte.")
        return
    cursor.execute("UPDATE users SET reseller_banned=1 WHERE id=?", (uid,))
    conn.commit()
    cursor.close()
    conn.close()
    bot.reply_to(message, f"🚫 Reseller {uid} ban kar diya gaya. Ab isko normal price dikhega.")
    try:
        bot.send_message(uid, "🚫 Aapka reseller access temporarily suspend kar diya gaya hai. Ab aapko normal price dikhega. Sawaal ho to support se contact karo.")
    except:
        pass

def reseller_unban_admin(message):
    if _check_cancel(message):
        return
    try:
        uid = int(message.text.strip())
    except:
        bot.reply_to(message, "❌ Invalid ID. Numeric Telegram ID bhejo.")
        return
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT is_reseller FROM users WHERE id=?", (uid,))
    row = cursor.fetchone()
    if not row or not row[0]:
        cursor.close()
        conn.close()
        bot.reply_to(message, f"⚠️ User {uid} reseller nahi hai.")
        return
    cursor.execute("UPDATE users SET reseller_banned=0 WHERE id=?", (uid,))
    conn.commit()
    cursor.close()
    conn.close()
    bot.reply_to(message, f"✅ Reseller {uid} unban kar diya gaya. Ab wapas reseller price milega.")
    try:
        bot.send_message(uid, "✅ Aapka reseller access wapas activate kar diya gaya hai!")
    except:
        pass

def show_reseller_list(chat_id, msg_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id, username, reseller_banned, balance FROM users WHERE is_reseller=1 ORDER BY id LIMIT 100")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton(" Back", callback_data="admin_cat_reseller", style="primary", icon_custom_emoji_id=btn_emo("admin_cat_reseller")))
    if not rows:
        bot.edit_message_text("📋 Koi reseller nahi hai abhi.", chat_id, msg_id, reply_markup=markup)
        return
    text = f"📋 <b>Resellers</b> (Total: {len(rows)})\n\n"
    for r in rows:
        uid, username, banned, balance = r
        status = "🚫 Banned" if banned else "🟢 Active"
        text += f"🆔 <code>{uid}</code>\n👤 @{username or 'N/A'}\n💰 ₹{balance}\nStatus: {status}\n\n"
    if len(text) > 4000:
        text = text[:3950] + "\n\n... (list trimmed)"
    bot.edit_message_text(text, chat_id, msg_id, parse_mode="HTML", reply_markup=markup)

def show_reseller_buy_menu(chat_id, msg_id):
    enabled = get_setting("reseller_buy_enabled") == "1"
    price = get_setting("reseller_buy_price") or "Not set"
    terms = get_setting("reseller_buy_terms") or "Not set (default text will be used)"
    success_msg = get_setting("reseller_buy_success_msg") or "Not set (default text will be used)"
    status = "🟢 ON (users can buy)" if enabled else "🔴 OFF (hidden from users)"
    text = (
        f"🛒 <b>Reseller Buy Settings</b>\n\n"
        f"Status: {status}\n"
        f"Price: ₹{price}\n\n"
        f"<b>Terms/condition text</b> (shown above Buy button):\n{html.escape(str(terms))}\n\n"
        f"<b>Success message</b> (shown after purchase):\n{html.escape(str(success_msg))}"
    )
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(
        InlineKeyboardButton("💰 Set Price", callback_data="admin_reseller_buy_setprice", style="success", icon_custom_emoji_id=btn_emo("admin_reseller_buy_setprice")),
        InlineKeyboardButton("📝 Set Terms Text", callback_data="admin_reseller_buy_setterms", style="success", icon_custom_emoji_id=btn_emo("admin_reseller_buy_setterms")),
        InlineKeyboardButton("🎉 Set Success Message", callback_data="admin_reseller_buy_setsuccessmsg", style="success", icon_custom_emoji_id=btn_emo("admin_reseller_buy_setsuccessmsg")),
        InlineKeyboardButton("🔁 Toggle ON/OFF", callback_data="admin_reseller_buy_toggle", style="success", icon_custom_emoji_id=btn_emo("admin_reseller_buy_toggle")),
        InlineKeyboardButton(" Back", callback_data="admin_cat_reseller", style="primary", icon_custom_emoji_id=btn_emo("admin_cat_reseller")),
    )
    bot.edit_message_text(text, chat_id, msg_id, parse_mode="HTML", reply_markup=markup)

def show_reseller_price_plans(chat_id, msg_id, prod_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id, days, price, duration_unit, reseller_price FROM plans WHERE product_id=? ORDER BY (CASE WHEN duration_unit='hours' THEN days ELSE days*24 END)", (prod_id,))
    plans = cursor.fetchall()
    cursor.close()
    conn.close()
    markup = InlineKeyboardMarkup(row_width=1)
    for pl in plans:
        rp = f"₹{pl[4]}" if pl[4] is not None else "not set"
        markup.add(InlineKeyboardButton(f"💰 {format_duration(pl[1], pl[3])} - Normal ₹{pl[2]} | Reseller: {rp}", callback_data=f"resellerprice_plan_{pl[0]}_{prod_id}", style="success", icon_custom_emoji_id=btn_emo("resellerprice_plan")))
    markup.add(InlineKeyboardButton(" Back", callback_data="admin_reseller_setprice", style="primary", icon_custom_emoji_id=btn_emo("admin_reseller_setprice")))
    bot.edit_message_text("💰 Kis plan ki reseller price set/change karni hai?" if plans else "⚠️ Is product ke liye koi plan nahi hai.", chat_id, msg_id, reply_markup=markup)

def save_reseller_price(message):
    if _check_cancel(message):
        return
    user_id = message.from_user.id
    state = user_states.get(user_id, {})
    plan_id = state.get("plan_id")
    prod_id = state.get("prod_id")
    if not plan_id:
        return
    raw = message.text.strip().lower()
    if raw in ("clear", "remove", "-1", "none"):
        new_price = None
    else:
        try:
            new_price = int(raw)
            if new_price < 0:
                raise ValueError
        except:
            bot.reply_to(message, "❌ Invalid price. Number bhejo (jaise 299), ya 'clear' bhejo reseller price hatane ke liye.")
            return
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("UPDATE plans SET reseller_price=? WHERE id=?", (new_price, plan_id))
    conn.commit()
    cursor.close()
    conn.close()
    user_states.pop(user_id, None)
    if new_price is None:
        bot.reply_to(message, "✅ Reseller price hata di gayi. Ab resellers ko bhi normal price dikhega.")
    else:
        bot.reply_to(message, f"✅ Reseller price set: ₹{new_price}")
    if prod_id:
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton(" Back to Plans", callback_data=f"resellerprice_prod_{prod_id}", style="primary", icon_custom_emoji_id=btn_emo("resellerprice_prod")))
        bot.send_message(message.chat.id, "Done!", reply_markup=markup)

def save_order_notify_template(message):
    if _check_cancel(message):
        return
    text = message.text.strip() if message.text else ""
    if not text:
        bot.reply_to(message, "❌ Empty text not allowed.")
        return
    if text.lower() in ("reset", "clear", "default"):
        set_setting("order_notify_template", "")
        set_setting("order_notify_template_entities", "")
        bot.reply_to(message, "✅ Default Order Notify text restore kar diya gaya.")
        return
    entities_json = extract_custom_emoji_json(message)
    set_setting("order_notify_template", text)
    set_setting("order_notify_template_entities", entities_json or "")
    bot.reply_to(message, "✅ Order Notify text saved! Agla purchase se ye naya text use hoga.")

def save_support_text(message):
    if _check_cancel(message):
        return
    text = message.text.strip() if message.text else ""
    if not text:
        bot.reply_to(message, "❌ Empty text not allowed.")
        return
    if text.lower() in ("reset", "clear", "default"):
        set_setting("support_text_template", "")
        set_setting("support_text_template_entities", "")
        bot.reply_to(message, "✅ Default Support text restore kar diya gaya.")
        return
    entities_json = extract_custom_emoji_json(message)
    set_setting("support_text_template", text)
    set_setting("support_text_template_entities", entities_json or "")
    bot.reply_to(message, "✅ Support text saved! Ab 'Support' button dabane par ye naya text dikhega.")

def save_store_highlights_text(message):
    if _check_cancel(message):
        return
    text = message.text.strip() if message.text else ""
    if not text:
        bot.reply_to(message, "❌ Empty text not allowed.")
        return
    if text.lower() in ("reset", "clear", "default"):
        set_setting("store_highlights_template", "")
        set_setting("store_highlights_template_entities", "")
        bot.reply_to(message, "✅ Default Store Highlights text restore kar diya gaya.")
        return
    entities_json = extract_custom_emoji_json(message)
    set_setting("store_highlights_template", text)
    set_setting("store_highlights_template_entities", entities_json or "")
    bot.reply_to(message, "✅ Store Highlights text saved! Ab main menu par ye naya text dikhega.")

def save_reseller_buy_price(message):
    if _check_cancel(message):
        return
    try:
        price = int(message.text.strip())
        if price < 0:
            raise ValueError
    except:
        bot.reply_to(message, "❌ Invalid price. Sirf number bhejo (jaise 400).")
        return
    set_setting("reseller_buy_price", str(price))
    bot.reply_to(message, f"✅ Reseller buy price set: ₹{price}")

def save_reseller_buy_terms(message):
    if _check_cancel(message):
        return
    text = message.text.strip() if message.text else ""
    if not text:
        bot.reply_to(message, "❌ Empty text not allowed.")
        return
    set_setting("reseller_buy_terms", text)
    bot.reply_to(message, "✅ Terms text saved!")

def save_reseller_buy_success_msg(message):
    if _check_cancel(message):
        return
    text = message.text.strip() if message.text else ""
    if not text:
        bot.reply_to(message, "❌ Empty text not allowed.")
        return
    set_setting("reseller_buy_success_msg", text)
    bot.reply_to(message, "✅ Success message saved!")

# ======================= MAIN =======================
if __name__ == "__main__":
    # One bot process per database. This prevents accidentally running two copies
    # of the bot against the same SQLite DB (the main cause of the screenshot's
    # repeated "database is locked" errors).
    try:
        import fcntl
        _instance_lock = open(INSTANCE_LOCK_PATH, "a+")
        fcntl.flock(_instance_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        _instance_lock.seek(0)
        _instance_lock.truncate()
        _instance_lock.write(str(os.getpid()))
        _instance_lock.flush()
    except (BlockingIOError, OSError):
        print("❌ Another Vetlam bot instance is already running with the SAME authoritative DB.")
        print(f"   DB: {DB_PATH}")
        print("   Stop the old bot process before starting this one.")
        raise SystemExit(1)

    # Safe first-run migration from the old main.py directory, only when the
    # authoritative DB does not exist. Never overwrite a live/new DB.
    try:
        if DB_PATH != LEGACY_DB_PATH and not os.path.exists(DB_PATH) and os.path.isfile(LEGACY_DB_PATH):
            shutil.copy2(LEGACY_DB_PATH, DB_PATH)
            print(f"📦 Migrated legacy DB -> authoritative DB: {LEGACY_DB_PATH} -> {DB_PATH}")
    except Exception as e:
        print(f"⚠️ Legacy DB migration skipped: {e}")

    # If DB is missing/fresh but a backup exists on phone storage, auto-restore it.
    # This protects against Termux app data being wiped (common on MIUI/Xiaomi phones).
    # IMPORTANT: the backup is only restored if it has the EXACT SAME filename as the
    # current DB_PATH. This means:
    #   - Same DB filename as before  -> matching backup found -> old data restored.
    #   - DB_PATH renamed (new setup) -> no matching backup     -> starts 100% fresh,
    #     never pulls in an unrelated/old backup from a different-named DB.
    # Auto-restore-from-backup disabled: it was overwriting fresh/new DB data with
    # old backup data whenever the host restarted the process (backup only ran
    # every 15 min, so anything added after the last backup would get wiped out
    # and replaced by the older backup copy). DB is left exactly as-is now.
    init_db()

    # Startup diagnostics: the hosting log now shows the exact live DB path.
    try:
        _db_size = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0
        print(f"🗄️ AUTHORITATIVE DB: {os.path.realpath(DB_PATH)}")
        print(f"📦 DB SIZE: {_db_size} bytes")
        print(f"🔒 DB LOCK: {INSTANCE_LOCK_PATH}")
        print("✅ Product/plan/settings/user data is read from and written to this DB.")
    except Exception as e:
        print(f"DB diagnostic error: {e}")

    # Set the Telegram native "Menu" button (bottom-left, next to message box) to show
    # a single command: /start -> "Open shop". Users can tap Menu -> this option anytime
    # to restart the bot, without typing /start manually.
    try:
        bot.set_my_commands([telebot.types.BotCommand("start", "Open shop")])
    except Exception as e:
        print("set_my_commands error:", e)
    # One-time cleanup: remove old timestamped backup_*.db files (we now keep only latest.db)
    try:
        if os.path.isdir(BACKUP_DIR):
            for fname in os.listdir(BACKUP_DIR):
                if fname.startswith("backup_") and fname.endswith(".db"):
                    try:
                        os.remove(os.path.join(BACKUP_DIR, fname))
                    except:
                        pass
    except Exception as e:
        print("Old backup cleanup error:", e)
    threading.Thread(target=expire_old_pending_payments, daemon=True).start()
    threading.Thread(target=process_scheduled_deletions, daemon=True).start()
    # auto_backup_loop disabled per request -- no backups taken, so no old data can ever come back.
    threading.Thread(target=reconcile_zapupi_payments, daemon=True).start()
    threading.Thread(target=run_webhook_server, daemon=True).start()
    print(f"🌐 Webhook server listening on port {WEBHOOK_LISTEN_PORT} (ZapUPI)...")
    print("🚀 Bot is running with automatic UPI (ZapUPI) payment system...")
    while True:
        try:
            bot.infinity_polling(timeout=30, long_polling_timeout=30)
        except Exception as e:
            print(f"Polling error: {e}")
            time.sleep(5)
