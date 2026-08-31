#!/usr/bin/env python3
"""
Personal Bot Hosting Panel
Single-user Telegram panel to host, run and monitor your own bots.

Only OWNER_ID can interact. Every other update is dropped.

Environment:
    BOT_TOKEN    (required)  token from @BotFather
    OWNER_ID     (optional)  your numeric Telegram id; first /start claims it
    MASTER_KEY   (optional)  Fernet key for backup encryption; generated if absent
    PORT         (optional)  keepalive HTTP port, default 10000
"""

from __future__ import annotations

import base64
import hashlib
import importlib
import io
import json
import os
import re
import secrets
import shutil
import signal
import string
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import traceback
import zipfile
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple

# ═════════════════════════════════════════════════════════════════
#  0. DEPENDENCY BOOTSTRAP
# ═════════════════════════════════════════════════════════════════

_REQUIRED = [
    ("telebot",             "pyTelegramBotAPI"),
    ("requests",            "requests"),
    ("cryptography.fernet", "cryptography"),
    ("flask",               "flask"),
    ("psutil",              "psutil"),
]


def _auto_install() -> None:
    missing = []
    for mod, pip_name in _REQUIRED:
        try:
            importlib.import_module(mod)
        except ImportError:
            missing.append(pip_name)
    if not missing:
        return
    print(f"[setup] installing: {', '.join(missing)}", flush=True)
    for extra in ([], ["--break-system-packages"], ["--user"],
                  ["--user", "--break-system-packages"]):
        try:
            subprocess.run(
                [sys.executable, "-m", "pip", "install", "--upgrade", "--quiet",
                 *extra, *missing],
                check=True,
            )
            return
        except Exception:
            continue
    sys.exit(f"[x] install failed. Run: pip install {' '.join(missing)}")


_auto_install()

import telebot                                    # noqa: E402
from telebot import types                         # noqa: E402
from telebot.apihelper import ApiTelegramException  # noqa: E402
import requests                                   # noqa: E402
from cryptography.fernet import Fernet, InvalidToken  # noqa: E402
from flask import Flask, jsonify                  # noqa: E402

try:
    import psutil
except ImportError:
    psutil = None  # type: ignore


# ═════════════════════════════════════════════════════════════════
#  1. CONFIG
# ═════════════════════════════════════════════════════════════════

BASE_DIR = Path(__file__).resolve().parent

DIRS: Dict[str, Path] = {
    "data":    BASE_DIR / "storage" / "data",
    "logs":    BASE_DIR / "storage" / "logs",
    "exports": BASE_DIR / "storage" / "exports",
    "sandbox": BASE_DIR / "sandbox",
    "tmp":     BASE_DIR / "storage" / "tmp",
}
for _p in DIRS.values():
    _p.mkdir(parents=True, exist_ok=True)

DB_FILE       = DIRS["data"] / "db.json"
SETTINGS_FILE = DIRS["data"] / "settings.json"
AUDIT_FILE    = DIRS["data"] / "audit.log"
MASTER_KEY_FILE = DIRS["data"] / "master.key"

TOKEN = (os.environ.get("BOT_TOKEN") or "").strip()
if not TOKEN:
    sys.exit("[x] BOT_TOKEN env var is required.")

try:
    OWNER_ID = int(os.environ.get("OWNER_ID", "0") or 0)
except ValueError:
    OWNER_ID = 0

try:
    KEEPALIVE_PORT = int(os.environ.get("PORT", 10000))
except ValueError:
    KEEPALIVE_PORT = 10000

BRAND       = "Personal Host Panel"
BRAND_VER   = "v3.0"
BRAND_TAG   = f"{BRAND} {BRAND_VER}"
FOOTER      = f"\n\n<i>{BRAND_TAG}</i>"

LOG_RING          = 400
MAX_LOG_LINES     = 60
MAX_UPLOAD_BYTES  = 100 * 1024 * 1024
DEFAULT_MAX_BOTS  = 25

ENTRY_PY   = ("bot.py", "main.py", "app.py", "run.py", "start.py")
ENTRY_NODE = ("index.js", "bot.js", "main.js", "app.js", "server.js")

# Env names never leaked to child processes.
SECRET_ENV_NAMES = {
    "BOT_TOKEN", "OWNER_ID", "MASTER_KEY",
    "GITHUB_TOKEN", "GH_TOKEN",
}

G = {
    "ok": "✓", "no": "✘", "warn": "⚠", "arrow": "→", "bullet": "•",
    "play": "▶", "stop": "■", "refresh": "↻", "back": "◀",
    "diamond": "◆", "star": "★", "spark": "✦", "plus": "⊕",
    "key": "❖", "lock": "▣", "shield": "◇", "eye": "◉",
    "folder": "▸", "upload": "▴", "download": "▾", "cloud": "☁",
    "settings": "⚙", "bolt": "⚡", "clock": "⏱", "graph": "▪",
    "div": "━" * 16, "div_eq": "═" * 16,
}


# ═════════════════════════════════════════════════════════════════
#  2. JSON STORE  (atomic + mtime cache)
# ═════════════════════════════════════════════════════════════════

_db_lock = threading.RLock()
_CACHE: Dict[str, Tuple[float, Any]] = {}


def _atomic_write(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, default=str, ensure_ascii=False),
                   encoding="utf-8")
    try:
        tmp.replace(path)
    except OSError:
        shutil.copyfile(str(tmp), str(path))
        tmp.unlink(missing_ok=True)


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        try:
            path.replace(path.with_suffix(".corrupt"))
        except Exception:
            pass
        return default


def _cached(path: Path, default: Any) -> Any:
    key = str(path)
    try:
        mtime = path.stat().st_mtime
    except FileNotFoundError:
        mtime = 0.0
    hit = _CACHE.get(key)
    if hit and hit[0] == mtime:
        return hit[1]
    data = _load_json(path, default)
    _CACHE[key] = (mtime, data)
    return data


def db_load() -> Dict[str, Any]:
    with _db_lock:
        d = json.loads(json.dumps(_cached(DB_FILE, {})))  # deep copy
    d.setdefault("bots", {})
    d.setdefault("audit", [])
    return d


def db_load_ro() -> Dict[str, Any]:
    """Read-only. Never mutate the result."""
    with _db_lock:
        d = _cached(DB_FILE, {})
    d.setdefault("bots", {})
    d.setdefault("audit", [])
    return d


def db_save(d: Dict[str, Any]) -> None:
    with _db_lock:
        _atomic_write(DB_FILE, d)
        _CACHE.pop(str(DB_FILE), None)


def settings_load() -> Dict[str, Any]:
    with _db_lock:
        return json.loads(json.dumps(_cached(SETTINGS_FILE, {})))


def settings_save(d: Dict[str, Any]) -> None:
    with _db_lock:
        _atomic_write(SETTINGS_FILE, d)
        _CACHE.pop(str(SETTINGS_FILE), None)


def get_setting(key: str, default: Any = None) -> Any:
    with _db_lock:
        return _cached(SETTINGS_FILE, {}).get(key, default)


def set_setting(key: str, value: Any) -> None:
    s = settings_load()
    s[key] = value
    settings_save(s)


def cache_clear() -> None:
    with _db_lock:
        _CACHE.clear()


# ═════════════════════════════════════════════════════════════════
#  3. UTILITIES
# ═════════════════════════════════════════════════════════════════

def esc(s: Any = "") -> str:
    return (str(s).replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def ts_iso() -> str:
    return now_utc().isoformat()


def stamp() -> str:
    return now_utc().strftime("%Y%m%d_%H%M%S")


def safe_name(s: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9_.-]+", "_", s or "").strip("._")
    return (s or "bot")[:48]


def fmt_bytes(n: float) -> str:
    n = float(n or 0)
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def fmt_dur(ms: int) -> str:
    if ms is None or ms < 0:
        return "—"
    s = int(ms) // 1000
    d, s = divmod(s, 86400)
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    parts = []
    if d:
        parts.append(f"{d}d")
    if h:
        parts.append(f"{h}h")
    if m:
        parts.append(f"{m}m")
    parts.append(f"{s}s")
    return " ".join(parts)


def fmt_ts(iso: Optional[str]) -> str:
    if not iso:
        return "—"
    try:
        dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return str(iso)[:19]


def rmrf(p: Any) -> None:
    try:
        shutil.rmtree(str(p), ignore_errors=True)
    except Exception:
        pass


def rand_id(n: int = 6) -> str:
    return secrets.token_hex(n)


def safe_join(root: Path, *parts: str) -> Path:
    """Path-traversal safe join. Raises ValueError on escape."""
    final = (root / Path(*parts)).resolve()
    rootp = root.resolve()
    if rootp != final and rootp not in final.parents:
        raise ValueError("path traversal detected")
    return final


def dir_size(path: Path) -> int:
    total = 0
    try:
        for root, _, files in os.walk(path):
            for f in files:
                try:
                    total += (Path(root) / f).stat().st_size
                except OSError:
                    pass
    except Exception:
        pass
    return total


def bullet(label: str, value: Any) -> str:
    return f"{G['bullet']} <b>{esc(label)}</b>: <code>{esc(value)}</code>"


def is_owner(uid: int) -> bool:
    return OWNER_ID > 0 and int(uid) == OWNER_ID


def max_bots() -> int:
    try:
        return max(1, int(get_setting("max_bots", DEFAULT_MAX_BOTS)))
    except Exception:
        return DEFAULT_MAX_BOTS


def audit(action: str, detail: str = "") -> None:
    line = f"[{ts_iso()}] {action} {detail}\n"
    try:
        with AUDIT_FILE.open("a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass
    with _db_lock:
        d = db_load()
        d["audit"].append({"ts": ts_iso(), "action": action, "detail": detail})
        d["audit"] = d["audit"][-300:]
        db_save(d)


# ═════════════════════════════════════════════════════════════════
#  4. BACKUP ENCRYPTION  (single master key)
# ═════════════════════════════════════════════════════════════════

def _ensure_master_key() -> bytes:
    """Return the Fernet master key. Order: env var, key file, generate."""
    env_key = (os.environ.get("MASTER_KEY") or "").strip()
    if env_key:
        try:
            Fernet(env_key.encode())
            return env_key.encode()
        except Exception:
            print("[key] MASTER_KEY env var is not a valid Fernet key — ignoring",
                  file=sys.stderr, flush=True)
    if MASTER_KEY_FILE.exists():
        raw = MASTER_KEY_FILE.read_bytes().strip()
        try:
            Fernet(raw)
            return raw
        except Exception:
            print("[key] master.key is corrupt — regenerating", file=sys.stderr)
    key = Fernet.generate_key()
    MASTER_KEY_FILE.write_bytes(key)
    try:
        MASTER_KEY_FILE.chmod(0o600)
    except Exception:
        pass
    print("[key] new master key generated", flush=True)
    return key


MASTER_KEY = _ensure_master_key()


def enc_bytes(data: bytes) -> bytes:
    return Fernet(MASTER_KEY).encrypt(data)


def dec_bytes(blob: bytes) -> bytes:
    return Fernet(MASTER_KEY).decrypt(blob)


# ═════════════════════════════════════════════════════════════════
#  5. BOT INSTANCE + PERSONAL-USE LOCK
# ═════════════════════════════════════════════════════════════════

bot = telebot.TeleBot(TOKEN, parse_mode="HTML", threaded=True, num_threads=4)

USER_STATES: Dict[int, Dict[str, Any]] = {}
START_TS = int(time.time() * 1000)

_orig_process = bot.process_new_updates


def _sender_id(u: Any) -> Optional[int]:
    for attr in ("message", "edited_message", "channel_post",
                 "edited_channel_post", "callback_query", "inline_query",
                 "my_chat_member", "chat_member"):
        obj = getattr(u, attr, None)
        if obj is not None:
            frm = getattr(obj, "from_user", None)
            if frm is not None:
                return frm.id
    return None


def _personal_process(updates: Any) -> None:
    """Drop every update that is not from the owner.

    While OWNER_ID is unset (fresh deploy) updates pass through so the
    first /start can claim ownership.
    """
    allowed = []
    for u in updates:
        uid = _sender_id(u)
        if OWNER_ID <= 0 or uid is None or uid == OWNER_ID:
            allowed.append(u)
            continue
        try:
            if getattr(u, "message", None) is not None:
                bot.send_message(u.message.chat.id,
                                 f"{G['lock']} This bot is private.")
            elif getattr(u, "callback_query", None) is not None:
                bot.answer_callback_query(u.callback_query.id,
                                          "This bot is private.", show_alert=True)
        except Exception:
            pass
    if allowed:
        _orig_process(allowed)


bot.process_new_updates = _personal_process


class Btn(types.InlineKeyboardButton):
    """InlineKeyboardButton with optional Bot API 9.4 style field."""

    def __init__(self, *args: Any, style: str = "", **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        if style:
            self.style = style  # type: ignore[attr-defined]

    def to_dict(self) -> Dict[str, Any]:
        d = super().to_dict()
        if getattr(self, "style", ""):
            d["style"] = self.style
        return d


# ═════════════════════════════════════════════════════════════════
#  6. KEEPALIVE HTTP
# ═════════════════════════════════════════════════════════════════

_ka = Flask(__name__)


@_ka.route("/")
def _ka_root() -> Any:
    return jsonify({
        "ok": True,
        "brand": BRAND_TAG,
        "uptime_ms": int(time.time() * 1000) - START_TS,
        "running_bots": sum(1 for x in RUNNING.values()
                            if x["proc"].poll() is None),
    })


@_ka.route("/health")
def _ka_health() -> Any:
    return jsonify({"status": "alive"})


def _start_keepalive() -> None:
    def _run() -> None:
        try:
            _ka.run(host="0.0.0.0", port=KEEPALIVE_PORT,
                    debug=False, use_reloader=False)
        except Exception as e:
            print(f"[keepalive] {e}", flush=True)

    threading.Thread(target=_run, daemon=True, name="keepalive").start()


# ═════════════════════════════════════════════════════════════════
#  7. UI HELPERS
# ═════════════════════════════════════════════════════════════════

_TAG_RE = re.compile(r"<(/?)([a-zA-Z][a-zA-Z0-9]*)(\s[^>]*)?>")


def _html_truncate(s: str, limit: int = 4000) -> str:
    """Cut without leaving an unclosed tag."""
    if len(s) <= limit:
        return s
    cut = s[:limit - 1]
    if cut.rfind("<") > cut.rfind(">"):
        cut = cut[:cut.rfind("<")]
    stack: List[str] = []
    for m in _TAG_RE.finditer(cut):
        closing, name = m.group(1), m.group(2).lower()
        if closing:
            if stack and stack[-1] == name:
                stack.pop()
        else:
            stack.append(name)
    return cut + "…" + "".join(f"</{t}>" for t in reversed(stack))


def ack(call: types.CallbackQuery, text: str = "") -> None:
    try:
        bot.answer_callback_query(call.id, text=text)
    except Exception:
        pass


def show(chat_id: int, text: str,
         kb: Optional[types.InlineKeyboardMarkup] = None,
         call: Optional[types.CallbackQuery] = None) -> None:
    """Edit in place when triggered by a callback, otherwise send."""
    text = _html_truncate(text, 4000)
    if call and call.message:
        try:
            bot.edit_message_text(text, chat_id=chat_id,
                                  message_id=call.message.message_id,
                                  reply_markup=kb, parse_mode="HTML",
                                  disable_web_page_preview=True)
            return
        except ApiTelegramException as e:
            if "message is not modified" in str(e).lower():
                return
        except Exception:
            pass
    try:
        bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=kb,
                         disable_web_page_preview=True)
    except Exception:
        plain = re.sub(r"<[^>]+>", "", text)
        try:
            bot.send_message(chat_id, plain or "…", reply_markup=kb,
                             disable_web_page_preview=True)
        except Exception as e:
            print(f"[show] {e}", file=sys.stderr, flush=True)


def notify(html: str) -> None:
    if OWNER_ID <= 0:
        return
    try:
        bot.send_message(OWNER_ID, html, parse_mode="HTML",
                         disable_web_page_preview=True)
    except Exception as e:
        print(f"[notify] {e}", flush=True)


# ─── keyboards ────────────────────────────────────────────────────

def kb_back(target: str = "menu_main", label: str = "Back"
            ) -> types.InlineKeyboardMarkup:
    return types.InlineKeyboardMarkup().add(
        Btn(f"{G['back']}  {label}", callback_data=target, style="primary"))


def kb_main() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        Btn(f"{G['diamond']}  My Bots", callback_data="menu_bots", style="primary"),
        Btn(f"{G['upload']}  Upload",   callback_data="menu_upload", style="primary"),
    )
    kb.add(
        Btn(f"{G['cloud']}  From GitHub", callback_data="menu_gh", style="primary"),
        Btn(f"{G['graph']}  Monitor",     callback_data="menu_monitor", style="primary"),
    )
    kb.add(
        Btn(f"{G['settings']}  Settings", callback_data="menu_settings", style="primary"),
        Btn(f"{G['eye']}  Help",          callback_data="menu_help", style="primary"),
    )
    return kb


def kb_bot(bot_id: str, running: bool) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    if running:
        kb.add(
            Btn(f"{G['stop']}  Stop",    callback_data=f"b_stop_{bot_id}", style="danger"),
            Btn(f"{G['refresh']}  Restart", callback_data=f"b_rst_{bot_id}", style="success"),
        )
    else:
        kb.add(
            Btn(f"{G['play']}  Start",   callback_data=f"b_start_{bot_id}", style="success"),
            Btn(f"{G['refresh']}  Restart", callback_data=f"b_rst_{bot_id}", style="primary"),
        )
    kb.add(
        Btn(f"{G['bolt']}  Logs",     callback_data=f"b_logs_{bot_id}", style="primary"),
        Btn(f"{G['key']}  Env Vars",  callback_data=f"b_env_{bot_id}", style="primary"),
    )
    kb.add(
        Btn(f"{G['download']}  Install Pkg", callback_data=f"b_pip_{bot_id}", style="primary"),
        Btn(f"{G['clock']}  Cron",           callback_data=f"b_cron_{bot_id}", style="primary"),
    )
    kb.add(
        Btn(f"{G['plus']}  Add File",  callback_data=f"b_add_{bot_id}", style="primary"),
        Btn(f"{G['arrow']}  Download", callback_data=f"b_dl_{bot_id}", style="primary"),
    )
    kb.add(
        Btn(f"{G['folder']}  Files",   callback_data=f"b_files_{bot_id}", style="primary"),
        Btn(f"{G['no']}  Delete",      callback_data=f"b_del_{bot_id}", style="danger"),
    )
    kb.add(Btn(f"{G['back']}  My Bots", callback_data="menu_bots", style="primary"))
    return kb


def kb_confirm(yes_cb: str, no_cb: str,
               yes: str = "Confirm", no: str = "Cancel"
               ) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        Btn(f"{G['ok']}  {yes}", callback_data=yes_cb, style="danger"),
        Btn(f"{G['no']}  {no}",  callback_data=no_cb,  style="primary"),
    )
    return kb


# ═════════════════════════════════════════════════════════════════
#  8. BOT RECORDS
# ═════════════════════════════════════════════════════════════════

def list_bots() -> List[Dict[str, Any]]:
    return sorted(
        (json.loads(json.dumps(b)) for b in db_load_ro()["bots"].values()),
        key=lambda x: x.get("name", ""),
    )


def find_bot(bot_id: str) -> Optional[Dict[str, Any]]:
    b = db_load_ro()["bots"].get(bot_id)
    return json.loads(json.dumps(b)) if b is not None else None


def save_bot(doc: Dict[str, Any]) -> Dict[str, Any]:
    d = db_load()
    d["bots"][doc["_id"]] = doc
    db_save(d)
    return doc


def delete_bot_doc(bot_id: str) -> None:
    d = db_load()
    d["bots"].pop(bot_id, None)
    db_save(d)


def new_bot_doc(name: str) -> Dict[str, Any]:
    bot_id = rand_id()
    bot_dir = DIRS["sandbox"] / bot_id
    bot_dir.mkdir(parents=True, exist_ok=True)
    return {
        "_id": bot_id,
        "name": safe_name(name),
        "dir": str(bot_dir),
        "created": ts_iso(),
        "env": {},
        "cron": {},
        "status": "stopped",
        "source": "upload",
        "last_error": "",
        "last_exit_code": None,
        "last_started": None,
        "crash_count": 0,
    }


# ═════════════════════════════════════════════════════════════════
#  9. RUNNER
# ═════════════════════════════════════════════════════════════════

RUNNING: Dict[str, Dict[str, Any]] = {}
_runner_lock = threading.Lock()
START_TIME = time.time()
_LOCK_FH: Any = None

_SKIP_DIRS = {".deps", "node_modules", ".tmp_run", "__pycache__",
              ".git", "venv", ".venv", "env"}


def _iter_files(bot_dir: Path, suffix: str) -> List[Path]:
    out = []
    for p in bot_dir.rglob(f"*{suffix}"):
        if any(part in _SKIP_DIRS for part in p.parts):
            continue
        out.append(p)
    return sorted(out, key=lambda x: (len(x.parts), str(x)))


def detect_entry(bot_dir: Path) -> Tuple[Optional[str], Optional[str]]:
    """Return (kind, path relative to bot_dir)."""
    for n in ENTRY_NODE:
        if (bot_dir / n).exists():
            return "node", n
    for n in ENTRY_PY:
        if (bot_dir / n).exists():
            return "python", n
    for n in ENTRY_PY:
        for p in _iter_files(bot_dir, ".py"):
            if p.name == n:
                return "python", str(p.relative_to(bot_dir))
    for n in ENTRY_NODE:
        for p in _iter_files(bot_dir, ".js"):
            if p.name == n:
                return "node", str(p.relative_to(bot_dir))
    py = _iter_files(bot_dir, ".py")
    if py:
        return "python", str(py[0].relative_to(bot_dir))
    js = _iter_files(bot_dir, ".js")
    if js:
        return "node", str(js[0].relative_to(bot_dir))
    return None, None


def safe_env(bot_dir: Path, extra: Optional[Dict[str, str]] = None
             ) -> Dict[str, str]:
    env = {k: v for k, v in os.environ.items() if k not in SECRET_ENV_NAMES}
    env["HOME"] = str(bot_dir)
    env["TMPDIR"] = str(bot_dir / ".tmp_run")
    env.setdefault("PATH", "/usr/local/bin:/usr/bin:/bin")
    env.setdefault("NODE_ENV", "production")
    env.setdefault("PYTHONUNBUFFERED", "1")
    deps = str(bot_dir / ".deps")
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{deps}:{existing}" if existing else deps
    Path(env["TMPDIR"]).mkdir(parents=True, exist_ok=True)
    Path(deps).mkdir(parents=True, exist_ok=True)
    for k, v in (extra or {}).items():
        if k in SECRET_ENV_NAMES:
            continue
        env[str(k)] = str(v)
    return env


# import name → PyPI package name
_PYPI_ALIAS: Dict[str, str] = {
    "telebot": "pyTelegramBotAPI", "telegram": "python-telegram-bot",
    "telethon": "Telethon", "pyrogram": "Pyrogram", "tgcrypto": "TgCrypto",
    "PIL": "Pillow", "cv2": "opencv-python", "bs4": "beautifulsoup4",
    "yaml": "PyYAML", "dotenv": "python-dotenv", "Crypto": "pycryptodome",
    "Cryptodome": "pycryptodomex", "dateutil": "python-dateutil",
    "magic": "python-magic", "skimage": "scikit-image",
    "sklearn": "scikit-learn", "google": "google-api-python-client",
    "OpenSSL": "pyOpenSSL", "psycopg2": "psycopg2-binary",
    "MySQLdb": "mysqlclient", "serial": "pyserial", "win32api": "pywin32",
    "discord": "discord.py", "apscheduler": "APScheduler",
    "github": "PyGithub", "nacl": "PyNaCl", "git": "GitPython",
    "jose": "python-jose", "pkg_resources": "setuptools",
    "attr": "attrs", "jwt": "PyJWT", "redis": "redis",
}

# modules whose installed copy must expose these symbols to count as present
_VALIDATE: Dict[str, List[str]] = {"telegram": ["Update", "Bot"]}

_PIP_FLAGS = ["--upgrade", "--no-input", "--no-warn-script-location",
              "--disable-pip-version-check"]


def _pip_env() -> Dict[str, str]:
    env = {**os.environ,
           "PIP_DISABLE_PIP_VERSION_CHECK": "1",
           "PIP_NO_INPUT": "1",
           "PIP_ROOT_USER_ACTION": "ignore"}
    env.pop("PYTHONUSERBASE", None)
    env.pop("PIP_USER", None)
    return env


def _purge_bad_install(deps_dir: Path, mod: str) -> None:
    """Remove a wrong-package install so pip can put the right one in."""
    try:
        target = deps_dir / mod
        if target.exists():
            rmrf(target)
        for child in list(deps_dir.iterdir()):
            n = child.name.lower()
            if n.startswith(mod.lower()) and n.endswith((".dist-info", ".egg-info")):
                rmrf(child)
    except Exception as e:
        print(f"[purge] {mod}: {e}", file=sys.stderr, flush=True)


def _scan_imports(bot_dir: Path) -> List[str]:
    import ast as _ast
    found: set = set()
    for pyfile in bot_dir.rglob("*.py"):
        if any(part in _SKIP_DIRS for part in pyfile.parts):
            continue
        try:
            tree = _ast.parse(pyfile.read_text(errors="ignore"))
        except Exception:
            continue
        for node in _ast.walk(tree):
            if isinstance(node, _ast.Import):
                for n in node.names:
                    if n.name:
                        found.add(n.name.split(".")[0])
            elif isinstance(node, _ast.ImportFrom):
                if node.level:
                    continue
                if node.module:
                    found.add(node.module.split(".")[0])
    return sorted(found)


def _third_party(modules: List[str], bot_dir: Path) -> List[str]:
    import importlib.util as _ilu
    stdlib = set(getattr(sys, "stdlib_module_names", set()))
    skip = stdlib | {"__future__", ""}
    deps_dir = bot_dir / ".deps"
    for child in bot_dir.iterdir():
        if child == deps_dir:
            continue
        if child.suffix == ".py":
            skip.add(child.stem)
        elif child.is_dir() and (child / "__init__.py").exists():
            skip.add(child.name)

    deps_str = str(deps_dir)
    added = deps_dir.exists() and deps_str not in sys.path
    if added:
        sys.path.insert(0, deps_str)

    out: List[str] = []
    seen: set = set()
    try:
        for m in modules:
            if not m or m in skip:
                continue
            try:
                if _ilu.find_spec(m) is not None:
                    needed = _VALIDATE.get(m)
                    if not needed:
                        continue
                    try:
                        real = importlib.import_module(m)
                        if all(hasattr(real, s) for s in needed):
                            continue
                    except Exception:
                        pass
                    sys.modules.pop(m, None)
                    _purge_bad_install(deps_dir, m)
            except (ImportError, ValueError):
                pass
            pip_name = _PYPI_ALIAS.get(m, m)
            if pip_name not in seen:
                seen.add(pip_name)
                out.append(pip_name)
    finally:
        if added:
            try:
                sys.path.remove(deps_str)
            except ValueError:
                pass
    return out


def install_deps(bot_dir: Path, kind: str, log: List[str]) -> None:
    try:
        if kind == "python":
            deps_dir = bot_dir / ".deps"
            deps_dir.mkdir(parents=True, exist_ok=True)
            env = _pip_env()

            req = bot_dir / "requirements.txt"
            if req.exists():
                log.append(f"{G['div']} pip install -r requirements.txt")
                r = subprocess.run(
                    [sys.executable, "-m", "pip", "install",
                     "--target", str(deps_dir), *_PIP_FLAGS, "-r", str(req)],
                    cwd=str(bot_dir), timeout=900,
                    capture_output=True, text=True, env=env,
                )
                for ln in (r.stdout or "").splitlines()[-12:]:
                    log.append(ln)
                for ln in (r.stderr or "").splitlines()[-8:]:
                    log.append(ln)
                log.append(f"[{G['ok']}] requirements done (rc={r.returncode})")

            missing = _third_party(_scan_imports(bot_dir), bot_dir)
            if missing:
                log.append(f"{G['div']} auto-install: {', '.join(missing)}")
                r = subprocess.run(
                    [sys.executable, "-m", "pip", "install",
                     "--target", str(deps_dir), *_PIP_FLAGS, *missing],
                    cwd=str(bot_dir), timeout=900,
                    capture_output=True, text=True, env=env,
                )
                for ln in (r.stdout or "").splitlines()[-12:]:
                    log.append(ln)
                for ln in (r.stderr or "").splitlines()[-8:]:
                    log.append(ln)
                log.append(f"[{G['ok']}] auto-install done (rc={r.returncode})")

        elif kind == "node":
            if not (bot_dir / "package.json").exists():
                return
            if (bot_dir / "node_modules").exists():
                log.append(f"[{G['ok']}] node_modules cached")
                return
            log.append(f"{G['div']} npm install")
            r = subprocess.run(
                ["npm", "install", "--omit=dev", "--no-audit", "--no-fund"],
                cwd=str(bot_dir), timeout=600, capture_output=True, text=True,
            )
            for ln in (r.stdout or "").splitlines()[-12:]:
                log.append(ln)
            for ln in (r.stderr or "").splitlines()[-8:]:
                log.append(ln)
            log.append(f"[{G['ok']}] npm done (rc={r.returncode})")

    except subprocess.TimeoutExpired:
        log.append(f"[{G['warn']}] dependency install timed out")
    except FileNotFoundError as e:
        log.append(f"[{G['warn']}] tool not found: {e}")
    except Exception as e:
        log.append(f"[{G['warn']}] install error: {e}")


def _write_log_file(bot_id: str, lines: List[str]) -> None:
    try:
        p = DIRS["logs"] / f"{bot_id}.log"
        with p.open("a", encoding="utf-8") as f:
            for ln in lines:
                f.write(ln + "\n")
        if p.stat().st_size > 2 * 1024 * 1024:
            tail = p.read_text(errors="ignore")[-512 * 1024:]
            p.write_text(tail, encoding="utf-8")
    except Exception:
        pass


def _drain(bot_id: str, proc: subprocess.Popen, log: Deque[str]) -> None:
    """Pump child stdout into the ring buffer, then handle exit."""
    buf: List[str] = []
    try:
        if proc.stdout:
            for raw in iter(proc.stdout.readline, b""):
                try:
                    txt = raw.decode("utf-8", "replace").rstrip()
                except Exception:
                    txt = repr(raw)
                log.append(txt)
                buf.append(txt)
                if len(buf) >= 20:
                    _write_log_file(bot_id, buf)
                    buf = []
    except Exception:
        pass
    if buf:
        _write_log_file(bot_id, buf)

    try:
        rc = proc.wait()
        log.append(f"{G['div']} exited rc={rc}")
        with _runner_lock:
            info = RUNNING.get(bot_id)
        manual = (info or {}).get("manual_stop", True)

        b = find_bot(bot_id)
        if b is not None:
            tail = [ln for ln in list(log)[-15:]
                    if ln and not ln.startswith(G["div"])]
            b["last_error"] = "\n".join(tail[-8:])[:1500]
            b["last_exit_code"] = int(rc) if rc is not None else None
            b["last_exit_at"] = ts_iso()
            if rc not in (0, None) and not manual:
                b["status"] = "crashed"
                b["crash_count"] = int(b.get("crash_count", 0)) + 1
            else:
                b["status"] = "stopped"
            save_bot(b)

        if manual or not b:
            with _runner_lock:
                RUNNING.pop(bot_id, None)
            return

        if rc not in (0, None) and get_setting("auto_restart", True):
            if int(b.get("crash_count", 0)) <= int(get_setting("max_crash_restarts", 5)):
                log.append(f"[{G['refresh']}] auto-restart in 5s…")
                time.sleep(5)
                with _runner_lock:
                    RUNNING.pop(bot_id, None)
                start_child(b)
                return
            notify(f"<b>{G['warn']} Auto-restart limit reached</b>\n"
                   f"{bullet('Bot', b.get('name'))}\n"
                   f"{bullet('Crashes', b.get('crash_count'))}")

        with _runner_lock:
            RUNNING.pop(bot_id, None)
    except Exception:
        traceback.print_exc()


def start_child(b: Dict[str, Any]) -> Dict[str, Any]:
    bid = b["_id"]
    with _runner_lock:
        existing = RUNNING.get(bid)
        if existing and existing["proc"].poll() is None:
            return {"ok": False, "error": "Already running."}

    bot_dir = Path(b["dir"])
    if not bot_dir.exists():
        return {"ok": False, "error": "Bot folder missing."}

    kind, entry = detect_entry(bot_dir)
    if not kind or not entry:
        return {"ok": False, "error": "No entry file found (bot.py / index.js)."}

    log: Deque[str] = deque(maxlen=LOG_RING)
    log.append(f"{G['div_eq']} START {ts_iso()}")
    install_deps(bot_dir, kind, log)  # type: ignore[arg-type]

    cmd = ["node", entry] if kind == "node" else [sys.executable, "-u", entry]
    try:
        proc = subprocess.Popen(
            cmd, cwd=str(bot_dir), env=safe_env(bot_dir, b.get("env")),
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            preexec_fn=os.setsid if os.name == "posix" else None,
        )
    except Exception as e:
        return {"ok": False, "error": f"spawn failed: {e}"}

    with _runner_lock:
        RUNNING[bid] = {
            "proc": proc, "kind": kind, "entry": entry,
            "started": time.time() * 1000, "log": log,
            "name": b["name"], "manual_stop": False,
        }
    threading.Thread(target=_drain, args=(bid, proc, log),
                     daemon=True, name=f"drain-{bid}").start()

    b["status"] = "running"
    b["last_started"] = ts_iso()
    b["last_error"] = ""
    b["last_exit_code"] = None
    b["entry"] = entry
    save_bot(b)
    return {"ok": True, "pid": proc.pid, "kind": kind, "entry": entry}


def stop_child(bot_id: str, manual: bool = True) -> Dict[str, Any]:
    with _runner_lock:
        info = RUNNING.get(bot_id)
    if not info:
        b = find_bot(bot_id)
        if b and b.get("status") != "stopped":
            b["status"] = "stopped"
            save_bot(b)
        return {"ok": True}

    info["manual_stop"] = manual
    proc = info["proc"]

    children: List[int] = []
    if psutil is not None:
        try:
            for ch in psutil.Process(proc.pid).children(recursive=True):
                children.append(ch.pid)
        except Exception:
            pass

    def _kill(pid: int, sig: int) -> None:
        try:
            os.kill(pid, sig)
        except Exception:
            pass

    try:
        if os.name == "posix":
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            except Exception:
                pass
            for pid in children:
                _kill(pid, signal.SIGTERM)
        else:
            proc.terminate()

        try:
            proc.wait(timeout=int(get_setting("stop_timeout", 8)))
        except subprocess.TimeoutExpired:
            if os.name == "posix":
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except Exception:
                    pass
                for pid in children:
                    _kill(pid, signal.SIGKILL)
                if psutil is not None:
                    try:
                        for ch in psutil.Process(proc.pid).children(recursive=True):
                            _kill(ch.pid, signal.SIGKILL)
                    except Exception:
                        pass
            else:
                proc.kill()
            try:
                proc.wait(timeout=3)
            except Exception:
                pass
    except Exception as e:
        with _runner_lock:
            RUNNING.pop(bot_id, None)
        b = find_bot(bot_id)
        if b:
            b["status"] = "stopped"
            save_bot(b)
        return {"ok": False, "error": str(e)}

    with _runner_lock:
        RUNNING.pop(bot_id, None)
    b = find_bot(bot_id)
    if b:
        b["status"] = "stopped"
        save_bot(b)
    return {"ok": True}


def restart_child(b: Dict[str, Any]) -> Dict[str, Any]:
    stop_child(b["_id"], manual=True)
    time.sleep(1)
    fresh = find_bot(b["_id"]) or b
    fresh["crash_count"] = 0
    save_bot(fresh)
    return start_child(fresh)


def child_status(bot_id: str, b: Dict[str, Any]) -> Dict[str, Any]:
    info = RUNNING.get(bot_id)
    running = bool(info and info["proc"].poll() is None)
    bot_dir = Path(b.get("dir") or "")
    kind = (info or {}).get("kind")
    entry = (info or {}).get("entry") or b.get("entry")
    if not kind and bot_dir.exists():
        kind, entry = detect_entry(bot_dir)
    cpu = mem = 0.0
    if running and psutil is not None:
        try:
            p = psutil.Process(info["proc"].pid)  # type: ignore[index]
            cpu = p.cpu_percent(interval=0.05)
            mem = p.memory_info().rss
        except Exception:
            pass
    return {
        "running": running,
        "pid": info["proc"].pid if running else None,  # type: ignore[index]
        "kind": kind or "—",
        "entry": entry or "—",
        "uptime_ms": int(time.time() * 1000 - info["started"]) if running else 0,  # type: ignore[index]
        "size": dir_size(bot_dir),
        "cpu": cpu,
        "mem": mem,
    }


def tail_log(bot_id: str, lines: int = MAX_LOG_LINES) -> str:
    info = RUNNING.get(bot_id)
    if info and info.get("log"):
        return "\n".join(list(info["log"])[-lines:])
    p = DIRS["logs"] / f"{bot_id}.log"
    if p.exists():
        try:
            return "\n".join(p.read_text(errors="ignore").splitlines()[-lines:])
        except Exception:
            pass
    return ""


# ═════════════════════════════════════════════════════════════════
# 10. BACKUP  (archive → encrypt → GitHub / Telegram)
# ═════════════════════════════════════════════════════════════════

_BACKUP_EXCLUDE = ("node_modules", ".deps", ".tmp_run", "__pycache__",
                   ".git", "venv", ".venv")


def build_backup() -> bytes:
    """tar.gz of storage/data + sandbox, then Fernet-encrypted."""
    def _filter(ti: tarfile.TarInfo) -> Optional[tarfile.TarInfo]:
        parts = ti.name.split("/")
        if any(x in parts for x in _BACKUP_EXCLUDE):
            return None
        if ti.name.endswith(".log"):
            return None
        if ti.name.endswith("master.key"):
            return None  # never ship the key inside its own ciphertext
        return ti

    tmp = DIRS["tmp"] / f"backup-{int(time.time())}.tar.gz"
    with tarfile.open(tmp, "w:gz") as tf:
        if DIRS["data"].exists():
            tf.add(str(DIRS["data"]), arcname="storage/data", filter=_filter)
        if DIRS["sandbox"].exists():
            tf.add(str(DIRS["sandbox"]), arcname="sandbox", filter=_filter)
    raw = tmp.read_bytes()
    tmp.unlink(missing_ok=True)
    return enc_bytes(raw)


def restore_backup(blob: bytes, wipe: bool = True) -> Dict[str, Any]:
    try:
        raw = dec_bytes(blob)
    except InvalidToken:
        return {"ok": False,
                "error": "Wrong master key — cannot decrypt this backup."}
    tmp = DIRS["tmp"] / f"restore-{int(time.time())}.tar.gz"
    tmp.write_bytes(raw)
    try:
        if wipe:
            for sub in (DIRS["sandbox"], DIRS["data"]):
                if sub.exists():
                    for child in sub.iterdir():
                        if child.name == "master.key":
                            continue
                        rmrf(child) if child.is_dir() else child.unlink(missing_ok=True)
        with tarfile.open(tmp, "r:gz") as tf:
            tf.extractall(str(BASE_DIR))
        for p in DIRS.values():
            p.mkdir(parents=True, exist_ok=True)
        cache_clear()
        return {"ok": True, "size": len(raw)}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        tmp.unlink(missing_ok=True)


# ─── GitHub ───────────────────────────────────────────────────────

GH: Dict[str, Any] = {
    "token": "", "repo": "", "branch": "main",
    "interval_min": 360, "auto": True,
    "last": None, "last_error": None, "busy": False,
}


def gh_load_config() -> None:
    GH["token"] = (os.environ.get("GITHUB_TOKEN")
                   or get_setting("gh_token", "") or "")
    GH["repo"] = (os.environ.get("GITHUB_REPO")
                  or get_setting("gh_repo", "") or "")
    GH["branch"] = (os.environ.get("GITHUB_BRANCH")
                    or get_setting("gh_branch", "main") or "main")
    try:
        GH["interval_min"] = max(15, int(get_setting("gh_interval_min", 360)))
    except Exception:
        GH["interval_min"] = 360
    GH["auto"] = bool(get_setting("gh_auto", True))


def gh_enabled() -> bool:
    return bool(GH["token"] and "/" in str(GH["repo"]))


def _gh(method: str, url: str, **kw: Any) -> requests.Response:
    h = kw.pop("headers", {}) or {}
    h.setdefault("Authorization", f"token {GH['token']}")
    h.setdefault("Accept", "application/vnd.github+json")
    h.setdefault("User-Agent", "personal-host-panel/3.0")
    return requests.request(method, url, headers=h, timeout=60, **kw)


def _gh_url(path: str = "", repo: Optional[str] = None) -> str:
    return f"https://api.github.com/repos/{repo or GH['repo']}/{path.lstrip('/')}"


def _gh_ensure_branch() -> bool:
    r = _gh("GET", _gh_url(f"branches/{GH['branch']}"))
    if r.status_code == 200:
        return True
    if r.status_code != 404:
        return False
    info = _gh("GET", _gh_url())
    if info.status_code != 200:
        return False
    default = info.json().get("default_branch", "main")
    ref = _gh("GET", _gh_url(f"git/ref/heads/{default}"))
    if ref.status_code != 200:
        return False
    _gh("POST", _gh_url("git/refs"),
        json={"ref": f"refs/heads/{GH['branch']}",
              "sha": ref.json()["object"]["sha"]})
    return True


def _gh_put(path: str, content: bytes, message: str) -> bool:
    sha = None
    g = _gh("GET", _gh_url(f"contents/{path}"), params={"ref": GH["branch"]})
    if g.status_code == 200:
        sha = g.json().get("sha")
    elif g.status_code != 404:
        return False
    body: Dict[str, Any] = {
        "message": message, "branch": GH["branch"],
        "content": base64.b64encode(content).decode(),
    }
    if sha:
        body["sha"] = sha
    r = _gh("PUT", _gh_url(f"contents/{path}"), json=body)
    return r.status_code in (200, 201)


def _gh_get(path: str, repo: Optional[str] = None) -> Optional[Dict[str, Any]]:
    try:
        r = _gh("GET", _gh_url(f"contents/{path}", repo),
                params={"ref": GH["branch"]})
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


def gh_backup_now() -> Dict[str, Any]:
    if not gh_enabled():
        return {"ok": False, "error": "GitHub not configured."}
    if GH["busy"]:
        return {"ok": False, "error": "Backup already running."}
    GH["busy"] = True
    try:
        if not _gh_ensure_branch():
            raise RuntimeError(f"branch {GH['branch']} unavailable")
        blob = build_backup()
        mb = len(blob) / 1024 / 1024
        if mb > 95:
            raise RuntimeError(f"backup {mb:.1f} MB exceeds GitHub 95 MB limit")
        ts = stamp()
        ok1 = _gh_put("backups/latest.enc", blob, f"backup {ts}")
        ok2 = _gh_put(f"backups/{ts}.enc", blob, f"snapshot {ts}")
        _gh_put("backups/manifest.json",
                json.dumps({"last": ts, "bytes": len(blob)}, indent=2).encode(),
                f"manifest {ts}")
        if not (ok1 and ok2):
            raise RuntimeError("upload failed")
        GH["last"] = ts
        GH["last_error"] = None
        return {"ok": True, "mb": f"{mb:.2f}", "ts": ts}
    except Exception as e:
        GH["last_error"] = str(e)
        return {"ok": False, "error": str(e)}
    finally:
        GH["busy"] = False


def gh_restore_latest() -> Dict[str, Any]:
    if not gh_enabled():
        return {"ok": False, "error": "GitHub not configured."}
    payload = _gh_get("backups/latest.enc")
    if payload is None:
        return {"ok": False, "error": "No backup found in repo."}
    try:
        blob = base64.b64decode(payload["content"])
    except Exception as e:
        return {"ok": False, "error": f"decode failed: {e}"}
    return restore_backup(blob, wipe=True)


def gh_auto_loop() -> None:
    while True:
        try:
            time.sleep(max(60, int(GH["interval_min"]) * 60))
            if gh_enabled() and GH["auto"]:
                res = gh_backup_now()
                if res.get("ok"):
                    print(f"[gh] backup ok ({res.get('mb')} MB)", flush=True)
                else:
                    print(f"[gh] backup failed: {res.get('error')}", flush=True)
                    notify(f"<b>{G['warn']} GitHub auto-backup failed</b>\n"
                           f"{bullet('Error', res.get('error'))}")
        except Exception as e:
            print(f"[gh] loop error: {e}", flush=True)


def gh_auto_restore_on_boot() -> Optional[Dict[str, Any]]:
    """Restore only when there is no local data at all."""
    if not (gh_enabled() and GH["auto"]):
        return None
    try:
        if db_load_ro()["bots"]:
            return {"ok": False, "skip": True, "reason": "local data present"}
    except Exception:
        pass
    return gh_restore_latest()


# ─── Telegram channel backup ──────────────────────────────────────

def tg_backup_chat() -> Optional[str]:
    v = get_setting("tg_backup_chat", None)
    return str(v) if v else None


def tg_backup_now() -> Dict[str, Any]:
    chat = tg_backup_chat()
    if not chat:
        return {"ok": False, "error": "No backup chat configured."}
    try:
        blob = build_backup()
        name = f"backup_{stamp()}.enc"
        bot.send_document(
            chat, (name, io.BytesIO(blob)),
            caption=(f"<b>{G['cloud']} Encrypted Backup</b>\n"
                     f"{bullet('Time', ts_iso()[:19])}\n"
                     f"{bullet('Size', fmt_bytes(len(blob)))}\n"
                     f"<i>Restore needs your master key.</i>"),
            parse_mode="HTML", visible_file_name=name,
        )
        audit("tg_backup", f"chat={chat} bytes={len(blob)}")
        return {"ok": True, "size": len(blob)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def tg_backup_loop() -> None:
    last = 0.0
    while True:
        try:
            time.sleep(120)
            if not (tg_backup_chat() and get_setting("tg_backup_auto", False)):
                continue
            try:
                iv = max(1, int(get_setting("tg_backup_interval_h", 6))) * 3600
            except Exception:
                iv = 6 * 3600
            now = time.time()
            if now - last >= iv:
                res = tg_backup_now()
                print(f"[tg] backup ok={res.get('ok')} "
                      f"err={res.get('error', '')}", flush=True)
                last = now
        except Exception as e:
            print(f"[tg] loop error: {e}", flush=True)


# ═════════════════════════════════════════════════════════════════
# 11. MENUS
# ═════════════════════════════════════════════════════════════════

def render_main(chat_id: int, call: Optional[types.CallbackQuery] = None,
                intro: str = "") -> None:
    bots = list_bots()
    running = sum(1 for x in RUNNING.values() if x["proc"].poll() is None)
    mem = 0
    if psutil is not None:
        try:
            mem = psutil.Process(os.getpid()).memory_info().rss
        except Exception:
            pass
    head = f"{intro}\n{G['div']}\n" if intro else ""
    txt = (
        f"<b>{esc(BRAND_TAG)}</b>\n"
        f"{G['div_eq']}\n{head}"
        f"{bullet('Bots', f'{len(bots)} / {max_bots()}  (running {running})')}\n"
        f"{bullet('Uptime', fmt_dur(int(time.time() * 1000) - START_TS))}\n"
        f"{bullet('Panel RAM', fmt_bytes(mem))}\n"
        f"{bullet('Auto-restart', 'on' if get_setting('auto_restart', True) else 'off')}\n"
        f"{bullet('GitHub backup', 'on' if gh_enabled() and GH['auto'] else 'off')}\n"
        f"{G['div']}\nPick an option below.{FOOTER}"
    )
    show(chat_id, txt, kb_main(), call=call)


def render_bots(call: types.CallbackQuery) -> None:
    bots = list_bots()
    txt = (f"<b>{G['diamond']} My Bots</b>\n{G['div_eq']}\n"
           f"{bullet('Slots', f'{len(bots)} / {max_bots()}')}\n")
    kb = types.InlineKeyboardMarkup()
    if not bots:
        txt += "\n<i>No bots yet. Tap Upload to add one.</i>"
    else:
        for b in bots:
            info = RUNNING.get(b["_id"])
            live = bool(info and info["proc"].poll() is None)
            mark = G["play"] if live else (
                G["warn"] if b.get("status") == "crashed" else G["stop"])
            src = " ☁" if b.get("source") == "github" else ""
            kb.add(Btn(f"{mark}  {b['name'][:32]}{src}",
                       callback_data=f"b_view_{b['_id']}"))
    kb.add(
        Btn(f"{G['upload']}  Upload",   callback_data="menu_upload", style="success"),
        Btn(f"{G['cloud']}  From GitHub", callback_data="menu_gh", style="primary"),
    )
    kb.add(Btn(f"{G['back']}  Main", callback_data="menu_main", style="primary"))
    show(call.message.chat.id, txt + FOOTER, kb, call=call)


def render_upload(call: types.CallbackQuery) -> None:
    bots = list_bots()
    txt = (
        f"<b>{G['upload']} Upload Bot</b>\n{G['div_eq']}\n"
        f"{bullet('Slots', f'{len(bots)} / {max_bots()}')}\n"
        f"{bullet('Max size', fmt_bytes(MAX_UPLOAD_BYTES))}\n"
        f"{G['div']}\n"
        f"Send your bot as a document.\n"
        f"Accepted: <code>.py .js .zip .tar.gz</code>\n\n"
        f"Entry file is auto-detected: <code>bot.py</code>, <code>main.py</code>, "
        f"<code>app.py</code>, <code>index.js</code>, …\n"
        f"Dependencies install automatically from "
        f"<code>requirements.txt</code> or by scanning imports.{FOOTER}"
    )
    USER_STATES[call.from_user.id] = {"flow": "await_upload"}
    show(call.message.chat.id, txt, kb_back("menu_main"), call=call)


def render_bot(call: types.CallbackQuery, bot_id: str) -> None:
    b = find_bot(bot_id)
    if not b:
        ack(call, "Not found")
        return
    st = child_status(bot_id, b)

    if st["running"]:
        status = f"{G['play']} Running"
    elif b.get("status") == "crashed":
        status = f"{G['warn']} Crashed"
    else:
        status = f"{G['stop']} Stopped"

    err = ""
    if not st["running"]:
        rc = b.get("last_exit_code")
        last = (b.get("last_error") or "").strip()
        if last or rc not in (None, 0):
            head = "Last error"
            if rc not in (None, 0):
                head += f" (exit {rc})"
            err = (f"\n{G['div']}\n<b>{G['no']} {head}</b>\n"
                   f"<pre>{esc(last or '(no output captured)')[:800]}</pre>")

    src = ""
    if b.get("source") == "github":
        src = f"\n{bullet('Source', 'GitHub')}\n{bullet('Repo', str(b.get('gh_repo', '?'))[:48])}"

    cron = b.get("cron") or {}
    cron_txt = ", ".join(f"{k.replace('_hours', '')}={v}h"
                         for k, v in cron.items()) or "—"

    txt = (
        f"<b>{G['diamond']} {esc(b['name'])}</b>\n"
        f"{G['div_eq']}\n"
        f"{bullet('Status', status)}\n"
        f"{bullet('Kind', st['kind'])}\n"
        f"{bullet('Entry', st['entry'])}\n"
        f"{bullet('PID', st['pid'] or '—')}\n"
        f"{bullet('Uptime', fmt_dur(st['uptime_ms']))}\n"
        f"{bullet('CPU', f'{st['cpu']:.1f}%')}\n"
        f"{bullet('Memory', fmt_bytes(st['mem']))}\n"
        f"{bullet('Disk', fmt_bytes(st['size']))}\n"
        f"{bullet('Env vars', len(b.get('env') or {}))}\n"
        f"{bullet('Cron', cron_txt)}\n"
        f"{bullet('Crashes', b.get('crash_count', 0))}\n"
        f"{bullet('Created', fmt_ts(b.get('created')))}"
        f"{src}{err}\n{G['div']}{FOOTER}"
    )
    show(call.message.chat.id, txt, kb_bot(bot_id, st["running"]), call=call)


def render_logs(call: types.CallbackQuery, bot_id: str) -> None:
    b = find_bot(bot_id)
    if not b:
        ack(call, "Not found")
        return
    logs = tail_log(bot_id) or "(no output yet)"
    txt = (f"<b>{G['bolt']} Logs — {esc(b['name'])}</b>\n"
           f"{G['div_eq']}\n<pre>{esc(logs[-3200:])}</pre>\n{G['div']}{FOOTER}")
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        Btn(f"{G['refresh']}  Refresh", callback_data=f"b_logs_{bot_id}", style="primary"),
        Btn(f"{G['download']}  Full Log", callback_data=f"b_logfile_{bot_id}", style="primary"),
    )
    kb.add(Btn(f"{G['back']}  Bot", callback_data=f"b_view_{bot_id}", style="primary"))
    show(call.message.chat.id, txt, kb, call=call)


def render_env(call: types.CallbackQuery, bot_id: str) -> None:
    b = find_bot(bot_id)
    if not b:
        ack(call, "Not found")
        return
    env = b.get("env") or {}
    rows = "\n".join(
        f"{G['bullet']} <code>{esc(k)}</code> = <code>{esc(_mask(str(v)))}</code>"
        for k, v in sorted(env.items())
    ) or "<i>No variables set.</i>"
    txt = (f"<b>{G['key']} Env Vars — {esc(b['name'])}</b>\n"
           f"{G['div_eq']}\n{rows}\n{G['div']}\n"
           f"Values are masked here but passed in full to the process.{FOOTER}")
    kb = types.InlineKeyboardMarkup()
    kb.add(Btn(f"{G['plus']}  Add / Edit", callback_data=f"b_envadd_{bot_id}",
               style="success"))
    for k in sorted(env)[:20]:
        kb.add(Btn(f"{G['no']}  Delete {k}",
                   callback_data=f"b_envdel_{bot_id}::{k}", style="danger"))
    kb.add(Btn(f"{G['back']}  Bot", callback_data=f"b_view_{bot_id}", style="primary"))
    show(call.message.chat.id, txt, kb, call=call)


def _mask(v: str) -> str:
    if len(v) <= 6:
        return "*" * len(v)
    return v[:3] + "*" * min(len(v) - 6, 12) + v[-3:]


def render_files(call: types.CallbackQuery, bot_id: str) -> None:
    b = find_bot(bot_id)
    if not b:
        ack(call, "Not found")
        return
    bot_dir = Path(b["dir"])
    rows: List[str] = []
    count = 0
    for p in sorted(bot_dir.rglob("*")):
        if any(part in _SKIP_DIRS for part in p.parts):
            continue
        if not p.is_file():
            continue
        count += 1
        if count <= 30:
            rows.append(f"{G['bullet']} <code>{esc(p.relative_to(bot_dir))}</code> "
                        f"({fmt_bytes(p.stat().st_size)})")
    extra = f"\n<i>…and {count - 30} more</i>" if count > 30 else ""
    txt = (f"<b>{G['folder']} Files — {esc(b['name'])}</b>\n"
           f"{G['div_eq']}\n"
           f"{bullet('Files', count)}\n"
           f"{bullet('Total', fmt_bytes(dir_size(bot_dir)))}\n"
           f"{G['div']}\n" + ("\n".join(rows) or "<i>empty</i>") + extra +
           f"\n{G['div']}{FOOTER}")
    show(call.message.chat.id, txt,
         kb_back(f"b_view_{bot_id}", "Bot"), call=call)


def render_cron(call: types.CallbackQuery, bot_id: str) -> None:
    b = find_bot(bot_id)
    if not b:
        ack(call, "Not found")
        return
    cron = b.get("cron") or {}
    txt = (
        f"<b>{G['clock']} Cron — {esc(b['name'])}</b>\n{G['div_eq']}\n"
        f"{bullet('Restart every', f\"{cron.get('restart_hours')}h\" if cron.get('restart_hours') else '—')}\n"
        f"{G['div']}\n"
        f"Send <code>restart=6</code> to restart every 6 hours.\n"
        f"Send <code>off</code> to disable.\n"
        f"/cancel to abort.{FOOTER}"
    )
    USER_STATES[call.from_user.id] = {"flow": "await_cron", "bot_id": bot_id}
    show(call.message.chat.id, txt,
         kb_back(f"b_view_{bot_id}", "Bot"), call=call)


def render_monitor(call: types.CallbackQuery) -> None:
    live = [(bid, i) for bid, i in list(RUNNING.items())
            if i["proc"].poll() is None]
    total_mem = 0.0
    rows: List[str] = []
    for bid, info in live[:20]:
        mem = 0.0
        cpu = 0.0
        if psutil is not None:
            try:
                p = psutil.Process(info["proc"].pid)
                mem = p.memory_info().rss
                cpu = p.cpu_percent(interval=0)
            except Exception:
                pass
        total_mem += mem
        rows.append(f"{G['bullet']} <b>{esc(info['name'][:22])}</b> "
                    f"pid {info['proc'].pid} · {fmt_bytes(mem)} · {cpu:.1f}% · "
                    f"{fmt_dur(int(time.time() * 1000 - info['started']))}")

    panel_mem = panel_cpu = 0.0
    disk_free = disk_total = 0
    load = (0.0, 0.0, 0.0)
    if psutil is not None:
        try:
            p = psutil.Process(os.getpid())
            panel_mem = p.memory_info().rss
            panel_cpu = p.cpu_percent(interval=0.2)
            du = psutil.disk_usage(str(BASE_DIR))
            disk_total, disk_free = du.total, du.free
        except Exception:
            pass
    try:
        load = os.getloadavg()
    except Exception:
        pass

    txt = (
        f"<b>{G['graph']} Live Monitor</b>\n{G['div_eq']}\n"
        f"{bullet('Panel uptime', fmt_dur(int(time.time() * 1000) - START_TS))}\n"
        f"{bullet('Panel RAM', fmt_bytes(panel_mem))}\n"
        f"{bullet('Panel CPU', f'{panel_cpu:.1f}%')}\n"
        f"{bullet('Load avg', f'{load[0]:.2f} {load[1]:.2f} {load[2]:.2f}')}\n"
        f"{bullet('Disk free', f'{fmt_bytes(disk_free)} / {fmt_bytes(disk_total)}')}\n"
        f"{bullet('Threads', threading.active_count())}\n"
        f"{G['div']}\n"
        f"{bullet('Running bots', len(live))}\n"
        f"{bullet('Bots RAM', fmt_bytes(total_mem))}\n"
        f"{G['div']}\n" + ("\n".join(rows) or "<i>Nothing running.</i>") +
        f"\n{G['div']}{FOOTER}"
    )
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        Btn(f"{G['refresh']}  Refresh", callback_data="menu_monitor", style="primary"),
        Btn(f"{G['no']}  Stop All",     callback_data="op_stopall", style="danger"),
    )
    kb.add(
        Btn(f"{G['refresh']}  Restart All", callback_data="op_restartall", style="success"),
        Btn(f"{G['back']}  Main",           callback_data="menu_main", style="primary"),
    )
    show(call.message.chat.id, txt, kb, call=call)


def render_settings(call: types.CallbackQuery) -> None:
    auto = bool(get_setting("auto_restart", True))
    txt = (
        f"<b>{G['settings']} Settings</b>\n{G['div_eq']}\n"
        f"{bullet('Owner ID', OWNER_ID)}\n"
        f"{bullet('Max bots', max_bots())}\n"
        f"{bullet('Auto-restart on crash', 'on' if auto else 'off')}\n"
        f"{bullet('Max crash restarts', get_setting('max_crash_restarts', 5))}\n"
        f"{bullet('GitHub repo', GH['repo'] or '—')}\n"
        f"{bullet('GitHub auto', 'on' if GH['auto'] else 'off')}\n"
        f"{bullet('Telegram backup chat', tg_backup_chat() or '—')}\n"
        f"{bullet('Keepalive port', KEEPALIVE_PORT)}\n"
        f"{G['div']}{FOOTER}"
    )
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        Btn(f"{G['ok'] if auto else G['no']}  Auto-restart",
            callback_data="set_auto_toggle",
            style="success" if auto else "danger"),
        Btn(f"{G['diamond']}  Max Bots", callback_data="set_maxbots", style="primary"),
    )
    kb.add(
        Btn(f"{G['cloud']}  GitHub Backup", callback_data="menu_ghbackup", style="primary"),
        Btn(f"{G['cloud']}  TG Backup",     callback_data="menu_tgbackup", style="primary"),
    )
    kb.add(
        Btn(f"{G['key']}  Master Key",  callback_data="set_showkey", style="danger"),
        Btn(f"{G['upload']}  Restore File", callback_data="set_restorefile", style="danger"),
    )
    kb.add(
        Btn(f"{G['eye']}  System Info", callback_data="menu_sysinfo", style="primary"),
        Btn(f"{G['eye']}  Audit Log",   callback_data="menu_audit", style="primary"),
    )
    kb.add(
        Btn(f"{G['warn']}  Clean Orphans", callback_data="op_clean", style="danger"),
        Btn(f"{G['download']}  Export Now", callback_data="op_export", style="primary"),
    )
    kb.add(
        Btn(f"{G['refresh']}  Reload Cache", callback_data="op_reload", style="success"),
        Btn(f"{G['back']}  Main",            callback_data="menu_main", style="primary"),
    )
    show(call.message.chat.id, txt, kb, call=call)


def render_ghbackup(call: types.CallbackQuery) -> None:
    txt = (
        f"<b>{G['cloud']} GitHub Backup</b>\n{G['div_eq']}\n"
        f"{bullet('Configured', 'yes' if gh_enabled() else 'no')}\n"
        f"{bullet('Repo', GH['repo'] or '—')}\n"
        f"{bullet('Branch', GH['branch'])}\n"
        f"{bullet('Interval', f\"{GH['interval_min']} min\")}\n"
        f"{bullet('Auto', 'on' if GH['auto'] else 'off')}\n"
        f"{bullet('Token', 'set' if GH['token'] else 'not set')}\n"
        f"{bullet('Last backup', GH['last'] or '—')}\n"
        f"{bullet('Last error', GH['last_error'] or '—')}\n"
        f"{G['div']}\n"
        f"Backups are a tar.gz of your DB and sandbox, "
        f"Fernet-encrypted with your master key before upload.{FOOTER}"
    )
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        Btn(f"{G['key']}  Set Token", callback_data="gh_set_token", style="primary"),
        Btn(f"{G['diamond']}  Set Repo", callback_data="gh_set_repo", style="primary"),
    )
    kb.add(
        Btn(f"{G['folder']}  Set Branch", callback_data="gh_set_branch", style="primary"),
        Btn(f"{G['clock']}  Interval",    callback_data="gh_set_interval", style="primary"),
    )
    kb.add(
        Btn(f"{G['ok'] if GH['auto'] else G['no']}  Auto",
            callback_data="gh_toggle_auto",
            style="success" if GH["auto"] else "danger"),
        Btn(f"{G['upload']}  Backup Now", callback_data="gh_backup", style="success"),
    )
    kb.add(
        Btn(f"{G['download']}  Restore Latest", callback_data="gh_restore", style="danger"),
        Btn(f"{G['no']}  Clear Config",         callback_data="gh_clear", style="danger"),
    )
    kb.add(Btn(f"{G['back']}  Settings", callback_data="menu_settings", style="primary"))
    show(call.message.chat.id, txt, kb, call=call)


def render_tgbackup(call: types.CallbackQuery) -> None:
    auto = bool(get_setting("tg_backup_auto", False))
    txt = (
        f"<b>{G['cloud']} Telegram Channel Backup</b>\n{G['div_eq']}\n"
        f"{bullet('Backup chat', tg_backup_chat() or '—')}\n"
        f"{bullet('Auto', 'on' if auto else 'off')}\n"
        f"{bullet('Interval', f\"{get_setting('tg_backup_interval_h', 6)} h\")}\n"
        f"{G['div']}\n"
        f"Create a private channel, add this bot as admin, then set the chat "
        f"(<code>@handle</code> or <code>-100…</code>).\n"
        f"The encrypted archive is posted there. To restore, forward the "
        f"<code>.enc</code> file back and use Restore File.{FOOTER}"
    )
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        Btn(f"{G['diamond']}  Set Chat", callback_data="tg_set_chat", style="primary"),
        Btn(f"{G['upload']}  Backup Now", callback_data="tg_backup", style="success"),
    )
    kb.add(
        Btn(f"{G['ok'] if auto else G['no']}  Auto",
            callback_data="tg_toggle_auto",
            style="success" if auto else "danger"),
        Btn(f"{G['clock']}  Interval", callback_data="tg_set_interval", style="primary"),
    )
    kb.add(
        Btn(f"{G['no']}  Clear Chat", callback_data="tg_clear", style="danger"),
        Btn(f"{G['back']}  Settings", callback_data="menu_settings", style="primary"),
    )
    show(call.message.chat.id, txt, kb, call=call)


def render_sysinfo(call: types.CallbackQuery) -> None:
    import platform
    mem = vms = cpu = 0.0
    if psutil is not None:
        try:
            p = psutil.Process(os.getpid())
            mi = p.memory_info()
            mem, vms = mi.rss, mi.vms
            cpu = p.cpu_percent(interval=0.2)
        except Exception:
            pass
    db_sz = DB_FILE.stat().st_size if DB_FILE.exists() else 0
    txt = (
        f"<b>{G['eye']} System Info</b>\n{G['div_eq']}\n"
        f"{bullet('Python', platform.python_version())}\n"
        f"{bullet('OS', f'{platform.system()} {platform.release()}')}\n"
        f"{bullet('Machine', platform.machine())}\n"
        f"{bullet('PID', os.getpid())}\n"
        f"{bullet('Uptime', fmt_dur(int((time.time() - START_TIME) * 1000)))}\n"
        f"{bullet('RSS', fmt_bytes(mem))}\n"
        f"{bullet('VMS', fmt_bytes(vms))}\n"
        f"{bullet('CPU', f'{cpu:.1f}%')}\n"
        f"{bullet('Threads', threading.active_count())}\n"
        f"{G['div']}\n"
        f"{bullet('DB size', fmt_bytes(db_sz))}\n"
        f"{bullet('Sandbox size', fmt_bytes(dir_size(DIRS['sandbox'])))}\n"
        f"{bullet('Logs size', fmt_bytes(dir_size(DIRS['logs'])))}\n"
        f"{bullet('Cache entries', len(_CACHE))}\n"
        f"{G['div']}{FOOTER}"
    )
    show(call.message.chat.id, txt, kb_back("menu_settings", "Settings"), call=call)


def render_audit(call: types.CallbackQuery) -> None:
    rows = db_load_ro()["audit"][-25:]
    body = "\n".join(
        f"{G['bullet']} <code>{esc(str(a.get('ts', ''))[11:19])}</code> "
        f"{esc(a.get('action'))} {esc(str(a.get('detail', ''))[:60])}"
        for a in reversed(rows)
    ) or "<i>empty</i>"
    txt = (f"<b>{G['eye']} Audit Log</b>\n{G['div_eq']}\n{body}\n"
           f"{G['div']}{FOOTER}")
    show(call.message.chat.id, txt, kb_back("menu_settings", "Settings"), call=call)


def render_help(call: types.CallbackQuery) -> None:
    txt = (
        f"<b>{G['eye']} Help</b>\n{G['div_eq']}\n"
        f"<b>Upload</b> — send a <code>.py</code>, <code>.js</code>, "
        f"<code>.zip</code> or <code>.tar.gz</code> document. A new bot is "
        f"created and started automatically.\n\n"
        f"<b>Entry file</b> — detected in this order: "
        f"<code>bot.py main.py app.py run.py</code>, then "
        f"<code>index.js bot.js main.js app.js</code>, then the first "
        f"<code>.py</code>/<code>.js</code> found.\n\n"
        f"<b>Dependencies</b> — installed into the bot's own "
        f"<code>.deps/</code> from <code>requirements.txt</code>, plus any "
        f"imports found by scanning the source. Nothing touches system "
        f"packages.\n\n"
        f"<b>Env vars</b> — per bot, injected into the process. "
        f"Panel secrets (<code>BOT_TOKEN</code>, <code>MASTER_KEY</code>, "
        f"<code>GITHUB_TOKEN</code>) are stripped from the child environment.\n\n"
        f"<b>Backups</b> — tar.gz of DB + sandbox, encrypted with your master "
        f"key, pushed to GitHub and/or a private Telegram channel. "
        f"<b>Save the master key</b> or backups are unreadable.\n\n"
        f"<b>From GitHub</b> — browse any repo you can read and deploy a "
        f"single file, or clone the whole repo as a bot.\n\n"
        f"<b>Commands</b>\n"
        f"/start /menu — main panel\n"
        f"/status — running bots\n"
        f"/backup — backup now\n"
        f"/key — show master key\n"
        f"/id — your Telegram id\n"
        f"/cancel — abort current prompt{FOOTER}"
    )
    show(call.message.chat.id, txt, kb_back("menu_main"), call=call)


# ═════════════════════════════════════════════════════════════════
# 12. GITHUB SOURCE  (browse + deploy)
# ═════════════════════════════════════════════════════════════════

def render_gh_menu(call: types.CallbackQuery) -> None:
    txt = (
        f"<b>{G['cloud']} Deploy from GitHub</b>\n{G['div_eq']}\n"
        f"{bullet('Token', 'set' if GH['token'] else 'not set')}\n"
        f"{bullet('Default repo', GH['repo'] or '—')}\n"
        f"{bullet('Branch', GH['branch'])}\n"
        f"{G['div']}\n"
        f"<b>Clone repo</b> — <code>git clone</code> the whole repository as a "
        f"new bot.\n"
        f"<b>Browse</b> — walk a repo tree and deploy a single file.\n\n"
        f"A token is only needed for private repos.{FOOTER}"
    )
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        Btn(f"{G['download']}  Clone Repo", callback_data="gh_clone", style="success"),
        Btn(f"{G['folder']}  Browse Repo",  callback_data="gh_browse", style="primary"),
    )
    kb.add(
        Btn(f"{G['key']}  Set Token",  callback_data="gh_set_token", style="primary"),
        Btn(f"{G['diamond']}  Set Repo", callback_data="gh_set_repo", style="primary"),
    )
    kb.add(Btn(f"{G['back']}  Main", callback_data="menu_main", style="primary"))
    show(call.message.chat.id, txt, kb, call=call)


def _file_icon(name: str) -> str:
    return {".py": "🐍", ".js": "📜", ".json": "📋", ".txt": "📝",
            ".md": "📝", ".zip": "📦", ".sh": "⚙", ".yml": "📋",
            ".yaml": "📋", ".toml": "📋", ".env": "🔐"}.get(
        Path(name).suffix.lower(), "📄")


def gh_browse(call: types.CallbackQuery, repo: str, path: str = "") -> None:
    ack(call, "Loading…")

    def _bg() -> None:
        try:
            r = _gh("GET", _gh_url(f"contents/{path}", repo),
                    params={"ref": GH["branch"]})
            if r.status_code != 200:
                show(call.message.chat.id,
                     f"<b>{G['no']} GitHub HTTP {r.status_code}</b>\n"
                     f"<code>{esc(str(r.text)[:300])}</code>",
                     kb_back("menu_gh", "GitHub"))
                return
            items = r.json()
            if not isinstance(items, list):
                items = [items]
            items.sort(key=lambda x: (0 if x.get("type") == "dir" else 1,
                                      x.get("name", "").lower()))
            st = USER_STATES.setdefault(call.from_user.id, {})
            st.update({"gh_repo": repo, "gh_path": path, "gh_items": items})

            dirs = [i for i in items if i.get("type") == "dir"]
            files = [i for i in items if i.get("type") != "dir"]
            txt = (f"<b>{G['folder']} {esc(repo)}/{esc(path or '')}</b>\n"
                   f"{G['div_eq']}\n"
                   f"{bullet('Folders', len(dirs))}\n"
                   f"{bullet('Files', len(files))}\n{G['div']}{FOOTER}")

            kb = types.InlineKeyboardMarkup(row_width=1)
            if path:
                kb.add(Btn("⬆  ..", callback_data="ghb_up", style="primary"))
            for idx, it in enumerate(items[:24]):
                icon = "📁" if it.get("type") == "dir" else _file_icon(it.get("name", ""))
                kb.add(Btn(f"{icon} {it.get('name', '?')[:34]}",
                           callback_data=f"ghb_i_{idx}", style="primary"))
            kb.add(Btn(f"{G['back']}  GitHub", callback_data="menu_gh", style="primary"))
            show(call.message.chat.id, txt, kb)
        except Exception as e:
            show(call.message.chat.id,
                 f"<b>{G['no']} Browse error</b>\n<code>{esc(e)}</code>",
                 kb_back("menu_gh", "GitHub"))

    threading.Thread(target=_bg, daemon=True).start()


def gh_view_file(call: types.CallbackQuery, repo: str, path: str) -> None:
    ack(call, "Loading file…")

    def _bg() -> None:
        payload = _gh_get(path, repo)
        if payload is None or isinstance(payload, list):
            show(call.message.chat.id, f"<b>{G['no']} Cannot read file.</b>",
                 kb_back("menu_gh", "GitHub"))
            return
        try:
            raw = base64.b64decode(str(payload.get("content", "")).replace("\n", ""))
        except Exception:
            raw = b""
        name = payload.get("name", Path(path).name)
        try:
            preview = raw[:1200].decode("utf-8")
        except Exception:
            preview = "(binary file)"
        st = USER_STATES.setdefault(call.from_user.id, {})
        st.update({"gh_repo": repo, "gh_file": path})

        txt = (f"<b>{_file_icon(name)} {esc(name)}</b>\n{G['div_eq']}\n"
               f"{bullet('Repo', repo)}\n"
               f"{bullet('Path', path)}\n"
               f"{bullet('Size', fmt_bytes(payload.get('size', len(raw))))}\n"
               f"{G['div']}\n<pre>{esc(preview)}</pre>"
               f"{'…' if len(raw) > 1200 else ''}{FOOTER}")

        kb = types.InlineKeyboardMarkup(row_width=2)
        if name.lower().endswith((".py", ".js")):
            kb.add(Btn(f"{G['play']}  Deploy as Bot",
                       callback_data="ghb_deploy", style="success"))
        kb.add(
            Btn(f"{G['download']}  Download", callback_data="ghb_dl", style="primary"),
            Btn(f"{G['back']}  Folder",       callback_data="ghb_folder", style="primary"),
        )
        kb.add(Btn(f"{G['back']}  GitHub", callback_data="menu_gh", style="primary"))
        show(call.message.chat.id, txt, kb)

    threading.Thread(target=_bg, daemon=True).start()


def gh_deploy_file(call: types.CallbackQuery, repo: str, path: str) -> None:
    if len(list_bots()) >= max_bots():
        ack(call, "Bot slot limit reached")
        return
    ack(call, "Deploying…")

    def _bg() -> None:
        payload = _gh_get(path, repo)
        if payload is None or isinstance(payload, list):
            notify(f"{G['no']} Cannot read {esc(path)}")
            return
        try:
            raw = base64.b64decode(str(payload.get("content", "")).replace("\n", ""))
        except Exception as e:
            notify(f"{G['no']} decode failed: {esc(e)}")
            return
        name = payload.get("name", Path(path).name)
        doc = new_bot_doc(Path(name).stem)
        doc.update({"source": "github", "gh_repo": repo, "gh_path": path})
        (Path(doc["dir"]) / name).write_bytes(raw)
        save_bot(doc)
        audit("gh_deploy_file", f"repo={repo} path={path} bot={doc['_id']}")
        res = start_child(doc)
        notify(
            f"<b>{G['ok'] if res.get('ok') else G['no']} GitHub deploy</b>\n"
            f"{bullet('File', name)}\n"
            f"{bullet('Bot', doc['name'])}\n"
            f"{bullet('Status', 'running' if res.get('ok') else res.get('error'))}"
        )

    threading.Thread(target=_bg, daemon=True).start()


def g
    ack(call, "Downloading…")

    def _bg() -> None:
        payload = _gh_get(path, repo)
        if payload is None or isinstance(payload, list):
            notify(f"{G['no']} Cannot read {esc(path)}")
            return
        try:
            raw = base64.b64decode(str(payload.get("content", "")).replace("\n", ""))
        except Exception as e:
            notify(f"{G['no']} decode failed: {esc(e)}")
            return
        name = payload.get("name", Path(path).name)
        bot.send_document(call.message.chat.id, (name, io.BytesIO(raw)),
                          caption=f"<code>{esc(repo)}/{esc(path)}</code> "
                                  f"({fmt_bytes(len(raw))})",
                          parse_mode="HTML", visible_file_name=name)

    threading.Thread(target=_bg, daemon=True).start()


def gh_clone_repo(repo_url: str, chat_id: int) -> None:
    """git clone a repo into a new bot dir, then start it."""
    def _bg() -> None:
        if len(list_bots()) >= max_bots():
            bot.send_message(chat_id, f"{G['no']} Bot slot limit reached.")
            return
        name = safe_name(repo_url.rstrip("/").split("/")[-1].removesuffix(".git"))
        doc = new_bot_doc(name)
        target = Path(doc["dir"])
        url = repo_url
        if GH["token"] and repo_url.startswith("https://github.com/"):
            url = repo_url.replace("https://", f"https://{GH['token']}@")
        try:
            r = subprocess.run(
                ["git", "clone", "--depth=1", url, str(target)],
                capture_output=True, text=True, timeout=300,
            )
            if r.returncode != 0:
                rmrf(target)
                delete_bot_doc(doc["_id"])
                err = (r.stderr or r.stdout or "")[:600]
                err = err.replace(GH["token"], "***") if GH["token"] else err
                bot.send_message(chat_id,
                                 f"<b>{G['no']} Clone failed</b>\n"
                                 f"<pre>{esc(err)}</pre>", parse_mode="HTML")
                return
        except FileNotFoundError:
            rmrf(target)
            delete_bot_doc(doc["_id"])
            bot.send_message(chat_id, f"{G['no']} git is not installed on the host.")
            return
        except Exception as e:
            rmrf(target)
            delete_bot_doc(doc["_id"])
            bot.send_message(chat_id, f"{G['no']} clone error: <code>{esc(e)}</code>",
                             parse_mode="HTML")
            return

        rmrf(target / ".git")
        doc.update({"source": "github", "gh_repo": repo_url})
        save_bot(doc)
        audit("gh_clone", f"repo={repo_url} bot={doc['_id']}")
        kind, entry = detect_entry(target)
        bot.send_message(
            chat_id,
            f"<b>{G['ok']} Repo cloned</b>\n"
            f"{bullet('Bot', doc['name'])}\n"
            f"{bullet('Kind', kind or 'unknown')}\n"
            f"{bullet('Entry', entry or 'not found')}\n"
            f"{bullet('Size', fmt_bytes(dir_size(target)))}\n"
            f"Starting…", parse_mode="HTML")
        res = start_child(doc)
        if not res.get("ok"):
            bot.send_message(chat_id,
                             f"{G['no']} Start failed: <code>{esc(res.get('error'))}</code>",
                             parse_mode="HTML")

    threading.Thread(target=_bg, daemon=True).start()


# ═════════════════════════════════════════════════════════════════
# 13. UPLOAD HANDLING
# ═════════════════════════════════════════════════════════════════

def _extract_archive(data: bytes, fname: str, dest: Path) -> int:
    """Extract zip/tar into dest with traversal protection. Returns file count."""
    count = 0
    low = fname.lower()
    if low.endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            for member in zf.infolist():
                if member.is_dir():
                    continue
                rel = member.filename.replace("\\", "/")
                if rel.startswith("/") or ".." in rel.split("/"):
                    continue
                try:
                    out = safe_join(dest, rel)
                except ValueError:
                    continue
                out.parent.mkdir(parents=True, exist_ok=True)
                out.write_bytes(zf.read(member))
                count += 1
    else:
        tmp = DIRS["tmp"] / f"up-{int(time.time())}-{fname}"
        tmp.write_bytes(data)
        try:
            with tarfile.open(tmp, "r:*") as tf:
                for member in tf.getmembers():
                    if not member.isfile():
                        continue
                    rel = member.name.replace("\\", "/").lstrip("./")
                    if rel.startswith("/") or ".." in rel.split("/"):
                        continue
                    try:
                        out = safe_join(dest, rel)
                    except ValueError:
                        continue
                    src = tf.extractfile(member)
                    if src is None:
                        continue
                    out.parent.mkdir(parents=True, exist_ok=True)
                    out.write_bytes(src.read())
                    count += 1
        finally:
            tmp.unlink(missing_ok=True)

    # If everything landed inside one wrapper folder, flatten it.
    entries = [p for p in dest.iterdir() if p.name not in _SKIP_DIRS]
    if len(entries) == 1 and entries[0].is_dir():
        inner = entries[0]
        for child in list(inner.iterdir()):
            shutil.move(str(child), str(dest / child.name))
        rmrf(inner)
    return count


def handle_upload(m: types.Message, target_bot_id: Optional[str] = None) -> None:
    doc = m.document
    if not doc:
        return
    if doc.file_size and doc.file_size > MAX_UPLOAD_BYTES:
        bot.reply_to(m, f"{G['no']} File too large "
                        f"(> {fmt_bytes(MAX_UPLOAD_BYTES)}).")
        return

    fname = doc.file_name or "upload.bin"
    if not re.match(r"^[A-Za-z0-9._\-]+$", fname):
        bot.reply_to(m, f"{G['warn']} Filename has unusual characters — rename it.")
        return

    low = fname.lower()
    is_archive = low.endswith((".zip", ".tar.gz", ".tgz", ".tar"))
    if not (is_archive or low.endswith((".py", ".js", ".mjs", ".txt", ".json",
                                        ".env", ".yml", ".yaml", ".toml"))):
        bot.reply_to(m, f"{G['no']} Unsupported type. Use "
                        f".py .js .zip .tar.gz")
        return

    try:
        f = bot.get_file(doc.file_id)
        raw = bot.download_file(f.file_path)
    except Exception as e:
        bot.reply_to(m, f"{G['no']} download failed: <code>{esc(e)}</code>",
                     parse_mode="HTML")
        return

    # ── add file to an existing bot ────────────────────────────
    if target_bot_id:
        b = find_bot(target_bot_id)
        if not b:
            bot.reply_to(m, f"{G['no']} Bot not found.")
            return
        dest = Path(b["dir"])
        try:
            if is_archive:
                n = _extract_archive(raw, fname, dest)
            else:
                safe_join(dest, fname).write_bytes(raw)
                n = 1
        except Exception as e:
            bot.reply_to(m, f"{G['no']} extract failed: <code>{esc(e)}</code>",
                         parse_mode="HTML")
            return
        audit("add_file", f"bot={target_bot_id} file={fname} count={n}")
        bot.reply_to(m,
                     f"<b>{G['ok']} Added to {esc(b['name'])}</b>\n"
                     f"{bullet('Files', n)}\n"
                     f"{bullet('Size', fmt_bytes(len(raw)))}\n"
                     f"Restart the bot to pick up the change.",
                     parse_mode="HTML")
        return

    # ── new bot ────────────────────────────────────────────────
    if len(list_bots()) >= max_bots():
        bot.reply_to(m, f"{G['no']} Bot limit reached ({max_bots()}). "
                        f"Delete one or raise the limit in Settings.")
        return

    b = new_bot_doc(Path(fname).stem)
    dest = Path(b["dir"])
    try:
        if is_archive:
            n = _extract_archive(raw, fname, dest)
            if n == 0:
                rmrf(dest)
                delete_bot_doc(b["_id"])
                bot.reply_to(m, f"{G['no']} Archive is empty or unreadable.")
                return
        else:
            (dest / fname).write_bytes(raw)
            n = 1
    except zipfile.BadZipFile:
        rmrf(dest)
        delete_bot_doc(b["_id"])
        bot.reply_to(m, f"{G['no']} Not a valid zip.")
        return
    except Exception as e:
        rmrf(dest)
        delete_bot_doc(b["_id"])
        bot.reply_to(m, f"{G['no']} extract failed: <code>{esc(e)}</code>",
                     parse_mode="HTML")
        return

    save_bot(b)
    USER_STATES.pop(m.from_user.id, None)
    audit("upload", f"bot={b['_id']} file={fname} files={n}")

    kind, entry = detect_entry(dest)
    status = bot.reply_to(
        m,
        f"<b>{G['upload']} Stored</b>\n"
        f"{bullet('Bot', b['name'])}\n"
        f"{bullet('Files', n)}\n"
        f"{bullet('Size', fmt_bytes(len(raw)))}\n"
        f"{bullet('Entry', entry or 'auto-detect')}\n"
        f"Installing dependencies and starting…",
        parse_mode="HTML",
    )

    def _bg() -> None:
        res = start_child(b)
        kb = types.InlineKeyboardMarkup()
        kb.add(Btn(f"{G['bolt']}  Logs", callback_data=f"b_logs_{b['_id']}"),
               Btn(f"{G['diamond']}  Open", callback_data=f"b_view_{b['_id']}"))
        if res.get("ok"):
            body = (f"<b>{G['ok']} {esc(b['name'])} is running</b>\n"
                    f"{bullet('Kind', res.get('kind'))}\n"
                    f"{bullet('Entry', res.get('entry'))}\n"
                    f"{bullet('PID', res.get('pid'))}")
        else:
            body = (f"<b>{G['no']} Start failed</b>\n"
                    f"{bullet('Bot', b['name'])}\n"
                    f"{bullet('Error', res.get('error'))}\n"
                    f"Open Logs to see why.")
        try:
            bot.edit_message_text(body, chat_id=status.chat.id,
                                  message_id=status.message_id,
                                  parse_mode="HTML", reply_markup=kb)
        except Exception:
            bot.send_message(status.chat.id, body, parse_mode="HTML",
                             reply_markup=kb)

    threading.Thread(target=_bg, daemon=True).start()


# ═════════════════════════════════════════════════════════════════
# 14. OPERATIONS
# ═════════════════════════════════════════════════════════════════

def op_stop_all() -> int:
    n = 0
    for bid in list(RUNNING.keys()):
        if stop_child(bid, manual=True).get("ok"):
            n += 1
    audit("stop_all", f"stopped={n}")
    return n


def op_restart_all() -> Tuple[int, int]:
    ok = fail = 0
    for bid in list(RUNNING.keys()):
        b = find_bot(bid)
        if not b:
            continue
        if restart_child(b).get("ok"):
            ok += 1
        else:
            fail += 1
    audit("restart_all", f"ok={ok} fail={fail}")
    return ok, fail


def op_clean_orphans() -> Tuple[int, int]:
    """Remove sandbox dirs and log files with no matching bot record."""
    valid = set(db_load_ro()["bots"].keys())
    dirs = logs = 0
    if DIRS["sandbox"].exists():
        for e in DIRS["sandbox"].iterdir():
            if e.is_dir() and e.name not in valid:
                rmrf(e)
                dirs += 1
    if DIRS["logs"].exists():
        for f in DIRS["logs"].iterdir():
            if f.is_file() and f.suffix == ".log" and f.stem not in valid:
                try:
                    f.unlink()
                    logs += 1
                except Exception:
                    pass
    for f in DIRS["tmp"].glob("*"):
        try:
            if f.is_file() and time.time() - f.stat().st_mtime > 3600:
                f.unlink()
        except Exception:
            pass
    audit("clean_orphans", f"dirs={dirs} logs={logs}")
    return dirs, logs


# ═════════════════════════════════════════════════════════════════
# 15. CALLBACK ROUTER
# ═════════════════════════════════════════════════════════════════

_CB_SEEN: Deque[Tuple[str, float]] = deque(maxlen=256)
_CB_LOCK = threading.Lock()


def _dup_callback(cid: str) -> bool:
    if not cid:
        return False
    now = time.time()
    with _CB_LOCK:
        while _CB_SEEN and now - _CB_SEEN[0][1] > 10:
            _CB_SEEN.popleft()
        if any(c == cid for c, _ in _CB_SEEN):
            return True
        _CB_SEEN.append((cid, now))
    return False


@bot.callback_query_handler(func=lambda c: True)
def cb_root(call: types.CallbackQuery) -> None:
    if _dup_callback(getattr(call, "id", "")):
        ack(call)
        return
    if not is_owner(call.from_user.id):
        ack(call, "Private bot.")
        return
    try:
        _route(call, call.data or "")
    except Exception as e:
        traceback.print_exc()
        try:
            bot.send_message(call.message.chat.id,
                             f"<b>{G['no']} Error</b>\n<code>{esc(e)}</code>",
                             parse_mode="HTML")
        except Exception:
            pass


def _route(call: types.CallbackQuery, data: str) -> None:
    cid = call.message.chat.id
    uid = call.from_user.id

    # ── main navigation ────────────────────────────────────────
    if data == "menu_main":
        ack(call); render_main(cid, call); return
    if data == "menu_bots":
        ack(call); render_bots(call); return
    if data == "menu_upload":
        ack(call); render_upload(call); return
    if data == "menu_monitor":
        ack(call); render_monitor(call); return
    if data == "menu_settings":
        ack(call); render_settings(call); return
    if data == "menu_help":
        ack(call); render_help(call); return
    if data == "menu_sysinfo":
        ack(call); render_sysinfo(call); return
    if data == "menu_audit":
        ack(call); render_audit(call); return
    if data == "menu_ghbackup":
        ack(call); render_ghbackup(call); return
    if data == "menu_tgbackup":
        ack(call); render_tgbackup(call); return
    if data == "menu_gh":
        ack(call); render_gh_menu(call); return

    # ── per-bot ────────────────────────────────────────────────
    if data.startswith("b_view_"):
        ack(call); render_bot(call, data[7:]); return
    if data.startswith("b_start_"):
        bid = data[8:]
        b = find_bot(bid)
        if not b:
            ack(call, "Not found"); return
        ack(call, "Starting…")
        res = start_child(b)
        if not res.get("ok"):
            ack(call, str(res.get("error"))[:190])
        render_bot(call, bid); return
    if data.startswith("b_stop_"):
        bid = data[7:]
        ack(call, "Stopping…")
        stop_child(bid, manual=True)
        render_bot(call, bid); return
    if data.startswith("b_rst_"):
        bid = data[6:]
        b = find_bot(bid)
        if not b:
            ack(call, "Not found"); return
        ack(call, "Restarting…")
        res = restart_child(b)
        if not res.get("ok"):
            ack(call, str(res.get("error"))[:190])
        render_bot(call, bid); return
    if data.startswith("b_logs_"):
        ack(call); render_logs(call, data[7:]); return
    if data.startswith("b_logfile_"):
        bid = data[10:]
        p = DIRS["logs"] / f"{bid}.log"
        if not p.exists():
            ack(call, "No log file yet"); return
        ack(call, "Sending…")
        with p.open("rb") as fh:
            bot.send_document(cid, fh,
                              caption=f"Log — {fmt_bytes(p.stat().st_size)}",
                              visible_file_name=f"{bid}.log")
        return
    if data.startswith("b_env_"):
        ack(call); render_env(call, data[6:]); return
    if data.startswith("b_envadd_"):
        bid = data[9:]
        USER_STATES[uid] = {"flow": "await_env", "bot_id": bid}
        ack(call)
        bot.send_message(cid,
                         f"{G['key']} Send <code>KEY=VALUE</code>.\n"
                         f"/cancel to abort.", parse_mode="HTML")
        return
    if data.startswith("b_envdel_"):
        bid, _, key = data[9:].partition("::")
        b = find_bot(bid)
        if not b:
            ack(call, "Not found"); return
        (b.setdefault("env", {})).pop(key, None)
        save_bot(b)
        audit("env_del", f"bot={bid} key={key}")
        ack(call, f"Deleted {key}")
        render_env(call, bid); return
    if data.startswith("b_cron_"):
        ack(call); render_cron(call, data[7:]); return
    if data.startswith("b_pip_"):
        bid = data[6:]
        USER_STATES[uid] = {"flow": "await_pip", "bot_id": bid}
        ack(call)
        bot.send_message(cid,
                         f"{G['download']} Send package names, space separated.\n"
                         f"e.g. <code>requests aiohttp python-dotenv</code>\n"
                         f"/cancel to abort.", parse_mode="HTML")
        return
    if data.startswith("b_add_"):
        bid = data[6:]
        USER_STATES[uid] = {"flow": "await_addfile", "bot_id": bid}
        ack(call)
        bot.send_message(cid,
                         f"{G['plus']} Send the file or archive to add to this bot.\n"
                         f"Existing files with the same name are overwritten.\n"
                         f"/cancel to abort.")
        return
    if data.startswith("b_files_"):
        ack(call); render_files(call, data[8:]); return
    if data.startswith("b_dl_"):
        bid = data[5:]
        b = find_bot(bid)
        if not b:
            ack(call, "Not found"); return
        ack(call, "Packaging…")

        def _bg() -> None:
            bot_dir = Path(b["dir"])
            if not bot_dir.exists():
                bot.send_message(cid, f"{G['no']} No files.")
                return
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
                for root, _, files in os.walk(bot_dir):
                    if any(part in _SKIP_DIRS for part in Path(root).parts):
                        continue
                    for fn in files:
                        fp = Path(root) / fn
                        zf.write(fp, arcname=fp.relative_to(bot_dir))
            buf.seek(0)
            name = f"{b['name']}.zip"
            bot.send_document(cid, (name, buf),
                              caption=f"<b>{esc(b['name'])}</b> "
                                      f"({fmt_bytes(buf.getbuffer().nbytes)})",
                              parse_mode="HTML", visible_file_name=name)

        threading.Thread(target=_bg, daemon=True).start()
        return
    if data.startswith("b_del_"):
        bid = data[6:]
        b = find_bot(bid)
        if not b:
            ack(call, "Not found"); return
        ack(call)
        show(cid,
             f"<b>{G['warn']} Delete bot</b>\n{G['div']}\n"
             f"{bullet('Bot', b['name'])}\n"
             f"{bullet('Size', fmt_bytes(dir_size(Path(b['dir']))))}\n\n"
             f"This stops the process and deletes all its files. "
             f"Not reversible unless you have a backup.",
             kb_confirm(f"b_delyes_{bid}", f"b_view_{bid}", "Delete"), call=call)
        return
    if data.startswith("b_delyes_"):
        bid = data[9:]
        b = find_bot(bid)
        if not b:
            ack(call, "Not found"); return
        stop_child(bid, manual=True)
        rmrf(b.get("dir", ""))
        (DIRS["logs"] / f"{bid}.log").unlink(missing_ok=True)
        delete_bot_doc(bid)
        audit("bot_delete", f"bot={bid} name={b.get('name')}")
        ack(call, "Deleted")
        render_bots(call); return

    # ── bulk ops ───────────────────────────────────────────────
    if data == "op_stopall":
        ack(call, "Stopping all…")
        threading.Thread(
            target=lambda: notify(f"{G['ok']} Stopped {op_stop_all()} bot(s)."),
            daemon=True).start()
        return
    if data == "op_restartall":
        ack(call, "Restarting all…")

        def _ra() -> None:
            ok, fail = op_restart_all()
            notify(f"{G['ok']} Restart-all: {ok} ok, {fail} failed.")

        threading.Thread(target=_ra, daemon=True).start()
        return
    if data == "op_clean":
        ack(call, "Cleaning…")

        def _cl() -> None:
            dirs, logs = op_clean_orphans()
            notify(f"{G['ok']} Cleaned {dirs} orphan folder(s), "
                   f"{logs} stale log(s).")

        threading.Thread(target=_cl, daemon=True).start()
        return
    if data == "op_reload":
        cache_clear()
        gh_load_config()
        ack(call, "Caches reloaded")
        render_settings(call); return
    if data == "op_export":
        ack(call, "Building archive…")

        def _ex() -> None:
            try:
                blob = build_backup()
                name = f"backup_{stamp()}.enc"
                bot.send_document(
                    cid, (name, io.BytesIO(blob)),
                    caption=(f"<b>{G['download']} Encrypted export</b>\n"
                             f"{bullet('Size', fmt_bytes(len(blob)))}\n"
                             f"<i>Needs your master key to restore.</i>"),
                    parse_mode="HTML", visible_file_name=name)
                audit("export", f"bytes={len(blob)}")
            except Exception as e:
                notify(f"{G['no']} Export failed: <code>{esc(e)}</code>")

        threading.Thread(target=_ex, daemon=True).start()
        return

    # ── settings ───────────────────────────────────────────────
    if data == "set_auto_toggle":
        cur = bool(get_setting("auto_restart", True))
        set_setting("auto_restart", not cur)
        audit("auto_restart", f"now={not cur}")
        ack(call, f"Auto-restart {'on' if not cur else 'off'}")
        render_settings(call); return
    if data == "set_maxbots":
        USER_STATES[uid] = {"flow": "await_maxbots"}
        ack(call)
        bot.send_message(cid, f"{G['diamond']} Send the max number of bots "
                              f"(current {max_bots()}).")
        return
    if data == "set_showkey":
        ack(call)
        bot.send_message(
            cid,
            f"<b>{G['key']} Master Key</b>\n{G['div']}\n"
            f"<code>{esc(MASTER_KEY.decode())}</code>\n\n"
            f"Store this somewhere safe and set it as the "
            f"<code>MASTER_KEY</code> env var on any new deploy. "
            f"Without it your backups cannot be decrypted.",
            parse_mode="HTML")
        return
    if data == "set_restorefile":
        USER_STATES[uid] = {"flow": "await_restore"}
        ack(call)
        bot.send_message(cid,
                         f"{G['warn']} Send the <code>.enc</code> backup file.\n"
                         f"This <b>replaces</b> the current DB and sandbox.\n"
                         f"/cancel to abort.", parse_mode="HTML")
        return

    # ── GitHub backup config ───────────────────────────────────
    if data == "gh_set_token":
        USER_STATES[uid] = {"flow": "await_gh_token"}
        ack(call)
        bot.send_message(cid, f"{G['key']} Send your GitHub personal access "
                              f"token (repo scope).")
        return
    if data == "gh_set_repo":
        USER_STATES[uid] = {"flow": "await_gh_repo"}
        ack(call)
        bot.send_message(cid, f"{G['diamond']} Send repo as "
                              f"<code>owner/name</code>.", parse_mode="HTML")
        return
    if data == "gh_set_branch":
        USER_STATES[uid] = {"flow": "await_gh_branch"}
        ack(call)
        bot.send_message(cid, f"{G['folder']} Send branch name.")
        return
    if data == "gh_set_interval":
        USER_STATES[uid] = {"flow": "await_gh_interval"}
        ack(call)
        bot.send_message(cid, f"{G['clock']} Send interval in minutes (min 15).")
        return
    if data == "gh_toggle_auto":
        GH["auto"] = not GH["auto"]
        set_setting("gh_auto", GH["auto"])
        ack(call, f"Auto backup {'on' if GH['auto'] else 'off'}")
        render_ghbackup(call); return
    if data == "gh_clear":
        for k in ("gh_token", "gh_repo"):
            set_setting(k, "")
        gh_load_config()
        audit("gh_clear", "")
        ack(call, "GitHub config cleared")
        render_ghbackup(call); return
    if data == "gh_backup":
        ack(call, "Backing up…")

        def _gb() -> None:
            res = gh_backup_now()
            notify(f"<b>{G['ok'] if res.get('ok') else G['no']} GitHub backup</b>\n"
                   f"{bullet('Size', res.get('mb', '—') + ' MB' if res.get('ok') else '—')}\n"
                   f"{bullet('Error', res.get('error') or 'none')}")

        threading.Thread(target=_gb, daemon=True).start()
        return
    if data == "gh_restore":
        ack(call)
        show(cid,
             f"<b>{G['warn']} Restore from GitHub</b>\n{G['div']}\n"
             f"This wipes the current DB and sandbox, then extracts "
             f"<code>backups/latest.enc</code>.\n"
             f"Running bots are stopped first.",
             kb_confirm("gh_restore_yes", "menu_ghbackup", "Restore"), call=call)
        return
    if data == "gh_restore_yes":
        ack(call, "Restoring…")

        def _gr() -> None:
            op_stop_all()
            res = gh_restore_latest()
            notify(f"<b>{G['ok'] if res.get('ok') else G['no']} Restore</b>\n"
                   f"{bullet('Size', fmt_bytes(res.get('size', 0)))}\n"
                   f"{bullet('Error', res.get('error') or 'none')}\n"
                   f"{'Send /start to reload the panel.' if res.get('ok') else ''}")

        threading.Thread(target=_gr, daemon=True).start()
        return

    # ── Telegram backup config ─────────────────────────────────
    if data == "tg_set_chat":
        USER_STATES[uid] = {"flow": "await_tg_chat"}
        ack(call)
        bot.send_message(cid, f"{G['cloud']} Send the channel handle or id "
                              f"(<code>@name</code> or <code>-100…</code>). "
                              f"Add this bot as admin there first.",
                         parse_mode="HTML")
        return
    if data == "tg_set_interval":
        USER_STATES[uid] = {"flow": "await_tg_interval"}
        ack(call)
        bot.send_message(cid, f"{G['clock']} Send interval in hours (min 1).")
        return
    if data == "tg_toggle_auto":
        cur = bool(get_setting("tg_backup_auto", False))
        set_setting("tg_backup_auto", not cur)
        ack(call, f"Auto TG backup {'on' if not cur else 'off'}")
        render_tgbackup(call); return
    if data == "tg_clear":
        set_setting("tg_backup_chat", None)
        set_setting("tg_backup_auto", False)
        ack(call, "Cleared")
        render_tgbackup(call); return
    if data == "tg_backup":
        ack(call, "Sending backup…")

        def _tb() -> None:
            res = tg_backup_now()
            notify(f"<b>{G['ok'] if res.get('ok') else G['no']} TG backup</b>\n"
                   f"{bullet('Size', fmt_bytes(res.get('size', 0)))}\n"
                   f"{bullet('Error', res.get('error') or 'none')}")

        threading.Thread(target=_tb, daemon=True).start()
        return

    # ── GitHub source ──────────────────────────────────────────
    if data == "gh_clone":
        USER_STATES[uid] = {"flow": "await_clone_url"}
        ack(call)
        bot.send_message(cid,
                         f"{G['cloud']} Send the repo URL.\n"
                         f"<code>https://github.com/user/repo</code>\n"
                         f"/cancel to abort.", parse_mode="HTML")
        return
    if data == "gh_browse":
        if not GH["repo"]:
            USER_STATES[uid] = {"flow": "await_browse_repo"}
            ack(call)
            bot.send_message(cid, f"{G['folder']} Send repo as "
                                  f"<code>owner/name</code>.", parse_mode="HTML")
            return
        gh_browse(call, str(GH["repo"]), ""); return
    if data == "ghb_up":
        st = USER_STATES.get(uid, {})
        parent = "/".join(str(st.get("gh_path", "")).rstrip("/").split("/")[:-1])
        gh_browse(call, str(st.get("gh_repo", "")), parent); return
    if data == "ghb_folder":
        st = USER_STATES.get(uid, {})
        gh_browse(call, str(st.get("gh_repo", "")), str(st.get("gh_path", ""))); return
    if data.startswith("ghb_i_"):
        st = USER_STATES.get(uid, {})
        items = st.get("gh_items") or []
        try:
            item = items[int(data[6:])]
        except Exception:
            ack(call, "Stale menu — reopen"); return
        repo = str(st.get("gh_repo", ""))
        if item.get("type") == "dir":
            gh_browse(call, repo, item["path"])
        else:
            gh_view_file(call, repo, item["path"])
        return
    if data == "ghb_deploy":
        st = USER_STATES.get(uid, {})
        gh_deploy_file(call, str(st.get("gh_repo", "")), str(st.get("gh_file", "")))
        return
    if data == "ghb_dl":
        st = USER_STATES.get(uid, {})
        gh_download_file(call, str(st.get("gh_repo", "")), str(st.get("gh_file", "")))
        return

    ack(call, "?")


# ═════════════════════════════════════════════════════════════════
# 16. MESSAGE HANDLERS
# ═════════════════════════════════════════════════════════════════

def _private(m: types.Message) -> bool:
    try:
        return m.chat.type == "private"
    except Exception:
        return True


def _claim_owner(m: types.Message) -> None:
    global OWNER_ID
    if OWNER_ID > 0:
        return
    stored = int(get_setting("owner_id", 0) or 0)
    if stored > 0:
        OWNER_ID = stored
        return
    OWNER_ID = m.from_user.id
    set_setting("owner_id", OWNER_ID)
    audit("owner_claim", f"uid={OWNER_ID}")
    bot.send_message(
        m.chat.id,
        f"<b>{G['star']} You are now the panel owner</b>\n{G['div']}\n"
        f"{bullet('Owner ID', OWNER_ID)}\n"
        f"Set <code>OWNER_ID={OWNER_ID}</code> as an env var to lock this in.\n\n"
        f"<b>{G['key']} Master key</b> (for backups — save it):\n"
        f"<code>{esc(MASTER_KEY.decode())}</code>",
        parse_mode="HTML")


@bot.message_handler(commands=["start", "menu"])
def cmd_start(m: types.Message) -> None:
    if not _private(m):
        return
    _claim_owner(m)
    if not is_owner(m.from_user.id):
        return
    render_main(m.chat.id, intro=f"Welcome back, "
                                 f"<b>{esc(m.from_user.first_name or 'friend')}</b>.")


@bot.message_handler(commands=["help"])
def cmd_help(m: types.Message) -> None:
    if not _private(m) or not is_owner(m.from_user.id):
        return
    kb = kb_back("menu_main")
    bot.send_message(m.chat.id, "Opening help…", reply_markup=kb)
    render_main(m.chat.id)


@bot.message_handler(commands=["id"])
def cmd_id(m: types.Message) -> None:
    if not _private(m):
        return
    bot.reply_to(m, f"<code>{m.from_user.id}</code>", parse_mode="HTML")


@bot.message_handler(commands=["cancel"])
def cmd_cancel(m: types.Message) -> None:
    if not _private(m) or not is_owner(m.from_user.id):
        return
    USER_STATES.pop(m.from_user.id, None)
    bot.reply_to(m, f"{G['ok']} Cancelled.")


@bot.message_handler(commands=["status"])
def cmd_status(m: types.Message) -> None:
    if not _private(m) or not is_owner(m.from_user.id):
        return
    live = [(bid, i) for bid, i in list(RUNNING.items())
            if i["proc"].poll() is None]
    rows = "\n".join(
        f"{G['play']} <b>{esc(i['name'][:24])}</b> — "
        f"{fmt_dur(int(time.time() * 1000 - i['started']))}"
        for _, i in live
    ) or "<i>Nothing running.</i>"
    bot.reply_to(m, f"<b>{G['graph']} Status</b>\n{G['div']}\n"
                    f"{bullet('Bots', len(list_bots()))}\n"
                    f"{bullet('Running', len(live))}\n{G['div']}\n{rows}",
                 parse_mode="HTML")


@bot.message_handler(commands=["backup"])
def cmd_backup(m: types.Message) -> None:
    if not _private(m) or not is_owner(m.from_user.id):
        return
    bot.reply_to(m, f"{G['refresh']} Building backup…")

    def _bg() -> None:
        parts = []
        if gh_enabled():
            r = gh_backup_now()
            parts.append(f"GitHub: {'ok' if r.get('ok') else r.get('error')}")
        if tg_backup_chat():
            r = tg_backup_now()
            parts.append(f"Telegram: {'ok' if r.get('ok') else r.get('error')}")
        if not parts:
            parts.append("No backup destination configured.")
        bot.send_message(m.chat.id, f"{G['ok']} " + " | ".join(parts))

    threading.Thread(target=_bg, daemon=True).start()


@bot.message_handler(commands=["key"])
def cmd_key(m: types.Message) -> None:
    if not _private(m) or not is_owner(m.from_user.id):
        return
    bot.reply_to(m, f"<b>{G['key']} Master Key</b>\n"
                    f"<code>{esc(MASTER_KEY.decode())}</code>\n\n"
                    f"Save it. Backups are useless without it.",
                 parse_mode="HTML")


@bot.message_handler(content_types=["document"])
def on_document(m: types.Message) -> None:
    if not _private(m) or not is_owner(m.from_user.id):
        return
    st = USER_STATES.get(m.from_user.id) or {}
    flow = st.get("flow", "")

    if flow == "await_restore":
        USER_STATES.pop(m.from_user.id, None)
        doc = m.document
        try:
            f = bot.get_file(doc.file_id)
            blob = bot.download_file(f.file_path)
        except Exception as e:
            bot.reply_to(m, f"{G['no']} download failed: <code>{esc(e)}</code>",
                         parse_mode="HTML")
            return
        bot.reply_to(m, f"{G['refresh']} Restoring…")

        def _bg() -> None:
            op_stop_all()
            res = restore_backup(blob, wipe=True)
            audit("restore_file", f"ok={res.get('ok')}")
            bot.send_message(
                m.chat.id,
                f"<b>{G['ok'] if res.get('ok') else G['no']} Restore</b>\n"
                f"{bullet('Size', fmt_bytes(res.get('size', 0)))}\n"
                f"{bullet('Error', res.get('error') or 'none')}\n"
                + ("Send /start to reload." if res.get("ok") else ""),
                parse_mode="HTML")

        threading.Thread(target=_bg, daemon=True).start()
        return

    if flow == "await_addfile":
        USER_STATES.pop(m.from_user.id, None)
        handle_upload(m, target_bot_id=str(st.get("bot_id")))
        return

    handle_upload(m)


@bot.message_handler(content_types=["text"])
def on_text(m: types.Message) -> None:
    if not _private(m) or not is_owner(m.from_user.id):
        return
    text = (m.text or "").strip()
    if text.startswith("/"):
        return
    uid = m.from_user.id
    st = USER_STATES.get(uid) or {}
    flow = st.get("flow", "")
    if not flow:
        return

    try:
        # ── env var ────────────────────────────────────────────
        if flow == "await_env":
            if "=" not in text:
                bot.reply_to(m, f"{G['no']} Use <code>KEY=VALUE</code>.",
                             parse_mode="HTML")
                return
            key, _, val = text.partition("=")
            key, val = key.strip(), val.strip()
            if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", key):
                bot.reply_to(m, f"{G['no']} Invalid key name.")
                return
            if key in SECRET_ENV_NAMES:
                bot.reply_to(m, f"{G['no']} <code>{esc(key)}</code> is reserved "
                                f"by the panel.", parse_mode="HTML")
                return
            b = find_bot(str(st.get("bot_id")))
            if not b:
                bot.reply_to(m, f"{G['no']} Bot not found.")
            else:
                b.setdefault("env", {})[key] = val
                save_bot(b)
                audit("env_set", f"bot={b['_id']} key={key}")
                bot.reply_to(m, f"{G['ok']} <code>{esc(key)}</code> saved. "
                                f"Restart the bot to apply.", parse_mode="HTML")
            USER_STATES.pop(uid, None)
            return

        # ── pip install ────────────────────────────────────────
        if flow == "await_pip":
            USER_STATES.pop(uid, None)
            pkgs = [p for p in text.split() if p]
            bad = [p for p in pkgs
                   if p.startswith("-")
                   or not re.match(r"^[A-Za-z0-9_.\-\[\]=<>!~,+]+$", p)]
            if bad:
                bot.reply_to(m, f"{G['no']} Invalid spec: "
                                f"<code>{esc(' '.join(bad))}</code>",
                             parse_mode="HTML")
                return
            if not pkgs or len(pkgs) > 15:
                bot.reply_to(m, f"{G['no']} Send 1–15 package names.")
                return
            b = find_bot(str(st.get("bot_id")))
            if not b:
                bot.reply_to(m, f"{G['no']} Bot not found.")
                return
            status = bot.reply_to(m, f"{G['refresh']} Installing "
                                     f"<code>{esc(' '.join(pkgs))}</code>…",
                                  parse_mode="HTML")

            def _bg() -> None:
                deps = Path(b["dir"]) / ".deps"
                deps.mkdir(parents=True, exist_ok=True)
                try:
                    r = subprocess.run(
                        [sys.executable, "-m", "pip", "install",
                         "--target", str(deps), *_PIP_FLAGS, *pkgs],
                        capture_output=True, text=True, timeout=600,
                        env=_pip_env(),
                    )
                    out = ((r.stdout or "") + "\n" + (r.stderr or "")).strip()
                    tail = "\n".join(
                        ln for ln in out.splitlines() if ln.strip())[-1200:]
                    head = (f"{G['ok']} Installed" if r.returncode == 0
                            else f"{G['no']} Install failed")
                    audit("pip_install",
                          f"bot={b['_id']} pkgs={' '.join(pkgs)} rc={r.returncode}")
                    body = (f"<b>{head}</b>\n{G['div']}\n"
                            f"<pre>{esc(tail) or '(no output)'}</pre>")
                except subprocess.TimeoutExpired:
                    body = f"{G['no']} Install timed out."
                except Exception as e:
                    body = f"{G['no']} Error: <code>{esc(e)}</code>"
                try:
                    bot.edit_message_text(body, chat_id=status.chat.id,
                                          message_id=status.message_id,
                                          parse_mode="HTML")
                except Exception:
                    bot.send_message(m.chat.id, body, parse_mode="HTML")

            threading.Thread(target=_bg, daemon=True).start()
            return

        # ── cron ───────────────────────────────────────────────
        if flow == "await_cron":
            b = find_bot(str(st.get("bot_id")))
            if not b:
                USER_STATES.pop(uid, None)
                bot.reply_to(m, f"{G['no']} Bot not found.")
                return
            low = text.lower()
            if low == "off":
                b["cron"] = {}
                save_bot(b)
                USER_STATES.pop(uid, None)
                bot.reply_to(m, f"{G['ok']} Cron disabled.")
                return
            cron = b.get("cron") or {}
            changed = False
            for tok in low.split():
                if "=" not in tok:
                    continue
                k, v = tok.split("=", 1)
                if k != "restart":
                    continue
                try:
                    n = int(v)
                except ValueError:
                    continue
                if n > 0:
                    cron["restart_hours"] = n
                    changed = True
            if not changed:
                bot.reply_to(m, f"{G['no']} Use <code>restart=6</code> or "
                                f"<code>off</code>.", parse_mode="HTML")
                return
            b["cron"] = cron
            save_bot(b)
            audit("cron_set", f"bot={b['_id']} {cron}")
            USER_STATES.pop(uid, None)
            bot.reply_to(m, f"{G['ok']} Cron: <code>{esc(json.dumps(cron))}</code>",
                         parse_mode="HTML")
            return

        # ── settings ───────────────────────────────────────────
        if flow == "await_maxbots":
            USER_STATES.pop(uid, None)
            try:
                n = max(1, min(500, int(text)))
            except ValueError:
                bot.reply_to(m, f"{G['no']} Send a number.")
                return
            set_setting("max_bots", n)
            audit("max_bots", str(n))
            bot.reply_to(m, f"{G['ok']} Max bots: {n}")
            return

        # ── GitHub backup config ───────────────────────────────
        if flow == "await_gh_token":
            set_setting("gh_token", text)
            gh_load_config()
            USER_STATES.pop(uid, None)
            audit("gh_token_set", "")
            bot.reply_to(m, f"{G['ok']} Token saved.")
            try:
                bot.delete_message(m.chat.id, m.message_id)
            except Exception:
                pass
            return
        if flow == "await_gh_repo":
            if "/" not in text:
                bot.reply_to(m, f"{G['no']} Use <code>owner/name</code>.",
                             parse_mode="HTML")
                return
            set_setting("gh_repo", text)
            gh_load_config()
            USER_STATES.pop(uid, None)
            bot.reply_to(m, f"{G['ok']} Repo: <code>{esc(text)}</code>",
                         parse_mode="HTML")
            return
        if flow == "await_gh_branch":
            set_setting("gh_branch", text or "main")
            gh_load_config()
            USER_STATES.pop(uid, None)
            bot.reply_to(m, f"{G['ok']} Branch: <code>{esc(text)}</code>",
                         parse_mode="HTML")
            return
        if flow == "await_gh_interval":
            USER_STATES.pop(uid, None)
            try:
                n = max(15, int(text))
            except ValueError:
                n = 360
            set_setting("gh_interval_min", n)
            gh_load_config()
            bot.reply_to(m, f"{G['ok']} Interval: {n} min")
            return

        # ── Telegram backup config ─────────────────────────────
        if flow == "await_tg_chat":
            USER_STATES.pop(uid, None)
            set_setting("tg_backup_chat", text)
            audit("tg_chat_set", text)
            res = tg_backup_now()
            bot.reply_to(m, f"{G['ok']} Chat set. Test backup: "
                            f"{'sent' if res.get('ok') else res.get('error')}")
            return
        if flow == "await_tg_interval":
            USER_STATES.pop(uid, None)
            try:
                n = max(1, int(text))
            except ValueError:
                n = 6
            set_setting("tg_backup_interval_h", n)
            bot.reply_to(m, f"{G['ok']} Interval: {n} h")
            return

        # ── GitHub source ──────────────────────────────────────
        if flow == "await_clone_url":
            USER_STATES.pop(uid, None)
            if not text.startswith(("https://", "git@", "http://")):
                bot.reply_to(m, f"{G['no']} Send a valid repo URL.")
                return
            bot.reply_to(m, f"{G['refresh']} Cloning… this can take a minute.")
            gh_clone_repo(text, m.chat.id)
            return
        if flow == "await_browse_repo":
            USER_STATES.pop(uid, None)
            if "/" not in text:
                bot.reply_to(m, f"{G['no']} Use <code>owner/name</code>.",
                             parse_mode="HTML")
                return
            kb = types.InlineKeyboardMarkup()
            kb.add(Btn(f"{G['folder']}  Browse {text[:28]}",
                       callback_data="gh_browse", style="primary"))
            set_setting("gh_repo", text)
            gh_load_config()
            bot.reply_to(m, f"{G['ok']} Repo set to <code>{esc(text)}</code>.",
                         parse_mode="HTML", reply_markup=kb)
            return

    except Exception as e:
        traceback.print_exc()
        USER_STATES.pop(uid, None)
        bot.reply_to(m, f"{G['no']} Error: <code>{esc(e)}</code>",
                     parse_mode="HTML")


# ═════════════════════════════════════════════════════════════════
# 17. CRON RUNNER
# ═════════════════════════════════════════════════════════════════

def cron_runner() -> None:
    last: Dict[str, float] = {}
    while True:
        try:
            now = time.time()
            for bid, b in db_load_ro()["bots"].items():
                hours = (b.get("cron") or {}).get("restart_hours")
                if not hours:
                    continue
                iv = int(hours) * 3600
                if now - last.get(bid, 0) >= iv:
                    if last.get(bid):  # skip the first pass after boot
                        doc = find_bot(bid)
                        if doc:
                            print(f"[cron] restarting {doc['name']}", flush=True)
                            restart_child(doc)
                    last[bid] = now
        except Exception:
            traceback.print_exc()
        time.sleep(60)


def janitor() -> None:
    while True:
        try:
            time.sleep(6 * 3600)
            dirs, logs = op_clean_orphans()
            if dirs or logs:
                print(f"[janitor] cleaned {dirs} dirs, {logs} logs", flush=True)
        except Exception:
            pass


# ═════════════════════════════════════════════════════════════════
# 18. BOOT
# ═════════════════════════════════════════════════════════════════

def _singleton_lock() -> Optional[Any]:
    """Prevent two panel instances polling the same token."""
    try:
        import fcntl
    except ImportError:
        return None
    path = DIRS["data"] / "panel.lock"
    try:
        fh = open(path, "w")
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        fh.write(str(os.getpid()))
        fh.flush()
        return fh
    except OSError:
        sys.exit(f"[x] another instance is already running (lock: {path})")


def banner() -> None:
    line = "=" * 60
    print(line)
    print(f"  {BRAND_TAG}")
    print(f"  owner id      : {OWNER_ID or '(unclaimed — first /start wins)'}")
    print(f"  keepalive port: {KEEPALIVE_PORT}")
    print(f"  github backup : {'on' if gh_enabled() else 'off'}")
    print(f"  telegram bkp  : {tg_backup_chat() or 'off'}")
    print(f"  master key    : {'from env' if os.environ.get('MASTER_KEY') else 'local file'}")
    print(line, flush=True)


def main() -> int:
    global OWNER_ID, _LOCK_FH
    _LOCK_FH = _singleton_lock()

    if OWNER_ID <= 0:
        stored = int(get_setting("owner_id", 0) or 0)
        if stored > 0:
            OWNER_ID = stored

    gh_load_config()
    banner()

    try:
        res = gh_auto_restore_on_boot()
        if res and res.get("ok"):
            print(f"[boot] restored backup ({fmt_bytes(res.get('size', 0))})",
                  flush=True)
    except Exception as e:
        print(f"[boot] restore skipped: {e}", flush=True)

    threading.Thread(target=gh_auto_loop, daemon=True, name="gh-backup").start()
    threading.Thread(target=tg_backup_loop, daemon=True, name="tg-backup").start()
    threading.Thread(target=cron_runner, daemon=True, name="cron").start()
    threading.Thread(target=janitor, daemon=True, name="janitor").start()
    _start_keepalive()

    try:
        bot.set_my_commands([
            types.BotCommand("start",  "open the panel"),
            types.BotCommand("menu",   "open the panel"),
            types.BotCommand("status", "running bots"),
            types.BotCommand("backup", "backup now"),
            types.BotCommand("key",    "show master key"),
            types.BotCommand("id",     "your telegram id"),
            types.BotCommand("cancel", "cancel current prompt"),
        ])
    except Exception:
        pass

    notify(f"<b>{G['ok']} Panel online</b>\n"
           f"{bullet('Bots', len(list_bots()))}\n"
           f"{bullet('Started', fmt_ts(ts_iso()))}")

    for b in list_bots():
        if b.get("status") in ("running", "crashed"):
            try:
                b["crash_count"] = 0
                save_bot(b)
                start_child(b)
            except Exception as e:
                print(f"[boot] autostart {b['name']} failed: {e}", flush=True)

    # A stale webhook would deliver every update twice alongside polling.
    try:
        bot.remove_webhook()
        try:
            bot.delete_webhook(drop_pending_updates=True)
        except Exception:
            pass
        print("[bot] webhook cleared", flush=True)
    except Exception as e:
        print(f"[bot] webhook clear warning: {e}", flush=True)

    print("[bot] polling…", flush=True)
    while True:
        try:
            bot.infinity_polling(skip_pending=True, timeout=30,
                                 long_polling_timeout=25)
        except KeyboardInterrupt:
            print("\n[bot] stopping…", flush=True)
            for bid in list(RUNNING.keys()):
                stop_child(bid, manual=True)
            return 0
        except Exception as e:
            print(f"[bot] poll error: {e}", flush=True)
            time.sleep(5)


if __name__ == "__main__":
    sys.exit(main())