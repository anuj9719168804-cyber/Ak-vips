"""Diskwala fallback resolver for vidbunker.in links (async port of the fbot-main/diskwala.py vidbunker path).

Used by vidbunker_api.fetch_vidbunker when the primary worker API fails. Two tiers, tried in order:

  1. Token API  - POST api2.diskwala.net/api/vidbunker/download ({"id": ...}, then {"link": ...}), poll .../status,
                  decrypt the AES-GCM reply. Needs a bearer token, taken from (first match wins):
                    * DISKWALA_TOKEN env (static token, handy for testing / manual override)
                    * a Telethon user session (SESSION env) that opens the "sky577bot" mini app (cached 1h)
                  Skipped silently when neither is configured.
  2. No-auth    - scrape the vidbunker.in watch page (<video>/<source>, embedded JSON, __NEXT_DATA__) and, if that
                  finds nothing, the internal temp_info endpoints.

Everything is best-effort and raises DiskwalaError with the real reason when all tiers fail.
"""
import asyncio
import json
import logging
import os
import re
import time
from typing import Optional
from urllib.parse import quote, urljoin, urlparse

import aiohttp

log = logging.getLogger("diskwala_api")

VIDBUNKER_API_DOWNLOAD = os.getenv("DISKWALA_VB_DOWNLOAD", "https://api2.diskwala.net/api/vidbunker/download").strip()
VIDBUNKER_API_STATUS = os.getenv("DISKWALA_VB_STATUS", "https://api2.diskwala.net/api/vidbunker/status").strip()
TEMP_INFO_ENDPOINTS = (
    "https://api2.diskwala.net/api/v1/file/temp_info",
    "https://api2.diskwala.net/api/vidbunker/temp_info",
)
# Public constant of the Diskwala web client (not a secret of ours) used to decrypt the {"_x","s","p","h"} replies.
ENCRYPTION_KEY = "e7109544dab612bd5b80b8a427ac474ba5541b9efff7a4ca1c8ef85df2489c23"

DISKWALA_ENABLED = os.getenv("DISKWALA_FALLBACK", "1").strip().lower() not in ("0", "false", "no", "off")
DISKWALA_TIMEOUT = max(20, int(os.getenv("DISKWALA_TIMEOUT", "75")))  # whole fallback (both tiers), seconds
STATIC_TOKEN = os.getenv("DISKWALA_TOKEN", "").strip()
SESSION = os.getenv("SESSION", "1AZWarzcBu05VzVtvhcIZvE8HBtYfT3K6JUeR9n1kvua24ufHs6A-blFqfztzBwgdpBjs7YThEepbfT_JgLZ44l_LnDwD-vSybauAfGu5ccJxnoVMqORpTNgx8j-M9ynKSvSO2wp9b1XBTVZiHjLDYwYe6b0qArzrUFr0X4o5sg_IZeM2rS6Gpla2CHmrfww2_6dmh7Ca9uc3K00Oh1au_AArOikG_drgACfOc4EG5FwWRlZoJIx8OXnFQ_AREuQoKSLAaRxNqWyuPVNURxhE6cq7dzdzmuAW2pHxkl9flUoYDZ7hBNrLDh_G638zTM1gy6C98W4XNnIN7T-LYmkqwnJTOH5_FuE=").strip()  # Telethon StringSession of a user account
TG_API_ID = os.getenv("DISKWALA_API_ID") or os.getenv("API_ID") or "33029767"
TG_API_HASH = os.getenv("DISKWALA_API_HASH") or os.getenv("API_HASH") or "5d897bed11bc8b062a12f6c1c3c5360a"

UA = "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Mobile Safari/537.36"
_ID_RE = re.compile(r"vidbunker\.[a-z]{2,}/watch/([A-Za-z0-9_-]+)", re.I)
_MEDIA_RE = r"https?://[^\s\"'<>\\]+\.(?:mp4|mkv|webm|m3u8)[^\s\"'<>\\]*"
_URL_KEYS = ("downloadUrl", "download_url", "directUrl", "direct_url", "contentUrl", "content_url",
             "streamUrl", "stream_url", "videoUrl", "video_url", "url", "link")


class DiskwalaError(Exception):
    pass


class DiskwalaAuthError(DiskwalaError):
    """Bearer token rejected (HTTP 401/403): a fresh token, not a retry, is needed."""


def extract_id(watch_url: str) -> Optional[str]:
    m = _ID_RE.search(watch_url or "")
    return m.group(1) if m else None


def token_tier_available() -> bool:
    return bool(STATIC_TOKEN or (SESSION and TG_API_ID and TG_API_HASH))


# ------------------------------------------------------------------- token --
_auth = {"token": None, "expires": 0.0}
_tg_client = None
_tg_lock: Optional[asyncio.Lock] = None


def _invalidate_token():
    _auth["token"], _auth["expires"] = None, 0.0


async def get_token() -> str:
    global _tg_client, _tg_lock
    if STATIC_TOKEN:
        return STATIC_TOKEN
    if _auth["token"] and time.time() < _auth["expires"]:
        return _auth["token"]
    if not (SESSION and TG_API_ID and TG_API_HASH):
        raise DiskwalaError("no token source configured (set DISKWALA_TOKEN or SESSION + API_ID/API_HASH)")
    if _tg_lock is None:
        _tg_lock = asyncio.Lock()
    async with _tg_lock:
        if _auth["token"] and time.time() < _auth["expires"]:
            return _auth["token"]
        try:
            from telethon import TelegramClient
            from telethon.sessions import StringSession
            from telethon.tl.functions.messages import RequestAppWebViewRequest
            from telethon.tl.types import DataJSON, InputBotAppShortName, InputPeerSelf
        except ImportError as e:
            raise DiskwalaError(f"telethon not installed: {e}")
        from urllib.parse import unquote
        if _tg_client is None:
            _tg_client = TelegramClient(StringSession(SESSION), int(TG_API_ID), TG_API_HASH)
        if not _tg_client.is_connected():
            await _tg_client.connect()
        bot = await _tg_client.get_input_entity("sky577bot")
        r = await _tg_client(RequestAppWebViewRequest(
            peer=InputPeerSelf(), app=InputBotAppShortName(bot_id=bot, short_name="open"),
            platform="android", write_allowed=True, start_param="", theme_params=DataJSON("{}")))
        try:
            frag = urlparse(r.url).fragment
            token = unquote(frag.split("tgWebAppData=", 1)[1].split("&tgWebAppVersion=", 1)[0])
        except Exception:
            raise DiskwalaError("could not read token from mini app url")
        _auth["token"], _auth["expires"] = token, time.time() + 3600
        return token


def decrypt_file(fd: dict) -> dict:
    """AES-GCM reply decrypt. The API has used two byte orders (ct+tag, and p+h as one blob): try both."""
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    except ImportError as e:
        raise DiskwalaError(f"cryptography not installed: {e}")
    aes = AESGCM(bytes.fromhex(ENCRYPTION_KEY))
    iv, p, h = bytes.fromhex(fd["s"]), bytes.fromhex(fd["p"]), bytes.fromhex(fd["h"])
    last: Exception = ValueError("decrypt failed")
    for blob in (p + h, h + p):
        try:
            return json.loads(aes.decrypt(iv, blob, None).decode("utf-8"))
        except Exception as e:
            last = e
    raise DiskwalaError(f"AES-GCM decryption failed: {last}")


# ---------------------------------------------------------------- helpers --
def _pick(d: dict, *keys):
    for k in keys:
        v = d.get(k)
        if v not in (None, ""):
            return v
    return None


def _to_int(v) -> int:
    try:
        if isinstance(v, str) and ":" in v:
            return sum(int(p) * 60 ** i for i, p in enumerate(reversed(v.split(":"))))
        return int(float(v))
    except Exception:
        return 0


def _usable(link) -> bool:
    """A real media/download link, not an echo of the vidbunker watch page."""
    if not (isinstance(link, str) and link.startswith("http")):
        return False
    u = urlparse(link)
    return not ((u.hostname or "").endswith("vidbunker.in") and "/watch/" in u.path.lower())


def _norm(file: dict, extra: Optional[dict] = None) -> dict:
    meta = {**file, **(extra or {})}
    link = next((file[k] for k in _URL_KEYS if _usable(file.get(k))), None)
    if not link:
        raise DiskwalaError(f"no download link in reply: {str(file)[:200]}")
    return {
        "link": link,
        "filename": _pick(file, "name", "fileName", "filename", "title"),
        "size": _to_int(_pick(file, "size", "fileSize", "length")),
        "thumbnail": next((meta[k] for k in ("thumb", "thumbnail", "thumbnailUrl", "poster", "image")
                           if isinstance(meta.get(k), str) and meta[k].startswith("http")), None),
        "duration": _to_int(_pick(meta, "duration", "duration_seconds", "durationSeconds", "video_duration")),
    }


# ------------------------------------------------------- tier 1: token API --
async def _token_resolve(watch_url: str, session: aiohttp.ClientSession) -> dict:
    token = await get_token()
    try:
        return await _token_resolve_with(watch_url, session, token)
    except DiskwalaAuthError as e:
        if STATIC_TOKEN:
            raise
        log.warning("diskwala token rejected (%s), refreshing once", e)
        _invalidate_token()
        return await _token_resolve_with(watch_url, session, await get_token())


async def _token_resolve_with(watch_url: str, session: aiohttp.ClientSession, token: str) -> dict:
    headers = {
        "Authorization": f"Bearer {token}", "X-Bot-Id": "diskwala", "Content-Type": "application/json",
        "Origin": "https://miniapp.diskwala.net", "Referer": "https://miniapp.diskwala.net/", "User-Agent": UA,
    }
    vid = extract_id(watch_url)
    payloads = [{"id": vid}, {"link": watch_url}] if vid else [{"link": watch_url}]
    data, ident = {}, watch_url
    for pl in payloads:  # the vidbunker routes key off the bare share id; the full url is the old shape
        async with session.post(VIDBUNKER_API_DOWNLOAD, headers=headers, json=pl,
                                timeout=aiohttp.ClientTimeout(total=30, connect=15)) as r:
            if r.status in (401, 403):
                raise DiskwalaAuthError(f"HTTP {r.status}")
            try:
                data = await r.json(content_type=None)
            except Exception:
                data = {"ok": False, "error": f"non-JSON reply (HTTP {r.status})"}
        if isinstance(data, dict) and data.get("ok"):
            ident = pl.get("id") or pl.get("link")
            break
    if not (isinstance(data, dict) and data.get("ok")):
        raise DiskwalaError(str((data or {}).get("error") or f"API error: {str(data)[:150]}"))

    status_url = f"{VIDBUNKER_API_STATUS}?link={quote(ident, safe='')}"
    interval = 0.5
    for _ in range(60):
        async with session.get(status_url, headers=headers, timeout=aiohttp.ClientTimeout(total=30, connect=15)) as r:
            if r.status in (401, 403):
                raise DiskwalaAuthError(f"HTTP {r.status} while polling")
            try:
                data = await r.json(content_type=None)
            except Exception:
                raise DiskwalaError(f"status: non-JSON reply (HTTP {r.status})")
        if not data.get("ok"):
            raise DiskwalaError(str(data.get("error") or f"status error: {str(data)[:150]}"))
        st = str(data.get("status", "")).lower()
        if st == "pending":
            await asyncio.sleep(interval)
            interval = min(interval * 1.5, 2.0)
            continue
        if st == "done":
            file = data.get("file")
            if not isinstance(file, dict) or not file:
                raise DiskwalaError(f"no file returned: {str(data)[:150]}")
            if file.get("_x"):
                file = decrypt_file(file)
            return _norm(file, {k: v for k, v in data.items() if k not in ("file", "ok", "status")})
        raise DiskwalaError(f"unexpected status {st!r}")
    raise DiskwalaError("timed out waiting for diskwala status")


# ---------------------------------------------------------- tier 2: no auth --
def _scan_page(html_text: str, base: str) -> dict:
    """Pull (link, name, thumb) out of a watch page without any API."""
    name = thumb = link = None
    m = re.search(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)', html_text, re.I)
    name = m.group(1).strip() if m else None
    m = re.search(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)', html_text, re.I)
    thumb = urljoin(base, m.group(1).strip()) if m else None

    m = re.search(r'<(?:video|source)[^>]+src=["\']([^"\']+)', html_text, re.I)
    if m:
        link = urljoin(base, m.group(1).strip())
    if not link:
        m = re.search(r'"(?:downloadUrl|download_url|directUrl|direct_url|contentUrl|content_url)"\s*:\s*"([^"]+)"', html_text)
        if m:
            link = urljoin(base, m.group(1).replace("\\/", "/"))
    nd_text = ""
    m = re.search(r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(\{.*?\})</script>', html_text, re.S)
    if m:
        try:
            nd_text = json.dumps(json.loads(m.group(1)))
        except Exception:
            nd_text = m.group(1)
    if not link and nd_text:
        for pat in (r'"(?:%s)"\s*:\s*"(%s)"' % ("|".join(_URL_KEYS + ("src", "source", "file")), _MEDIA_RE),):
            mm = re.search(pat, nd_text, re.I)
            if mm:
                link = mm.group(1).replace("\\/", "/")
        if not name:
            mm = re.search(r'"(?:title|name|filename)"\s*:\s*"([^"]{3,200})"', nd_text)
            name = mm.group(1) if mm else None
        if not thumb:
            mm = re.search(r'"(?:thumbnail|thumb|poster|image|cover)"\s*:\s*"(https?://[^"]+)"', nd_text)
            thumb = mm.group(1).replace("\\/", "/") if mm else None
    if not link:
        mm = re.search(_MEDIA_RE, html_text)
        link = mm.group(0) if mm else None
    return {"link": link if _usable(link) else None, "name": name, "thumb": thumb}


async def _temp_info(vid: str, session: aiohttp.ClientSession, headers: dict) -> dict:
    for ep in TEMP_INFO_ENDPOINTS:
        try:
            async with session.post(ep, json={"id": vid}, headers={**headers, "Accept": "application/json"},
                                    timeout=aiohttp.ClientTimeout(total=15, connect=10)) as r:
                if r.status != 200:
                    continue
                data = await r.json(content_type=None)
        except Exception:
            continue
        if not isinstance(data, dict):
            continue
        d = data.get("data") if isinstance(data.get("data"), dict) else data
        if any(_usable(d.get(k)) for k in _URL_KEYS):
            try:
                return _norm(d)
            except DiskwalaError:
                continue
    return {}


async def _html_resolve(watch_url: str, session: aiohttp.ClientSession) -> dict:
    headers = {
        "User-Agent": UA, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9", "Referer": "https://vidbunker.in/", "Origin": "https://vidbunker.in",
    }
    found: dict = {"link": None, "name": None, "thumb": None}
    page_err = None
    try:
        async with session.get(watch_url, headers=headers, allow_redirects=True,
                               timeout=aiohttp.ClientTimeout(total=20, connect=10)) as r:
            if r.status >= 400:
                page_err = f"page HTTP {r.status}"
            else:
                found = _scan_page(await r.text(errors="ignore"), watch_url)
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        page_err = f"page fetch failed: {str(e) or e.__class__.__name__}"

    if found["link"]:
        return {"link": found["link"], "filename": found["name"], "size": 0, "thumbnail": found["thumb"], "duration": 0}
    vid = extract_id(watch_url)
    if vid:
        info = await _temp_info(vid, session, headers)
        if info:
            info["thumbnail"] = info.get("thumbnail") or found["thumb"]
            info["filename"] = info.get("filename") or found["name"]
            return info
    raise DiskwalaError(page_err or "no media url on page or in temp_info")


# ------------------------------------------------------------------ entry --
async def resolve(watch_url: str, session: aiohttp.ClientSession) -> dict:
    """-> {"link", "filename", "size", "thumbnail", "duration"}. Raises DiskwalaError if every tier fails."""
    if not DISKWALA_ENABLED:
        raise DiskwalaError("diskwala fallback disabled")
    errors = []

    async def run():
        if token_tier_available():
            try:
                return await _token_resolve(watch_url, session)
            except (DiskwalaError, aiohttp.ClientError, asyncio.TimeoutError) as e:
                errors.append(f"token-api: {str(e) or e.__class__.__name__}")
                log.info("diskwala token tier failed for %s: %s", watch_url, errors[-1])
        try:
            return await _html_resolve(watch_url, session)
        except (DiskwalaError, aiohttp.ClientError, asyncio.TimeoutError) as e:
            errors.append(f"html: {str(e) or e.__class__.__name__}")
            raise

    try:
        return await asyncio.wait_for(run(), timeout=DISKWALA_TIMEOUT)
    except asyncio.TimeoutError:
        raise DiskwalaError("; ".join(errors + ["timed out"]))
    except DiskwalaError:
        raise DiskwalaError("; ".join(errors))
