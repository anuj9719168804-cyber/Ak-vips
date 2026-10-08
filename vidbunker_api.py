"""VidBunker resolver: link detection + API client (ported from the Vid-bunker-downloader repo, async/aiohttp).

Flow for a `https://vidbunker.in/watch/<id>` link:
  1. POST {"url": watch_url} to the worker API (retried on 429/5xx/network errors) -> {"link": ..., "filename": ...}
  2. If that fails, Diskwala (api2.diskwala.net token API, then page/__NEXT_DATA__ scrape) is tried (diskwala_api.py).
  3. If that fails too, the worker's plain GET endpoint (`API?url=<watch_url>`) is used as a streaming fallback.
  4. The true file size is probed (HEAD, then a 1-byte Range request) because the worker's GET is chunked and
     carries no Content-Length.
"""
import asyncio
import logging
import os
import re
import time
from dataclasses import dataclass, field
from typing import List, Optional, Tuple
from urllib.parse import quote, urlparse

import aiohttp

import diskwala_api

API_URL = os.getenv("VIDBUNKER_API", "https://vidbunker-backend.dailyweb577.workers.dev/api/download").strip()
API_RETRIES = max(1, int(os.getenv("API_RETRIES", "4")))
API_TIMEOUT = max(10, int(os.getenv("API_TIMEOUT", "60")))  # seconds per attempt (the worker can be slow to resolve)
# upper bound for one whole resolve (all retries + fallback + size probe); callers wrap fetch_vidbunker in wait_for(this)
RESOLVE_TIMEOUT = (API_RETRIES * (API_TIMEOUT + 10) + API_TIMEOUT + 30
                   + (diskwala_api.DISKWALA_TIMEOUT + 10 if diskwala_api.DISKWALA_ENABLED else 0))

# vid-player fallback (2nd tier, after the worker POST, before Diskwala). GET {VIDPLAYER_API}?url=<watch url>
VIDPLAYER_API = os.getenv("VIDPLAYER_API", "https://vid-player.dgxserver.online/api/download").strip()
VIDPLAYER_ENABLED = os.getenv("VIDPLAYER_FALLBACK", "1").strip().lower() not in ("0", "false", "no", "off")
VIDPLAYER_TIMEOUT = max(10, int(os.getenv("VIDPLAYER_TIMEOUT", "40")))

VIDBUNKER_HOSTS = ("vidbunker.in",)
VIDEO_EXTS = {"mp4", "mkv", "mov", "avi", "webm", "m4v", "ts", "flv", "3gp"}

# vidbunker.in links, with or without scheme / www.
_URL_RE = re.compile(r"(?:https?://)?(?:[a-z0-9-]+\.)*vidbunker\.in/[^\s<>\"']+", re.I)

_LINK_KEYS = ("link", "download_link", "download_url", "direct_link", "url")
_NAME_KEYS = ("filename", "file_name", "name", "title")
_THUMB_KEYS = ("thumbnail", "thumb", "poster", "image", "cover")


log = logging.getLogger("vidbunker_api")


class VidBunkerError(Exception):
    pass


@dataclass
class VBFile:
    name: str
    size: int
    download_url: Optional[str]
    stream_url: Optional[str] = None
    m3u8_url: Optional[str] = None
    thumb: Optional[str] = None
    is_dir: bool = False
    path: str = ""
    source: str = ""          # the vidbunker watch url (lets the bot ask the API for a fresh link)
    fallback: bool = False    # True when the link came from the worker's GET fallback endpoint
    duration: int = 0         # seconds, when the API provides it
    ctime: int = 0
    resolved: float = field(default_factory=time.time)  # when the link was resolved (links can expire)

    @property
    def is_video(self) -> bool:
        return self.name.lower().rsplit(".", 1)[-1] in VIDEO_EXTS


@dataclass
class VBResult:
    title: str = ""
    files: List[VBFile] = field(default_factory=list)


# ------------------------------------------------------------ link detection --
def is_vidbunker_host(host: str) -> bool:
    host = (host or "").lower()
    return any(host == h or host.endswith("." + h) for h in VIDBUNKER_HOSTS)


def extract_urls(text: Optional[str]) -> List[str]:
    """All de-duplicated vidbunker links in `text`, in order of appearance (scheme-less links get https://)."""
    seen, out = set(), []
    for m in _URL_RE.finditer(text or ""):
        url = m.group(0).rstrip(").,;:!]>'\"")
        if not url.lower().startswith("http"):
            url = "https://" + url
        host = urlparse(url).hostname or ""
        if is_vidbunker_host(host) and url not in seen:
            seen.add(url)
            out.append(url)
    return out


def extract_vidbunker_url(text: Optional[str]) -> Optional[str]:
    urls = extract_urls(text)
    return urls[0] if urls else None


# ------------------------------------------------------------------ helpers --
def _clean_name(name: Optional[str], fallback: str) -> str:
    name = (name or "").strip().replace("/", "_").replace("\\", "_")
    name = re.sub(r'[<>:"|?*\x00-\x1f]', "", name).strip(". ")
    return name or fallback


def _guess_name(watch_url: str) -> str:
    slug = urlparse(watch_url).path.rstrip("/").split("/")[-1] or "video"
    return f"{slug}.mp4"


def _with_ext(name: str) -> str:
    """Everything VidBunker serves is a video: make sure the name has an extension so Telegram treats it as one."""
    return name if "." in name.rsplit("/", 1)[-1] else name + ".mp4"


def _to_int(v) -> int:
    try:
        return int(float(str(v).strip()))
    except Exception:
        return 0


def _first_str(d: dict, keys, http_only: bool = False) -> Optional[str]:
    for k in keys:
        v = d.get(k)
        if isinstance(v, str) and v.strip() and (not http_only or v.startswith("http")):
            return v.strip()
    return None


def _pick_link(d: dict) -> Optional[str]:
    """First usable download link in an API reply. A value pointing back at a vidbunker.in page (e.g. an error reply
    that just echoes the watch url under "url") is NOT a download link and is skipped."""
    for k in _LINK_KEYS:
        v = d.get(k)
        if not (isinstance(v, str) and v.strip().startswith("http")):
            continue
        v = v.strip()
        if is_vidbunker_host(urlparse(v).hostname or "") and "/watch/" in v.lower():
            continue
        return v
    return None


def fallback_link(watch_url: str) -> str:
    return f"{API_URL}?url={quote(watch_url, safe='')}"


# ----------------------------------------------------------- size probing --
async def probe_size(session: aiohttp.ClientSession, link: str, headers: Optional[dict] = None) -> Tuple[int, bool]:
    """-> (total_size, accepts_ranges). total_size is 0 when unknown."""
    headers = headers or {}
    total, accepts = 0, False
    try:
        async with session.head(link, headers=headers, allow_redirects=True,
                                timeout=aiohttp.ClientTimeout(total=30)) as r:
            ctype = (r.headers.get("Content-Type") or "").lower()
            cl = r.headers.get("Content-Length")
            if r.status < 400 and cl and cl.isdigit() and "text/html" not in ctype and "json" not in ctype:
                total = int(cl)
            accepts = (r.headers.get("Accept-Ranges") or "").lower() == "bytes"
    except Exception:
        pass
    if not total or not accepts:
        try:
            async with session.get(link, headers={**headers, "Range": "bytes=0-0"}, allow_redirects=True,
                                   timeout=aiohttp.ClientTimeout(total=30)) as r:
                ctype = (r.headers.get("Content-Type") or "").lower()
                if r.status == 206 and "text/html" not in ctype and "json" not in ctype:
                    accepts = True
                    tail = (r.headers.get("Content-Range") or "").rsplit("/", 1)[-1]
                    if tail.isdigit():
                        total = int(tail)
        except Exception:
            pass
    return total, accepts


# --------------------------------------------------------------- resolving --
async def _post_api(watch_url: str, session: aiohttp.ClientSession) -> dict:
    """POST to the worker API with retries. Returns the parsed JSON (must contain a link)."""
    last: Exception = VidBunkerError("unknown error")
    for attempt in range(API_RETRIES):
        try:
            async with session.post(API_URL, json={"url": watch_url},
                                    timeout=aiohttp.ClientTimeout(total=API_TIMEOUT, connect=15)) as r:
                if r.status == 200:
                    data = await r.json(content_type=None)
                    if isinstance(data, dict) and _pick_link(data):
                        return data
                    last = VidBunkerError(f"API 200 but no link: {str(data)[:200]}")
                elif r.status in (429, 500, 502, 503, 504):
                    last = VidBunkerError(f"transient status {r.status}")
                else:
                    last = VidBunkerError(f"API status {r.status}: {(await r.text())[:200]}")
                    break  # not worth retrying (bad link, blocked, ...)
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:  # ValueError = non-JSON body
            last = e if str(e) else VidBunkerError(e.__class__.__name__)
        if attempt < API_RETRIES - 1:
            await asyncio.sleep(min(2 ** attempt, 10))
    raise VidBunkerError(str(last) or last.__class__.__name__)


async def _get_fallback(watch_url: str, session: aiohttp.ClientSession) -> str:
    """The worker's GET endpoint streams the video directly. Returns the link if it really serves a file."""
    link = fallback_link(watch_url)
    async with session.get(link, allow_redirects=True,
                           timeout=aiohttp.ClientTimeout(total=API_TIMEOUT, connect=15)) as r:
        ctype = (r.headers.get("Content-Type") or "").lower()
        if r.status == 200 and ("video" in ctype or "octet-stream" in ctype):
            return link
        raise VidBunkerError(f"fallback status {r.status}, content-type {ctype!r}")


def _deep_pick(obj, depth: int = 0) -> Optional[str]:
    """Find a download link anywhere in a (possibly nested) JSON reply: top level, data/result/files[0], ..."""
    if depth > 4:
        return None
    if isinstance(obj, dict):
        link = _pick_link(obj)
        if link:
            return link
        for k in ("data", "result", "results", "video", "file", "files", "list"):
            if k in obj:
                got = _deep_pick(obj[k], depth + 1)
                if got:
                    return got
    elif isinstance(obj, list):
        for it in obj[:5]:
            got = _deep_pick(it, depth + 1)
            if got:
                return got
    return None


def _deep_first(obj, keys, depth: int = 0):
    """First non-empty value for any of `keys` in a nested JSON reply (for name / size / thumb)."""
    if depth > 4:
        return None
    if isinstance(obj, dict):
        for k in keys:
            v = obj.get(k)
            if isinstance(v, (str, int, float)) and str(v).strip():
                return v
        for v in obj.values():
            if isinstance(v, (dict, list)):
                got = _deep_first(v, keys, depth + 1)
                if got is not None:
                    return got
    elif isinstance(obj, list):
        for it in obj[:5]:
            got = _deep_first(it, keys, depth + 1)
            if got is not None:
                return got
    return None


async def _get_vidplayer(watch_url: str, session: aiohttp.ClientSession) -> dict:
    """vid-player.dgxserver.online/api/download?url=<watch> -> the reply dict with a usable link. Raises VidBunkerError."""
    api = f"{VIDPLAYER_API}?url={quote(watch_url, safe='')}"
    try:
        async with session.get(api, headers={"Accept": "application/json"}, allow_redirects=True,
                               timeout=aiohttp.ClientTimeout(total=VIDPLAYER_TIMEOUT, connect=10)) as r:
            if r.status != 200:
                raise VidBunkerError(f"vid-player status {r.status}")
            try:
                data = await r.json(content_type=None)
            except ValueError:
                raw = await r.text()  # plain-text reply that is just the link
                data = {"link": raw.strip()} if raw.strip().startswith("http") else None
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        raise VidBunkerError(f"vid-player: {str(e) or e.__class__.__name__}")
    if not data:
        raise VidBunkerError("vid-player: empty reply")
    link = _deep_pick(data)
    if not link:
        raise VidBunkerError(f"vid-player: no link in reply: {str(data)[:200]}")
    if isinstance(data, dict):
        data.setdefault("link", link)
    return data if isinstance(data, dict) else {"link": link}


def _build_file(url: str, data: dict, link: str, is_fallback: bool) -> VBFile:
    data = data or {}
    name_raw = _first_str(data, _NAME_KEYS) or _deep_first(data, _NAME_KEYS)
    name = _with_ext(_clean_name(name_raw if isinstance(name_raw, str) else None, _guess_name(url)))
    return VBFile(
        name=name,
        size=_to_int(data.get("size") or data.get("file_size") or _deep_first(data, ("size", "file_size")) or 0),
        download_url=link,
        stream_url=link,
        thumb=_first_str(data, _THUMB_KEYS, http_only=True),
        source=url,
        fallback=is_fallback,
        duration=_to_int(data.get("duration")),
    )


async def _tier_worker(url, session):
    data = await _post_api(url, session)
    return _pick_link(data), data


async def _tier_vidplayer(url, session):
    vp = await _get_vidplayer(url, session)
    return _deep_pick(vp), vp


async def _tier_diskwala(url, session):
    dw = await diskwala_api.resolve(url, session)
    return dw["link"], dw


async def _tier_get(url, session):
    return await _get_fallback(url, session), {}


async def resolve_candidates(url: str, session: aiohttp.ClientSession, timeout: float = 15.0) -> List[VBFile]:
    """Ask EVERY resolver tier at the same time and return each working link as a VBFile (order: worker, vid-player,
    diskwala, worker GET). Used by the stream race and by RANK_FIRST. Never raises; slow tiers are simply dropped."""
    tiers = [("worker", _tier_worker)]
    if VIDPLAYER_ENABLED:
        tiers.append(("vid-player", _tier_vidplayer))
    if diskwala_api.DISKWALA_ENABLED:
        tiers.append(("diskwala", _tier_diskwala))
    tiers.append(("worker-get", _tier_get))
    tasks = [(name, asyncio.create_task(fn(url, session))) for name, fn in tiers]
    done, pending = await asyncio.wait([t for _, t in tasks], timeout=timeout)
    for t in pending:
        t.cancel()
    out, seen = [], set()
    for name, t in tasks:
        if t not in done or t.cancelled() or t.exception():
            continue
        try:
            link, data = t.result()
        except Exception:
            continue
        if link and link not in seen and link.startswith("http"):
            seen.add(link)
            out.append(_build_file(url, data, link, is_fallback=(name == "worker-get")))
    return out


async def fetch_vidbunker(url: str, session: aiohttp.ClientSession, probe: bool = True) -> VBResult:
    """Resolve a vidbunker watch link into a one-file VBResult. Raises VidBunkerError with the real reason."""
    if not is_vidbunker_host(urlparse(url).hostname or ""):
        raise VidBunkerError(f"not a VidBunker link: {url}")

    data, is_fallback, err = None, False, None
    try:
        data = await _post_api(url, session)
        link = _pick_link(data)
    except VidBunkerError as e:
        err, data = e, None
        dw_err = vp_err = None
        if VIDPLAYER_ENABLED:  # 2nd tier: vid-player API (real direct link)
            try:
                vp = await _get_vidplayer(url, session)
                link, data = _deep_pick(vp), vp
                log.info("resolved %s via vid-player fallback (worker API said: %s)", url, err)
            except Exception as e_vp:
                vp_err = str(e_vp) or e_vp.__class__.__name__
        if data is None:
            try:  # 3rd tier: Diskwala (real direct link, so is_fallback stays False)
                dw = await diskwala_api.resolve(url, session)
                link, data = dw["link"], dw
            except Exception as e_dw:  # DiskwalaError / aiohttp / anything: never let it hide the worker fallback
                dw_err = str(e_dw) or e_dw.__class__.__name__
        if data is None:
            try:  # 3rd tier: the worker's streaming GET endpoint
                link, is_fallback = await _get_fallback(url, session), True
            except (aiohttp.ClientError, asyncio.TimeoutError, VidBunkerError) as e2:
                raise VidBunkerError(f"could not resolve {url}: {err}; vid-player: {vp_err}; diskwala: {dw_err}; "
                                     f"fallback: {str(e2) or e2.__class__.__name__}")
        else:
            log.info("resolved %s via diskwala fallback (worker API said: %s)", url, err)

    data = data or {}
    name_raw = _first_str(data, _NAME_KEYS) or _deep_first(data, _NAME_KEYS)
    name = _with_ext(_clean_name(name_raw if isinstance(name_raw, str) else None, _guess_name(url)))
    thumb = _first_str(data, _THUMB_KEYS, http_only=True)
    f = VBFile(
        name=name,
        size=_to_int(data.get("size") or data.get("file_size") or _deep_first(data, ("size", "file_size")) or 0),
        download_url=link,
        stream_url=link,
        thumb=thumb,
        source=url,
        fallback=is_fallback,
        duration=_to_int(data.get("duration")),
    )
    if probe and not f.size:  # best effort - the download still works without a known size
        try:
            f.size, _ = await asyncio.wait_for(probe_size(session, link), timeout=25)
        except Exception:
            pass
    return VBResult(title=name, files=[f])
