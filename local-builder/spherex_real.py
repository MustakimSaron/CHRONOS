"""
Real SPHEREx data access for CHRONOS-102 (no astropy needed).

How it works
------------
1. IRSA SIA2 (with the TAP tables as a backup) lists every SPHEREx spectral image covering a sky position.
2. Instead of downloading whole 70 MB files (or 5 MB cutouts that always carry the full PSF cube), we read only
   the bytes we need with HTTP Range requests: the FITS headers, a small pixel box from IMAGE and ZODI,
   and the tiny WCS-WAVE table at the end of the file.
3. The celestial WCS (TAN + SIP) and the wavelength lookup table (WAVE-TAB, 7x7 control points, bilinear)
   are evaluated here in numpy.
If the server ignores Range requests, the IRSA cutout service is used instead (same parser, whole small file).

All results are cached on disk (cache/spherex/*.json) so a field opened once works instantly afterwards.
"""
from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import math
import os
import re
import time
import warnings
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np

IRSA = "https://irsa.ipac.caltech.edu/"
TAP = "https://irsa.ipac.caltech.edu/TAP/sync"
S3 = "https://nasa-irsa-spherex.s3.amazonaws.com/"
BLOCK = 2880
NATIVE_PX = 6.15  # arcsec per SPHEREx pixel
# FLAGS bits rejected (cosmic-ray TRANSIENT 0, OVERFLOW 1, SUR_ERROR 2, NONFUNC 6, DICHROIC 7, MISSING_DATA 9, HOT 10, COLD 11,
# PHANMISS 14, NONLINEAR 15, PERSIST 17, OUTLIER 19); the SOURCE bit (21) is kept - it marks real sources.
BAD_FLAGS = sum(1 << b for b in (0, 1, 2, 6, 7, 9, 10, 11, 14, 15, 17, 19))


def _mask_flags(f, img, x0, y0, w, h):
    """Sets flagged pixels to NaN (returns the image and the masked fraction; no FLAGS extension -> unchanged)."""
    try:
        fl = f.box("FLAGS", x0, y0, w, h)
    except Exception:  # noqa: BLE001
        return img, 0.0
    ok = np.isfinite(fl)
    bad = np.zeros(img.shape, bool)
    bad[ok] = (fl[ok].astype(np.int64) & BAD_FLAGS) != 0
    img = img.copy()
    img[bad] = np.nan
    return img, float(bad.mean())

# The HTTP getter is injected by app.py (it carries the proxy/VPN/retry settings).
_GET: Callable = None  # type: ignore


def configure(getter: Callable):
    global _GET
    _GET = getter


# ------------------------------------------------------------------ FITS header parsing
def parse_header(raw: bytes) -> Tuple[Dict[str, object], int]:
    """Parses one FITS header starting at raw[0]; returns (cards, header_length_in_bytes) or raises."""
    cards: Dict[str, object] = {}
    n = 0
    while True:
        if (n + 1) * 80 > len(raw):
            raise EOFError("header continues beyond the bytes read")
        card = raw[n * 80:(n + 1) * 80].decode("ascii", "replace")
        n += 1
        key = card[:8].strip()
        if key == "END":
            break
        if card[8:10] != "= " or not key:
            continue
        val = card[10:]
        if val.lstrip().startswith("'"):
            m = re.match(r"\s*'((?:[^']|'')*)'", val)
            cards[key] = m.group(1).replace("''", "'").rstrip() if m else ""
        else:
            v = val.split("/")[0].strip()
            if v in ("T", "F"):
                cards[key] = v == "T"
            else:
                try:
                    cards[key] = int(v)
                except ValueError:
                    try:
                        cards[key] = float(v.replace("D", "E"))
                    except ValueError:
                        cards[key] = v
    length = int(math.ceil(n * 80 / BLOCK) * BLOCK)
    return cards, length


def data_size(h: Dict[str, object]) -> int:
    naxis = int(h.get("NAXIS", 0))
    if naxis == 0:
        return 0
    n = abs(int(h.get("BITPIX", 8))) // 8
    for i in range(1, naxis + 1):
        n *= int(h.get(f"NAXIS{i}", 0))
    n = (n + int(h.get("PCOUNT", 0))) * int(h.get("GCOUNT", 1))
    return int(math.ceil(n / BLOCK) * BLOCK)


class Source:
    """Byte source: HTTP with Range requests, or an in-memory file (cutout / test)."""

    def __init__(self, url: Optional[str] = None, blob: Optional[bytes] = None):
        self.url, self.blob, self.requests, self.bytes = url, blob, 0, 0

    def read(self, offset: int, length: int) -> bytes:
        if self.blob is not None:
            return self.blob[offset:offset + length]
        r = _GET(self.url, None, 40, retries=1, headers_extra={"Range": f"bytes={offset}-{offset + length - 1}"})
        self.requests += 1
        if r.status_code == 200:
            raise RangeUnsupported(self.url)
        if r.status_code != 206:
            raise IOError(f"HTTP {r.status_code} for {self.url}")
        self.bytes += len(r.content)
        return r.content

    def read_tail(self, length: int) -> Tuple[bytes, int]:
        """Last `length` bytes and the file size."""
        if self.blob is not None:
            return self.blob[-length:], len(self.blob)
        r = _GET(self.url, None, 40, retries=1, headers_extra={"Range": f"bytes=-{length}"})
        self.requests += 1
        if r.status_code != 206:
            raise RangeUnsupported(self.url)
        m = re.search(r"/(\d+)$", r.headers.get("Content-Range", ""))
        size = int(m.group(1)) if m else -1
        self.bytes += len(r.content)
        return r.content, size


class RangeUnsupported(Exception):
    pass


class SpxFile:
    """One SPHEREx spectral image (MEF): IMAGE, FLAGS, VARIANCE, ZODI, PSF, WCS-WAVE."""

    def __init__(self, src: Source):
        self.src = src
        self.hdus: Dict[str, Tuple[Dict[str, object], int]] = {}  # name -> (header, data_offset)
        size = 10 * 23040  # primary + IMAGE headers in one read (230 KB); read more if they are longer
        while True:
            head = src.read(0, size)
            try:
                h0, l0 = parse_header(head)
                off = l0 + data_size(h0)
                h1, l1 = parse_header(head[off:])
                break
            except EOFError:
                if size >= 64 * 23040 or len(head) < size:
                    raise
                size *= 2
        if h1.get("ZIMAGE"):
            raise ValueError("tile-compressed file: use the cutout path")
        self.hdus["IMAGE"] = (h1, off + l1)
        self._next = off + l1 + data_size(h1)
        self.img = h1

    def _hdu(self, name: str):
        """Walks the extension chain until `name` (reads each header with one small request)."""
        while name not in self.hdus:
            size = 24 * BLOCK  # 69 KB: real extension headers (FLAGS, ZODI, WCS-WAVE...) fit in one read
            while True:
                raw = self.src.read(self._next, size)
                try:
                    h, ln = parse_header(raw)
                    break
                except EOFError:
                    if size >= 256 * BLOCK or len(raw) < size:
                        raise
                    size *= 4
            ext = str(h.get("EXTNAME", f"HDU{len(self.hdus) + 1}")).strip().upper()
            self.hdus[ext] = (h, self._next + ln)
            self._next = self._next + ln + data_size(h)
            if len(self.hdus) > 8:
                raise KeyError(name)
        return self.hdus[name]

    def box(self, name: str, x0: int, y0: int, w: int, h: int) -> np.ndarray:
        """Reads image rows y0..y0+h-1 (0-based), columns x0..x0+w-1 of a 2-D extension (one request)."""
        hdr, doff = self._hdu(name)
        nx, ny = int(hdr["NAXIS1"]), int(hdr["NAXIS2"])
        bp = int(hdr["BITPIX"])
        dt = {-32: ">f4", -64: ">f8", 32: ">i4", 16: ">i2", 8: "u1"}[bp]
        bpp = abs(bp) // 8
        x0c, y0c = max(0, x0), max(0, y0)
        x1c, y1c = min(nx, x0 + w), min(ny, y0 + h)
        out = np.full((h, w), np.nan, dtype=np.float64)
        if x1c <= x0c or y1c <= y0c:
            return out
        raw = self.src.read(doff + (y0c * nx) * bpp, (y1c - y0c) * nx * bpp)
        arr = np.frombuffer(raw, dtype=dt).reshape(y1c - y0c, nx)[:, x0c:x1c].astype(np.float64)
        if hdr.get("BSCALE") or hdr.get("BZERO"):
            arr = arr * float(hdr.get("BSCALE", 1.0)) + float(hdr.get("BZERO", 0.0))
        out[y0c - y0:y1c - y0, x0c - x0:x1c - x0] = arr
        return out

    def wave_table(self) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """WCS-WAVE: control points X(7), Y(7) and VALUES[iy, ix, 0=wavelength|1=bandwidth] in micron."""
        name = str(self.img.get("PS1_0W", "WCS-WAVE")).strip().upper()
        hdr, doff = self._hdu(name)
        nrow = int(hdr["NAXIS1"]) * int(hdr["NAXIS2"])
        raw = self.src.read(doff, nrow)
        cols, pos = {}, 0
        for i in range(1, int(hdr["TFIELDS"]) + 1):
            form = str(hdr[f"TFORM{i}"]).strip()
            m = re.match(r"(\d*)([A-Z])", form)
            rep, code = int(m.group(1) or 1), m.group(2)
            size = {"J": 4, "E": 4, "D": 8, "I": 2, "K": 8, "B": 1}[code]
            dt = {"J": ">i4", "E": ">f4", "D": ">f8", "I": ">i2", "K": ">i8", "B": "u1"}[code]
            a = np.frombuffer(raw[pos:pos + rep * size], dtype=dt).astype(np.float64)
            tdim = hdr.get(f"TDIM{i}")
            if tdim:
                dims = [int(d) for d in re.findall(r"\d+", str(tdim))]
                a = a.reshape(dims[::-1])  # FITS order (fastest first) -> numpy
            cols[str(hdr[f"TTYPE{i}"]).strip().upper()] = a
            pos += rep * size
        X, Y, V = cols[str(self.img.get("PS1_2W", "X")).upper()], cols[str(self.img.get("PS2_2W", "Y")).upper()], cols[str(self.img.get("PS1_1W", "VALUES")).upper()]
        if V.ndim == 1:  # no TDIM: assume (2, nx, ny) FITS order -> [iy, ix, m]
            V = V.reshape(len(Y), len(X), V.size // (len(X) * len(Y)))
        return X, Y, V


# ------------------------------------------------------------------ WCS (TAN + SIP) and WAVE-TAB
class TanSip:
    def __init__(self, h: Dict[str, object]):
        self.crpix = np.array([float(h["CRPIX1"]), float(h["CRPIX2"])])
        self.crval = np.radians([float(h["CRVAL1"]), float(h["CRVAL2"])])
        if "CD1_1" in h:
            self.cd = np.array([[float(h.get("CD1_1", 0)), float(h.get("CD1_2", 0))], [float(h.get("CD2_1", 0)), float(h.get("CD2_2", 0))]])
        else:
            pc = np.array([[float(h.get("PC1_1", 1)), float(h.get("PC1_2", 0))], [float(h.get("PC2_1", 0)), float(h.get("PC2_2", 1))]])
            self.cd = pc * np.array([[float(h.get("CDELT1", 1))], [float(h.get("CDELT2", 1))]])
        self.cdinv = np.linalg.inv(self.cd)
        self.A = self._poly(h, "A")
        self.B = self._poly(h, "B")
        self.AP = self._poly(h, "AP")
        self.BP = self._poly(h, "BP")

    @staticmethod
    def _poly(h, k):
        order = int(h.get(f"{k}_ORDER", 0))
        return [(i, j, float(h[f"{k}_{i}_{j}"])) for i in range(order + 1) for j in range(order + 1 - i) if f"{k}_{i}_{j}" in h]

    @staticmethod
    def _ev(terms, u, v):
        s = np.zeros_like(u, dtype=np.float64)
        for i, j, c in terms:
            s = s + c * u ** i * v ** j
        return s

    def world_to_pixel(self, ra, dec):
        """RA/Dec in degrees -> 0-based pixel (x, y)."""
        ra, dec = np.radians(np.asarray(ra, float)), np.radians(np.asarray(dec, float))
        a0, d0 = self.crval
        cosc = np.sin(d0) * np.sin(dec) + np.cos(d0) * np.cos(dec) * np.cos(ra - a0)
        xi = np.degrees(np.cos(dec) * np.sin(ra - a0) / cosc)
        eta = np.degrees((np.cos(d0) * np.sin(dec) - np.sin(d0) * np.cos(dec) * np.cos(ra - a0)) / cosc)
        U = self.cdinv[0, 0] * xi + self.cdinv[0, 1] * eta
        V = self.cdinv[1, 0] * xi + self.cdinv[1, 1] * eta
        if self.AP or self.BP:
            u, v = U + self._ev(self.AP, U, V), V + self._ev(self.BP, U, V)
        elif self.A or self.B:  # invert the forward SIP by fixed-point iteration
            u, v = U.copy(), V.copy()
            for _ in range(20):
                u, v = U - self._ev(self.A, u, v), V - self._ev(self.B, u, v)
        else:
            u, v = U, V
        return u + self.crpix[0] - 1, v + self.crpix[1] - 1

    def pixel_to_world(self, x, y):
        u, v = np.asarray(x, float) + 1 - self.crpix[0], np.asarray(y, float) + 1 - self.crpix[1]
        U, V = u + self._ev(self.A, u, v), v + self._ev(self.B, u, v)
        xi, eta = np.radians(self.cd[0, 0] * U + self.cd[0, 1] * V), np.radians(self.cd[1, 0] * U + self.cd[1, 1] * V)
        a0, d0 = self.crval
        den = np.cos(d0) - eta * np.sin(d0)
        ra = a0 + np.arctan2(xi, den)
        dec = np.arctan2(np.sin(d0) + eta * np.cos(d0), np.sqrt(xi ** 2 + den ** 2))
        return np.degrees(ra) % 360, np.degrees(dec)


def wave_at(f: SpxFile, x: float, y: float) -> Tuple[float, float]:
    """Wavelength and bandwidth (um) at 0-based pixel x, y via the WAVE-TAB bilinear lookup."""
    X, Y, V = f.wave_table()
    h = f.img
    px = float(h.get("CRVAL1W", 1)) + float(h.get("CDELT1W", 1)) * ((x + 1) - float(h.get("CRPIX1W", 1)))
    py = float(h.get("CRVAL2W", 1)) + float(h.get("CDELT2W", 1)) * ((y + 1) - float(h.get("CRPIX2W", 1)))
    ix = int(np.clip(np.searchsorted(X, px) - 1, 0, len(X) - 2))
    iy = int(np.clip(np.searchsorted(Y, py) - 1, 0, len(Y) - 2))
    tx = float(np.clip((px - X[ix]) / (X[ix + 1] - X[ix]), 0, 1))
    ty = float(np.clip((py - Y[iy]) / (Y[iy + 1] - Y[iy]), 0, 1))
    out = []
    for m in (int(h.get("PV1_3W", 1)) - 1, int(h.get("PV2_3W", 2)) - 1):
        q = V[..., m]
        out.append((1 - tx) * (1 - ty) * q[iy, ix] + tx * (1 - ty) * q[iy, ix + 1] + (1 - tx) * ty * q[iy + 1, ix] + tx * ty * q[iy + 1, ix + 1])
    return float(out[0]), float(out[1])


# ------------------------------------------------------------------ archive queries
def _cache_path(kind: str, key: str, cache_dir: str) -> str:
    d = os.path.join(cache_dir, "spherex")
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, kind + "_" + hashlib.md5(key.encode()).hexdigest()[:16] + ".json")


def _cached(kind, key, cache_dir, max_age=7 * 86400):
    p = _cache_path(kind, key, cache_dir)
    try:
        if time.time() - os.path.getmtime(p) < max_age:
            with open(p, encoding="utf-8") as fh:
                return json.load(fh)
    except OSError:
        pass
    return None


def _store(kind, key, cache_dir, obj):
    try:
        with open(_cache_path(kind, key, cache_dir), "w", encoding="utf-8") as fh:
            json.dump(obj, fh)
    except OSError:
        pass


def _rel_rank(rel: str) -> int:
    """Data releases (dr1...) supersede quick releases (qr1...); higher numbers are newer."""
    m = re.match(r"(dr|qr)(\d+)", rel or "")
    return (1000 if m and m.group(1) == "dr" else 0) + (int(m.group(2)) if m else -1)


SIA = "https://irsa.ipac.caltech.edu/SIA"
SIA_COLLECTIONS = ("spherex_dr1", "spherex_qr3", "spherex_dr1_deep", "spherex_qr3_deep")


def _to_mjd(v) -> Optional[float]:
    v = (v or "").strip()
    if not v:
        return None
    try:
        x = float(v)
        return x if 40000 < x < 90000 else None
    except ValueError:
        pass
    try:  # an ISO date-time
        import datetime as _dt
        d = _dt.datetime.fromisoformat(v.replace("Z", "+00:00"))
        return (d.replace(tzinfo=None) - _dt.datetime(1858, 11, 17)).total_seconds() / 86400.0
    except Exception:  # noqa: BLE001
        return None


def _mjd_from_name(uri: str) -> Optional[float]:
    """Middle of the observing week written in the file name (level2_2025W19_...)."""
    m = re.search(r"level2_(\d{4})W(\d{2})", uri)
    if not m:
        return None
    import datetime as _dt
    d = _dt.datetime(int(m.group(1)), 1, 1) + _dt.timedelta(days=(int(m.group(2)) - 1) * 7 + 3.5)
    return (d - _dt.datetime(1858, 11, 17)).total_seconds() / 86400.0


def _sia_items(ra: float, dec: float) -> Optional[List[Dict]]:
    """Spectral images covering a position from IRSA SIA2 (the quick method). None = the answer could not be read."""
    params = [("COLLECTION", c) for c in SIA_COLLECTIONS] + [("POS", f"circle {ra:.6f} {dec:.6f} 0.01"), ("RESPONSEFORMAT", "CSV")]
    r = _GET(SIA, params, 60, retries=2)
    r.raise_for_status()
    text = r.text.strip()
    if not text:
        return []
    if text.startswith("<"):
        raise RuntimeError("IRSA SIA error: " + re.sub(r"\s+", " ", text)[:200])
    rows = list(csv.DictReader(io.StringIO(text)))
    if not rows:
        return []
    out = []
    for row in rows:
        low = {str(k).strip().lower(): (v or "").strip() for k, v in row.items() if k}
        cands = [low.get("access_url", ""), low.get("obs_publisher_did", "")] + list(low.values())
        path = None
        for c in cands:
            m = re.search(r"((?:ibe/data/spherex/)(?:[^\s,\"'?]+?)\.fits)", c) or re.search(r"((?:dr|qr)\d+/level2/[^\s,\"'?]+?\.fits)", c)
            if m:
                path = m.group(1)
                break
        if not path:
            continue
        if not path.startswith("ibe/"):
            path = "ibe/data/spherex/" + path
        mjd = None
        for k in ("t_min", "t_mid", "time_bounds_lower", "t_start"):
            mjd = _to_mjd(low.get(k, ""))
            if mjd:
                break
        mjd = mjd or _mjd_from_name(path)
        if not mjd:
            continue
        dm = re.search(r"_\d+D(\d)", path)
        det = int(dm.group(1)) if dm else int(re.sub(r"\D", "", low.get("energy_bandpassname", "") or low.get("instrument_name", "")) or 0)
        m2 = re.search(r"(level2_[0-9A-Za-z]+_[0-9A-Za-z]+_\d+_\d+D\d)", path)
        rel = (re.search(r"/((?:dr|qr)\d+)/", path) or [None, ""])[1]
        out.append({"uri": path, "mjd": mjd, "det": det, "obs": m2.group(1) if m2 else path, "release": rel})
    return out if out else None


def _tap_items(ra: float, dec: float) -> List[Dict]:
    """Backup: the same list from the IRSA TAP tables (slower, sometimes unavailable)."""
    q = ("SELECT a.uri, p.time_bounds_lower, p.energy_bandpassname FROM spherex.artifact a "
         "JOIN spherex.plane p ON a.planeid = p.planeid "
         f"WHERE 1 = CONTAINS(POINT('ICRS', {ra:.6f}, {dec:.6f}), p.poly) ORDER BY p.time_bounds_lower")
    r = _GET(TAP, {"REQUEST": "doQuery", "LANG": "ADQL", "FORMAT": "csv", "QUERY": q}, 45, retries=1)
    r.raise_for_status()
    if r.text.lstrip().startswith("<"):
        raise RuntimeError("IRSA TAP error: " + re.sub(r"\s+", " ", r.text)[:200])
    out = []
    for uri, t, band in list(csv.reader(io.StringIO(r.text)))[1:]:
        if not uri.endswith(".fits"):
            continue
        m = re.search(r"(level2_[0-9A-Za-z]+_[0-9A-Za-z]+_\d+_\d+D\d)", uri)
        rel = (re.search(r"/((?:dr|qr)\d+)/", uri) or [None, ""])[1]
        out.append({"uri": uri, "mjd": float(t), "det": int(re.sub(r"\D", "", band) or 0), "obs": m.group(1) if m else uri, "release": rel})
    return out


def _dedupe(items: List[Dict]) -> List[Dict]:
    seen, out = {}, []
    for item in sorted(items, key=lambda i: i["mjd"]):
        obs, rel = item["obs"], item["release"]
        if obs in seen:  # keep the newest processing release of the same exposure
            if _rel_rank(rel) > _rel_rank(out[seen[obs]]["release"]):
                out[seen[obs]] = item
            continue
        seen[obs] = len(out)
        out.append(item)
    return out


def list_images(ra: float, dec: float, cache_dir: str) -> List[Dict]:
    """All SPHEREx spectral images whose footprint contains (ra, dec), oldest first, de-duplicated across releases.
    IRSA SIA2 answers in seconds; the TAP tables are only a backup."""
    key = f"v3|{ra:.4f}|{dec:.4f}"
    hit = _cached("list", key, cache_dir, max_age=86400)
    if hit is not None:
        return hit
    items, sia_err = None, ""
    try:
        items = _sia_items(ra, dec)
    except Exception as exc:  # noqa: BLE001
        sia_err = f"{type(exc).__name__}: {str(exc)[:110]}"
    if items is None:
        try:
            items = _tap_items(ra, dec)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"IRSA SIA2 ({sia_err or 'answer not readable'}) and TAP ({type(exc).__name__}: {str(exc)[:90]}) both failed")
    out = _dedupe(items)
    _store("list", key, cache_dir, out)
    return out


def _urls(uri: str) -> List[str]:
    u = uri.lstrip("/")
    s3key = re.sub(r"^ibe/data/spherex/", "", u)
    return [IRSA + u, S3 + s3key]


def open_file(uri: str) -> Tuple[SpxFile, str]:
    """Opens an exposure with Range requests (IRSA, then the AWS mirror)."""
    last = None
    for url in _urls(uri):
        try:
            return SpxFile(Source(url=url)), url
        except Exception as exc:  # noqa: BLE001
            last = exc
    raise last if last else IOError("no source")


def open_cutout(uri: str, ra: float, dec: float, size_px: int) -> Tuple[SpxFile, str]:
    """Fallback when Range requests are not possible: the IRSA cutout service (whole small MEF, gzip)."""
    url = IRSA + uri.lstrip("/") + f"?center={ra:.6f},{dec:.6f}d&size={max(0.01, size_px * NATIVE_PX / 3600.0):.4f}"
    r = _GET(url, None, 90, retries=1)
    r.raise_for_status()
    blob = r.content
    if blob[:2] == b"\x1f\x8b":
        blob = gzip.decompress(blob)
    return SpxFile(Source(blob=blob)), url


# ------------------------------------------------------------------ products
def sample_point(item: Dict, ra: float, dec: float, half: int = 2) -> Optional[Dict]:
    """Flux at (ra, dec) from one exposure: mean of a (2*half+1)^2 box of IMAGE - ZODI, plus wavelength."""
    try:
        f, via = open_file(item["uri"])
    except Exception:
        f, via = open_cutout(item["uri"], ra, dec, 2 * half + 9)
    w = TanSip(f.img)
    x, y = w.world_to_pixel(ra, dec)
    x, y = float(x), float(y)
    nx, ny = int(f.img["NAXIS1"]), int(f.img["NAXIS2"])
    if not (half <= x < nx - half and half <= y < ny - half):
        return None
    xi, yi = int(round(x)), int(round(y))
    img = f.box("IMAGE", xi - half, yi - half, 2 * half + 1, 2 * half + 1)
    try:
        zod = f.box("ZODI", xi - half, yi - half, 2 * half + 1, 2 * half + 1)
    except Exception:
        zod = np.zeros_like(img)
    sub = img - zod
    sub, _ = _mask_flags(f, sub, xi - half, yi - half, 2 * half + 1, 2 * half + 1)
    good = np.isfinite(sub)
    if good.sum() < 3:
        return None
    lam, bw = wave_at(f, x, y)
    centre = sub[half, half] if np.isfinite(sub[half, half]) else float(np.nanmedian(sub))
    return {"lam": round(lam, 5), "bw": round(bw, 5), "flux": float(centre), "box_mean": float(np.nanmean(sub)),
            "zodi": float(np.nanmean(zod)), "mjd": item["mjd"], "det": item["det"], "obs": item["obs"],
            "src": "s3" if "amazonaws" in via else ("cutout" if "?center" in via else "irsa")}


def spectrum(ra: float, dec: float, cache_dir: str, max_images: int = 110, workers: int = 12) -> Dict:
    """Real SPHEREx spectrum at a sky position from up to `max_images` exposures (all detectors, all epochs)."""
    key = f"m1|{ra:.5f}|{dec:.5f}|{max_images}"
    hit = _cached("spec", key, cache_dir)
    if hit is not None:
        hit["cached"] = True
        return hit
    imgs = list_images(ra, dec, cache_dir)
    pick = imgs
    if len(imgs) > max_images:  # spread the picks evenly over detectors and time
        by_det: Dict[int, List[Dict]] = {}
        for it in imgs:
            by_det.setdefault(it["det"], []).append(it)
        per = max(1, max_images // max(1, len(by_det)))
        pick = []
        for lst in by_det.values():
            idx = np.linspace(0, len(lst) - 1, min(per, len(lst))).round().astype(int)
            pick += [lst[i] for i in sorted(set(idx))]
    errors, pts = [], []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for it, res in zip(pick, ex.map(lambda it: _safe(sample_point, it, ra, dec), pick)):
            if isinstance(res, Exception):
                errors.append(f"{it['obs']}: {type(res).__name__}: {str(res)[:80]}")
            elif res:
                pts.append(res)
    pts.sort(key=lambda p: p["lam"])
    out = {"ra": ra, "dec": dec, "n_available": len(imgs), "n_used": len(pts), "points": pts,
           "errors": errors[:8], "n_errors": len(errors), "unit": "MJy/sr (IMAGE - ZODI)", "cached": False}
    if pts:
        _store("spec", key, cache_dir, out)
    return out


def _safe(fn, *a):
    try:
        return fn(*a)
    except Exception as exc:  # noqa: BLE001
        return exc


def _grid(ra: float, dec: float, n: int, scale_arcsec: float):
    """Output grid matching the viewer: north up, east left, centre (n/2, n/2), `scale` arcsec/px."""
    j, i = np.mgrid[0:n, 0:n]
    xi = -((i + 0.5) - n / 2) * scale_arcsec / 3600.0   # east to the left
    eta = -((j + 0.5) - n / 2) * scale_arcsec / 3600.0  # north up (row 0 = north)
    a0, d0 = math.radians(ra), math.radians(dec)
    xr, er = np.radians(xi), np.radians(eta)
    den = math.cos(d0) - er * math.sin(d0)
    gra = np.degrees(a0 + np.arctan2(xr, den)) % 360
    gdec = np.degrees(np.arctan2(math.sin(d0) + er * math.cos(d0), np.sqrt(xr ** 2 + den ** 2)))
    return gra, gdec


def _bilinear(arr, x, y):
    x0, y0 = np.floor(x).astype(int), np.floor(y).astype(int)
    tx, ty = x - x0, y - y0
    h, w = arr.shape

    def g(yy, xx):
        ok = (xx >= 0) & (xx < w) & (yy >= 0) & (yy < h)
        out = np.full(xx.shape, np.nan)
        out[ok] = arr[yy[ok], xx[ok]]
        return out
    return (g(y0, x0) * (1 - tx) * (1 - ty) + g(y0, x0 + 1) * tx * (1 - ty) + g(y0 + 1, x0) * (1 - tx) * ty + g(y0 + 1, x0 + 1) * tx * ty)


def reproject(item: Dict, ra: float, dec: float, n: int, scale: float) -> Optional[Dict]:
    """One exposure resampled onto the viewer grid (IMAGE - ZODI), with its wavelength at the centre."""
    gra, gdec = _grid(ra, dec, n, scale)
    try:
        f, via = open_file(item["uri"])
    except Exception:
        f, via = open_cutout(item["uri"], ra, dec, int(n * scale / NATIVE_PX * 1.6) + 8)
    w = TanSip(f.img)
    px, py = w.world_to_pixel(gra, gdec)
    x0, x1 = int(np.floor(np.nanmin(px))) - 1, int(np.ceil(np.nanmax(px))) + 2
    y0, y1 = int(np.floor(np.nanmin(py))) - 1, int(np.ceil(np.nanmax(py))) + 2
    if not (np.isfinite(px).all() and np.isfinite(py).all()):
        raise ValueError("WCS gave no pixel position for this field")
    nx, ny = int(f.img["NAXIS1"]), int(f.img["NAXIS2"])
    if x1 < 0 or y1 < 0 or x0 > nx or y0 > ny:
        raise ValueError(f"field falls outside this image (pixel {np.nanmean(px):.0f},{np.nanmean(py):.0f} of {nx}x{ny})")
    if x1 - x0 > 600 or y1 - y0 > 600:
        raise ValueError(f"field maps onto {x1 - x0}x{y1 - y0} px (WCS scale problem)")
    img = f.box("IMAGE", x0, y0, x1 - x0, y1 - y0)
    try:
        zod = f.box("ZODI", x0, y0, x1 - x0, y1 - y0)
        img = img - zod
    except Exception:
        pass
    img, masked = _mask_flags(f, img, x0, y0, x1 - x0, y1 - y0)
    if masked > 0:  # fill flagged pixels with the median of their valid neighbours so they do not punch holes in the resampled image
        fill = img.copy()
        for _ in range(2):
            nb = np.stack([np.roll(np.roll(fill, dy, 0), dx, 1) for dy in (-1, 0, 1) for dx in (-1, 0, 1) if dy or dx])
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", RuntimeWarning)  # all-NaN neighbourhoods
                med = np.nanmedian(nb, axis=0)
            fill = np.where(np.isfinite(fill), fill, med)
        img = fill
    out = _bilinear(img, px - x0, py - y0)
    fin = float(np.isfinite(out).mean())
    if fin < 0.5:
        raise ValueError(f"only {fin * 100:.0f}% of the field has valid pixels in this image (edge or masked)")
    cx, cy = w.world_to_pixel(ra, dec)
    lam, bw = wave_at(f, float(cx), float(cy))
    return {"data": np.where(np.isfinite(out), out, 0.0).astype(np.float32), "lam": lam, "bw": bw,
            "mjd": item["mjd"], "det": item["det"], "obs": item["obs"], "masked": round(masked, 4)}


def field(ra: float, dec: float, fov_arcmin: float, cache_dir: str, n: int = 64) -> Dict:
    """
    Real two-epoch image set for the viewer: per detector D1..D6 one exposure from the first visit and the
    exposure from the latest visit (>= 60 days later) whose wavelength at the target is closest.
    """
    scale = fov_arcmin * 60.0 / n
    key = f"m1|{ra:.5f}|{dec:.5f}|{fov_arcmin:.2f}|{n}"
    hit = _cached("field", key, cache_dir)
    if hit is not None:
        hit["cached"] = True
        return hit
    imgs = list_images(ra, dec, cache_dir)
    if not imgs:
        return {"ra": ra, "dec": dec, "n_available": 0, "frames": [], "note": "SPHEREx has not observed this position in the public releases yet"}
    t0 = imgs[0]["mjd"]
    first = [it for it in imgs if it["mjd"] - t0 < 30]
    later = [it for it in imgs if it["mjd"] - t0 >= 60]
    if later:
        t1 = later[-1]["mjd"]
        later = [it for it in later if t1 - it["mjd"] < 30]
    errors, frames = [], []

    def job(it):
        r = _safe(reproject, it, ra, dec, n, scale)
        return it, r

    plan = []
    for d in range(1, 7):
        e1 = [it for it in first if it["det"] == d][:1]
        e2 = [it for it in later if it["det"] == d][:2]  # two candidates per detector; keep the closest wavelength
        plan += [("E1", it) for it in e1] + [("E2", it) for it in e2]
    with ThreadPoolExecutor(max_workers=max(1, len(plan))) as ex:  # all exposures in one parallel round
        res = list(ex.map(lambda pe: (pe[0],) + job(pe[1]), plan))
    by = {}
    for ep, it, r in res:
        if isinstance(r, Exception):
            errors.append(f"{it['obs']}: {type(r).__name__}: {str(r)[:80]}")
            continue
        if r is None:
            continue
        by.setdefault((it["det"], ep), []).append(r)
    for d in range(1, 7):
        a = (by.get((d, "E1")) or [None])[0]
        cands = by.get((d, "E2")) or []
        b = min(cands, key=lambda c: abs(c["lam"] - a["lam"])) if (a and cands) else (cands[0] if cands else None)
        for ep, fr in (("E1", a), ("E2", b)):
            if fr:
                frames.append({"det": d, "epoch": ep, "lam": round(fr["lam"], 4), "bw": round(fr["bw"], 4), "mjd": fr["mjd"],
                               "obs": fr["obs"], "masked": fr.get("masked", 0), "data": [round(float(v), 4) for v in fr["data"].ravel()]})
    if not frames:
        why = errors[0] if errors else "no exposure from both visits"
        note = f"None of the {len(plan)} exposures tried (of {len(imgs)} listed) could be used. First reason: {why}"
    else:
        note = ""
    out = {"ra": ra, "dec": dec, "fov": fov_arcmin, "n": n, "scale": scale, "n_available": len(imgs), "frames": frames, "note": note,
           "n_tried": len(plan), "later_visit": bool(later),
           "errors": errors[:8], "n_errors": len(errors), "unit": "MJy/sr (IMAGE - ZODI)", "cached": False,
           "epochs_mjd": [imgs[0]["mjd"], later[-1]["mjd"] if later else None]}
    if frames:
        _store("field", key, cache_dir, out)
    return out
