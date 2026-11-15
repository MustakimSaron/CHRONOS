"""
Real SPHEREx data access for CHRONOS-102 (no astropy needed).

How it works
------------
1. IRSA SIA2 discovers SPHEREx spectral images in DR1/QR3 wide and deep collections; TAP async is a fallback.
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
from urllib.parse import urljoin, urlparse, unquote

import requests
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np

IRSA = "https://irsa.ipac.caltech.edu/"
TAP = "https://irsa.ipac.caltech.edu/TAP/sync"
TAP_ASYNC = "https://irsa.ipac.caltech.edu/TAP/async"
SIA2 = "https://irsa.ipac.caltech.edu/SIA"
SIA2_COLLECTIONS = ("spherex_dr1", "spherex_qr3", "spherex_dr1_deep", "spherex_qr3_deep")
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

# HTTP functions are injected by app.py so SPHEREx uses the application's shared session.
_GET: Callable = None  # type: ignore
_POST: Callable = None  # type: ignore
_VERIFY = True
_USER_AGENT = "CHRONOS-102-V7.3/1.0"
_TRANSIENT_HTTP = {502, 503, 504}

# A second session bypasses HTTP(S)_PROXY / environment proxy settings. It is
# used only after the normal application transport fails for an IRSA request.
# This directly tests the exact failure mode seen on the hosted Vercel function.
_DIRECT_SESSION = requests.Session()
_DIRECT_SESSION.trust_env = False
_DIRECT_SESSION.mount("https://", requests.adapters.HTTPAdapter(pool_connections=8, pool_maxsize=24, max_retries=0))
_DIRECT_SESSION.mount("http://", requests.adapters.HTTPAdapter(pool_connections=4, pool_maxsize=12, max_retries=0))


def configure(getter: Callable, poster: Optional[Callable] = None,
              verify: bool = True, user_agent: str = "CHRONOS-102-V7.3/1.0"):
    global _GET, _POST, _VERIFY, _USER_AGENT
    _GET = getter
    _POST = poster
    _VERIFY = bool(verify)
    _USER_AGENT = user_agent or "CHRONOS-102-V7.3/1.0"


def _direct_request(method: str, url: str, params=None, data=None,
                    timeout: float = 40, headers_extra=None,
                    allow_redirects=True):
    headers = {"User-Agent": _USER_AGENT}
    if headers_extra:
        headers.update(headers_extra)
    return _DIRECT_SESSION.request(
        method=method,
        url=url,
        params=params,
        data=data,
        timeout=(min(8.0, float(timeout)), float(timeout)),
        headers=headers,
        verify=_VERIFY,
        allow_redirects=allow_redirects,
    )


def _spx_get(url, params=None, timeout=40, retries=1, headers_extra=None):
    """Try shared transport first, then direct egress if IRSA returns proxy/5xx errors."""
    if _GET is None:
        raise RuntimeError("SPHEREx HTTP getter is not configured")

    shared_problem = None
    try:
        response = _GET(url, params, timeout, retries=retries, headers_extra=headers_extra)
        if response.status_code not in _TRANSIENT_HTTP:
            response._chronos_transport = "shared-session"
            return response
        shared_problem = f"shared-session HTTP {response.status_code}"
    except (requests.ConnectionError, requests.Timeout) as exc:
        shared_problem = f"shared-session {type(exc).__name__}: {str(exc)[:140]}"

    # Do not let an environment proxy failure prevent a direct IRSA attempt.
    try:
        response = _direct_request("GET", url, params=params, timeout=timeout,
                                   headers_extra=headers_extra, allow_redirects=True)
        response._chronos_transport = "direct-no-env-proxy"
        if response.status_code not in _TRANSIENT_HTTP:
            return response
        direct_problem = f"direct-no-env-proxy HTTP {response.status_code}"
    except Exception as exc:  # noqa: BLE001
        direct_problem = f"direct-no-env-proxy {type(exc).__name__}: {str(exc)[:140]}"

    raise RuntimeError(f"IRSA GET failed via both transports ({shared_problem}; {direct_problem})")


def _spx_post(url, data=None, timeout=40, retries=0, headers_extra=None, **kwargs):
    """TAP POST with one direct-egress fallback; POST is not automatically replayed."""
    if _POST is None:
        raise RuntimeError("IRSA asynchronous TAP POST is not configured")

    shared_problem = None
    try:
        response = _POST(url, data=data, timeout=timeout, retries=retries,
                         headers_extra=headers_extra, **kwargs)
        if response.status_code not in _TRANSIENT_HTTP:
            response._chronos_transport = "shared-session"
            return response
        shared_problem = f"shared-session HTTP {response.status_code}"
    except (requests.ConnectionError, requests.Timeout) as exc:
        shared_problem = f"shared-session {type(exc).__name__}: {str(exc)[:140]}"

    try:
        response = _direct_request("POST", url, data=data, timeout=timeout,
                                   headers_extra=headers_extra,
                                   allow_redirects=kwargs.get("allow_redirects", True))
        response._chronos_transport = "direct-no-env-proxy"
        if response.status_code not in _TRANSIENT_HTTP:
            return response
        direct_problem = f"direct-no-env-proxy HTTP {response.status_code}"
    except Exception as exc:  # noqa: BLE001
        direct_problem = f"direct-no-env-proxy {type(exc).__name__}: {str(exc)[:140]}"

    raise RuntimeError(f"IRSA POST failed via both transports ({shared_problem}; {direct_problem})")


def _tap_async_csv(adql: str, timeout: float = 190.0) -> List[List[str]]:
    """Run an IRSA TAP query as a UWS async job and return CSV rows (without header)."""
    if _POST is None:
        raise RuntimeError("IRSA asynchronous TAP POST is not configured")

    response = _spx_post(
        TAP_ASYNC,
        data={"REQUEST": "doQuery", "LANG": "ADQL", "FORMAT": "csv", "QUERY": adql},
        timeout=30,
        retries=0,
        allow_redirects=False,
    )
    if response.status_code not in (200, 201, 302, 303):
        response.raise_for_status()
        raise RuntimeError(f"IRSA async TAP job submission returned HTTP {response.status_code}")

    location = response.headers.get("Location")
    if not location:
        raise RuntimeError("IRSA async TAP did not return a job Location header")
    job_url = urljoin(TAP_ASYNC.rstrip("/") + "/", location)
    if not job_url.startswith(TAP_ASYNC + "/"):
        raise RuntimeError("IRSA async TAP returned an unexpected job URL")

    # UWS jobs are created in PENDING state; explicitly start the job.
    run_response = _spx_post(job_url + "/phase", data={"PHASE": "RUN"}, timeout=20, retries=0, allow_redirects=False)
    if run_response.status_code not in (200, 201, 202, 303):
        run_response.raise_for_status()
        raise RuntimeError(f"IRSA async TAP could not start job (HTTP {run_response.status_code})")

    deadline = time.monotonic() + timeout
    phase = "PENDING"
    while time.monotonic() < deadline:
        phase_response = _spx_get(job_url + "/phase", timeout=15, retries=1)
        phase_response.raise_for_status()
        phase = phase_response.text.strip().upper()
        if phase == "COMPLETED":
            break
        if phase in ("ERROR", "ABORT", "ABORTED"):
            detail = ""
            try:
                err = _spx_get(job_url + "/error", timeout=10, retries=1)
                detail = re.sub(r"\s+", " ", err.text).strip()[:240]
            except Exception:
                pass
            raise RuntimeError(f"IRSA async TAP job {phase.lower()}: {detail or 'no archive error detail returned'}")
        if phase not in ("PENDING", "QUEUED", "EXECUTING", "UNKNOWN", "HELD", "SUSPENDED"):
            raise RuntimeError(f"IRSA async TAP returned unexpected job phase: {phase[:80]}")
        time.sleep(2.0)
    else:
        raise requests.Timeout(f"IRSA async TAP query exceeded {int(timeout)} seconds (last phase: {phase})")

    result = _spx_get(job_url + "/results/result", timeout=45, retries=2)
    result.raise_for_status()
    text = result.text.strip()
    if text.startswith("<"):
        msgs = [x.strip() for x in re.findall(r"<INFO[^>]*>([^<]*)</INFO>", text, re.S) if x.strip()]
        raise RuntimeError("IRSA TAP result error: " + (msgs[0] if msgs else re.sub(r"\s+", " ", text)[:240]))
    rows = list(csv.reader(io.StringIO(text)))
    return rows[1:] if rows else []


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
        r = _spx_get(self.url, None, 40, retries=2, headers_extra={"Range": f"bytes={offset}-{offset + length - 1}"})
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
        r = _spx_get(self.url, None, 40, retries=2, headers_extra={"Range": f"bytes=-{length}"})
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


def _csv_dict_rows(text: str) -> List[Dict[str, str]]:
    """Read SIA2 CSV while tolerating UTF-8 BOM and leading service comment lines."""
    lines = [line for line in (text or "").lstrip("\ufeff").splitlines() if line.strip() and not line.lstrip().startswith("#")]
    if not lines:
        return []
    reader = csv.DictReader(io.StringIO("\n".join(lines)))
    return [{str(k).strip().lower(): (v.strip() if isinstance(v, str) else v)
             for k, v in row.items() if k is not None} for row in reader]


def _row_value(row: Dict[str, str], *keys):
    for key in keys:
        value = row.get(key.lower())
        if value not in (None, "", "null", "NULL"):
            return value
    return None


def _release_from_row(row: Dict[str, str], uri: str) -> str:
    path = urlparse(uri).path if uri.startswith("http") else uri
    match = re.search(r"/(dr|qr)(\d+)/", path, re.I)
    if match:
        return (match.group(1) + match.group(2)).lower()
    collection = str(_row_value(row, "obs_collection") or "").lower()
    match = re.search(r"(dr|qr)(\d+)", collection)
    return (match.group(1) + match.group(2)).lower() if match else ""


def _normalise_sia2_rows(rows: List[Dict[str, str]]) -> List[Dict]:
    out, seen = [], {}
    for row in rows:
        uri = _row_value(row, "access_url")
        if not uri:
            continue
        uri = unquote(str(uri).strip())
        if not uri.startswith(("http://", "https://")):
            uri = urljoin(IRSA, uri)
        path = urlparse(uri).path
        if not path.lower().endswith(".fits"):
            continue
        t = _row_value(row, "t_min", "t_max")
        try:
            mjd = float(t)
        except (TypeError, ValueError):
            continue
        filename = path.rsplit("/", 1)[-1]
        # Example: level2_2026W21_2A_0001_2D1_spx_l2b-....fits
        detector_match = re.search(r"_(?:\d+)D([1-6])(?:_|\.)", filename, re.I)
        det = int(detector_match.group(1)) if detector_match else 0
        obs_match = re.search(r"(level2_[0-9A-Za-z]+_[0-9A-Za-z]+_\d+_\d+D[1-6])", filename, re.I)
        obs = obs_match.group(1) if obs_match else str(_row_value(row, "obs_id", "obs_publisher_did") or filename)
        release = _release_from_row(row, uri)
        item = {
            "uri": uri,
            "cloud_access": _row_value(row, "cloud_access"),
            "mjd": mjd,
            "det": det,
            "obs": obs,
            "release": release,
            "collection": _row_value(row, "obs_collection") or "",
        }
        if det not in range(1, 7):
            continue
        if obs in seen:
            if _rel_rank(release) > _rel_rank(out[seen[obs]]["release"]):
                out[seen[obs]] = item
            continue
        seen[obs] = len(out)
        out.append(item)
    out.sort(key=lambda item: item["mjd"])
    return out


def _sia2_query_url(ra: float, dec: float) -> str:
    """Build the same repeated-COLLECTION GET URL used by the successful Colab V7.3 test."""
    collections = "&".join(f"COLLECTION={collection}" for collection in SIA2_COLLECTIONS)
    return (f"{SIA2}?{collections}"
            f"&POS=circle+{ra:.6f}+{dec:.6f}+0.01"
            "&RESPONSEFORMAT=CSV")


def _sia2_fetch_rows(ra: float, dec: float, timeout: float = 55.0):
    """Fetch SIA2 CSV using the Colab-tested URL and validate the response."""
    url = _sia2_query_url(ra, dec)
    response = _spx_get(url, None, timeout=timeout, retries=2)
    response.raise_for_status()
    body = (response.text or "").lstrip("\ufeff").strip()
    if body.startswith("<"):
        messages = [x.strip() for x in re.findall(r"<INFO[^>]*>([^<]*)</INFO>", body, re.S) if x.strip()]
        message = messages[0] if messages else re.sub(r"\s+", " ", body)[:200]
        raise RuntimeError("IRSA SIA2 returned an XML error: " + message)
    rows = _csv_dict_rows(body)
    if rows and not any(_row_value(row, "access_url") for row in rows):
        raise RuntimeError("IRSA SIA2 returned records without an access_url column")
    return response, rows


def probe_sia2(ra: float, dec: float, timeout: float = 18.0) -> Dict:
    """Probe production SIA2, reporting transport, raw rows and usable FITS rows."""
    try:
        response, rows = _sia2_fetch_rows(ra, dec, timeout=timeout)
        images = _normalise_sia2_rows(rows)
        result = {
            "ok": bool(response.status_code == 200 and (not rows or images)),
            "http": response.status_code,
            "records": len(rows),
            "fits_records": len(images),
            "transport": getattr(response, "_chronos_transport", "shared-session"),
            "collections": list(SIA2_COLLECTIONS),
        }
        if rows and not images:
            result["error"] = "SIA2 returned rows, but none matched the SPHEREx Level-2 FITS metadata; columns=" + ",".join(rows[0].keys())[:180]
        return result
    except Exception as exc:  # noqa: BLE001
        detail = re.sub(r"\s+", " ", str(exc))[:260]
        return {"ok": False, "error": f"{type(exc).__name__}: {detail}"}


def _list_images_sia2(ra: float, dec: float) -> List[Dict]:
    response, rows = _sia2_fetch_rows(ra, dec, timeout=55.0)
    normalised = _normalise_sia2_rows(rows)
    if rows and not normalised:
        raise RuntimeError("IRSA SIA2 returned records, but none matched the expected SPHEREx Level-2 FITS filename/metadata format")
    return normalised


def _list_images_tap_fallback(ra: float, dec: float) -> List[Dict]:
    """Legacy TAP fallback for very recent ingests not yet indexed by SIA2."""
    q = ("SELECT a.uri, p.time_bounds_lower, p.energy_bandpassname FROM spherex.artifact a "
         "JOIN spherex.plane p ON a.planeid = p.planeid "
         f"WHERE 1 = CONTAINS(POINT('ICRS', {ra:.6f}, {dec:.6f}), p.poly) "
         "AND a.uri LIKE '%.fits'")
    rows = _tap_async_csv(q, timeout=190)
    seen, out = {}, []
    for row in rows:
        if len(row) < 3:
            continue
        uri, t, band = row[:3]
        if not uri or not str(uri).lower().endswith(".fits"):
            continue
        m = re.search(r"(level2_[0-9A-Za-z]+_[0-9A-Za-z]+_\d+_\d+D[1-6])", str(uri))
        obs = m.group(1) if m else str(uri)
        rel = (re.search(r"/((?:dr|qr)\d+)/", str(uri)) or [None, ""])[1]
        item = {"uri": str(uri), "cloud_access": None, "mjd": float(t),
                "det": int(re.sub(r"\D", "", str(band)) or 0), "obs": obs, "release": rel}
        if item["det"] not in range(1, 7):
            continue
        if obs in seen:
            if _rel_rank(rel) > _rel_rank(out[seen[obs]]["release"]):
                out[seen[obs]] = item
            continue
        seen[obs] = len(out)
        out.append(item)
    out.sort(key=lambda item: item["mjd"])
    return out


def list_images(ra: float, dec: float, cache_dir: str) -> List[Dict]:
    """SIA2-first discovery across DR1/QR3 wide and deep collections; TAP async is a fallback."""
    key = f"v4-sia2|{ra:.5f}|{dec:.5f}"
    hit = _cached("list", key, cache_dir, max_age=86400)
    if hit is not None:
        return hit
    sia_error = None
    try:
        out = _list_images_sia2(ra, dec)
        # A successful empty SIA2 result is a valid no-coverage answer. Do not turn it
        # into a long TAP wait; the SIA2 index can lag the newest weekly ingest by about a day.
        if out:
            _store("list", key, cache_dir, out)
        return out
    except Exception as exc:  # Use TAP async only if the SIA2 request itself failed.
        sia_error = exc
    try:
        out = _list_images_tap_fallback(ra, dec)
        if out:
            _store("list", key, cache_dir, out)
            return out
        if sia_error is None:
            _store("list", key, cache_dir, [])
            return []
    except Exception as tap_error:
        if sia_error is not None:
            raise RuntimeError(f"SIA2 failed ({type(sia_error).__name__}: {str(sia_error)[:120]}); TAP fallback failed ({type(tap_error).__name__}: {str(tap_error)[:120]})") from tap_error
        raise
    if sia_error is not None:
        raise RuntimeError(f"SIA2 query failed ({type(sia_error).__name__}: {str(sia_error)[:180]}) and TAP fallback returned no usable rows")
    _store("list", key, cache_dir, [])
    return []


def _cloud_url(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = str(value).strip()
    if text.startswith("https://") or text.startswith("http://"):
        return text
    if text.startswith("s3://"):
        parsed = urlparse(text)
        if parsed.netloc and parsed.path:
            return f"https://{parsed.netloc}.s3.amazonaws.com/{parsed.path.lstrip('/')}"
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            for key in ("url", "uri", "https", "access_url", "cloud_url"):
                candidate = _cloud_url(obj.get(key))
                if candidate:
                    return candidate
    except Exception:
        pass
    return None


def _urls(uri: str, cloud_access: Optional[str] = None) -> List[str]:
    """Return on-prem IRSA, cloud_access and public S3 URLs without mangling full URLs."""
    urls = []
    cloud = _cloud_url(cloud_access)
    if cloud:
        urls.append(cloud)
    if str(uri).startswith(("http://", "https://")):
        urls.append(str(uri))
        path = urlparse(str(uri)).path.lstrip("/")
    else:
        path = str(uri).lstrip("/")
        urls.append(urljoin(IRSA, path))
    s3key = re.sub(r"^ibe/data/spherex/", "", path)
    if s3key:
        urls.append(S3 + s3key)
    # Preserve order while removing duplicate candidates.
    return list(dict.fromkeys(urls))


def _irsa_product_url(uri: str) -> str:
    if str(uri).startswith(("http://", "https://")):
        parsed = urlparse(str(uri))
        return urljoin(IRSA, parsed.path.lstrip("/"))
    return urljoin(IRSA, str(uri).lstrip("/"))


def open_file(uri: str, cloud_access: Optional[str] = None) -> Tuple[SpxFile, str]:
    """Opens an exposure with Range requests (cloud access first when SIA2 provides it, then IRSA/S3)."""
    last = None
    for url in _urls(uri, cloud_access):
        try:
            return SpxFile(Source(url=url)), url
        except Exception as exc:  # noqa: BLE001
            last = exc
    raise last if last else IOError("no source")


def open_cutout(uri: str, ra: float, dec: float, size_px: int) -> Tuple[SpxFile, str]:
    """Fallback when Range requests are not possible: the IRSA cutout service (whole small MEF, gzip)."""
    url = _irsa_product_url(uri) + f"?center={ra:.6f},{dec:.6f}d&size={max(0.01, size_px * NATIVE_PX / 3600.0):.4f}"
    r = _spx_get(url, None, 90, retries=2)
    r.raise_for_status()
    blob = r.content
    if blob[:2] == b"\x1f\x8b":
        blob = gzip.decompress(blob)
    return SpxFile(Source(blob=blob)), url


# ------------------------------------------------------------------ products
def sample_point(item: Dict, ra: float, dec: float, half: int = 2) -> Optional[Dict]:
    """Flux at (ra, dec) from one exposure: mean of a (2*half+1)^2 box of IMAGE - ZODI, plus wavelength."""
    try:
        f, via = open_file(item["uri"], item.get("cloud_access"))
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


def spectrum(ra: float, dec: float, cache_dir: str, max_images: int = 110, workers: int = 12, query_ra: Optional[float] = None, query_dec: Optional[float] = None, pmra_mas_yr: float = 0.0, pmdec_mas_yr: float = 0.0, ref_epoch: float = 2000.0) -> Dict:
    """Real SPHEREx spectrum at a sky position from up to `max_images` exposures (all detectors, all epochs)."""
    query_ra = ra if query_ra is None else query_ra
    query_dec = dec if query_dec is None else query_dec
    key = f"m2|{ra:.5f}|{dec:.5f}|{query_ra:.5f}|{query_dec:.5f}|{pmra_mas_yr:.2f}|{pmdec_mas_yr:.2f}|{ref_epoch:.2f}|{max_images}"
    hit = _cached("spec", key, cache_dir)
    if hit is not None:
        hit["cached"] = True
        return hit
    imgs = list_images(query_ra, query_dec, cache_dir)
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
    def sample_at_epoch(it):
        obs_year = 2000.0 + (float(it["mjd"]) - 51544.5) / 365.25
        dt = obs_year - ref_epoch
        if not pmra_mas_yr and not pmdec_mas_yr:
            sample_ra, sample_dec = ra, dec
        else:
            cos_dec = max(1e-6, math.cos(math.radians(dec)))
            sample_ra = (ra + (pmra_mas_yr / 3600000.0) * dt / cos_dec) % 360.0
            sample_dec = max(-90.0, min(90.0, dec + (pmdec_mas_yr / 3600000.0) * dt))
        result = sample_point(it, sample_ra, sample_dec)
        if result:
            result["target_ra"] = sample_ra
            result["target_dec"] = sample_dec
        return result
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for it, res in zip(pick, ex.map(lambda it: _safe(sample_at_epoch, it), pick)):
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
        f, via = open_file(item["uri"], item.get("cloud_access"))
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


def field(ra: float, dec: float, fov_arcmin: float, cache_dir: str, n: int = 64, query_ra: Optional[float] = None, query_dec: Optional[float] = None) -> Dict:
    """
    Real two-epoch image set for the viewer: per detector D1..D6 one exposure from the first visit and the
    exposure from the latest visit (>= 60 days later) whose wavelength at the target is closest.
    """
    scale = fov_arcmin * 60.0 / n
    query_ra = ra if query_ra is None else query_ra
    query_dec = dec if query_dec is None else query_dec
    key = f"m2|{ra:.5f}|{dec:.5f}|{query_ra:.5f}|{query_dec:.5f}|{fov_arcmin:.2f}|{n}"
    hit = _cached("field", key, cache_dir)
    if hit is not None:
        hit["cached"] = True
        return hit
    imgs = list_images(query_ra, query_dec, cache_dir)
    if not imgs:
        return {"ra": ra, "dec": dec, "query_ra": query_ra, "query_dec": query_dec, "n_available": 0, "frames": [], "note": "SPHEREx has not observed this position in the public releases yet"}
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
    with ThreadPoolExecutor(max_workers=max(1, min(8, len(plan))), thread_name_prefix="spherex-field") as ex:  # bounded exposure downloads
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
    out = {"ra": ra, "dec": dec, "query_ra": query_ra, "query_dec": query_dec, "fov": fov_arcmin, "n": n, "scale": scale, "n_available": len(imgs), "frames": frames, "note": note,
           "n_tried": len(plan), "later_visit": bool(later),
           "errors": errors[:8], "n_errors": len(errors), "unit": "MJy/sr (IMAGE - ZODI)", "cached": False,
           "epochs_mjd": [imgs[0]["mjd"], later[-1]["mjd"] if later else None]}
    if frames:
        _store("field", key, cache_dir, out)
    return out
