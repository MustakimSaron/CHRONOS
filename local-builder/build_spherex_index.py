#!/usr/bin/env python3
"""Build the CHRONOS-102 SPHEREx DR1/QR3 spatial index from the audit CSV.

Run in Google Colab (or another Python environment) with:
    pip install boto3 astropy numpy pandas tqdm
    python build_spherex_index.py --input chronos102_retry_audit.csv \
        --db chronos102_spherex_build.sqlite --out spherex_index_tiles --workers 12

The audit CSV is the canonical input: it contains BOTH l2b and l2b_retry rows.
Do not combine it with chronos102_science_manifest.csv (that would duplicate the
standard l2b records).

The builder reads only FITS headers through S3 byte-range requests. It does not
download the 70 MB science images. It is resumable: completed files are retained
in the SQLite staging DB. After a complete scan, it exports 5-degree gzip JSONL
shards suitable for static hosting (S3/R2/another public object store).
"""
from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
import os
import shutil
import sqlite3
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import boto3
import numpy as np
import warnings
from astropy.io import fits
from astropy.wcs import WCS
from astropy.coordinates import SkyCoord
import astropy.units as u
from botocore import UNSIGNED
from botocore.config import Config
from tqdm.auto import tqdm

BUCKET = "nasa-irsa-spherex"
S3_BASE = f"https://{BUCKET}.s3.us-east-1.amazonaws.com/"
TILE_DEG = 5.0
N_RA = int(360 / TILE_DEG)
N_DEC = int(180 / TILE_DEG)
RANGE_SIZES = (46080, 92160, 184320, 262144, 524288)

S3 = boto3.client(
    "s3",
    region_name="us-east-1",
    config=Config(
        signature_version=UNSIGNED,
        max_pool_connections=64,
        retries={"max_attempts": 4, "mode": "standard"},
        connect_timeout=12,
        read_timeout=40,
    ),
)


def _header_at(data: bytes, offset: int) -> Tuple[fits.Header, int]:
    """Parse one FITS header at a 2880-byte aligned offset."""
    for block_start in range(offset, len(data), 2880):
        block = data[block_start:block_start + 2880]
        if len(block) < 2880:
            break
        for card_start in range(0, 2880, 80):
            card = block[card_start:card_start + 80]
            if card[:8].strip() == b"END":
                end = block_start + card_start + 80
                raw = data[offset:end]
                header = fits.Header.fromstring(raw.decode("ascii"), sep="")
                header_len = ((len(raw) + 2879) // 2880) * 2880
                return header, header_len
    raise EOFError("FITS header extends beyond current byte range")


def _padded_data_size(h: fits.Header) -> int:
    naxis = int(h.get("NAXIS", 0))
    bitpix = abs(int(h.get("BITPIX", 8)))
    elements = int(np.prod([int(h[f"NAXIS{i}"]) for i in range(1, naxis + 1)])) if naxis else 0
    size = (elements * bitpix // 8 + int(h.get("PCOUNT", 0))) * int(h.get("GCOUNT", 1))
    return ((size + 2879) // 2880) * 2880


def _image_header_from_range(data: bytes) -> fits.Header:
    offset = 0
    for _ in range(24):
        h, header_len = _header_at(data, offset)
        if str(h.get("EXTNAME", "")).strip().upper() == "IMAGE":
            return h
        offset += header_len + _padded_data_size(h)
        if offset >= len(data):
            break
    raise EOFError("IMAGE extension header not present in fetched byte range")


def _tile_ids(corners: List[List[float]], center_ra: float, center_dec: float) -> List[Tuple[int, int]]:
    """Conservatively assign a footprint to every 5-degree RA/Dec tile it touches."""
    decs = [p[1] for p in corners]
    dec_pad = 0.025
    dec_lo = max(-90.0, min(decs) - dec_pad)
    dec_hi = min(90.0, max(decs) + dec_pad)
    iy0 = max(0, min(N_DEC - 1, int(math.floor((dec_lo + 90.0) / TILE_DEG))))
    iy1 = max(0, min(N_DEC - 1, int(math.floor((dec_hi + 90.0) / TILE_DEG))))

    # Unwrap RA around the image center before finding the range.
    ras = [((float(p[0]) - center_ra + 180.0) % 360.0) - 180.0 + center_ra for p in corners]
    cos_dec = max(0.05, abs(math.cos(math.radians(center_dec))))
    ra_pad = 0.025 / cos_dec
    ra_lo = min(ras) - ra_pad
    ra_hi = max(ras) + ra_pad

    if ra_hi - ra_lo >= 360.0 or abs(center_dec) > 88.0:
        ix_values = range(N_RA)
    else:
        ix_values = sorted({ix % N_RA for ix in range(
            int(math.floor(ra_lo / TILE_DEG)),
            int(math.floor(ra_hi / TILE_DEG)) + 1
        )})

    return [(ix, iy) for iy in range(iy0, iy1 + 1) for ix in ix_values]


def inspect_one(row: Dict[str, str]) -> Tuple[str, Optional[Dict], Optional[str]]:
    key = (row.get("s3_key") or "").strip()
    if not key:
        return "error", None, "missing s3_key"

    last_error = None
    header = None
    for size in RANGE_SIZES:
        try:
            obj = S3.get_object(Bucket=BUCKET, Key=key, Range=f"bytes=0-{size - 1}")
            try:
                data = obj["Body"].read()
            finally:
                obj["Body"].close()
            header = _image_header_from_range(data)
            break
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {str(exc)[:180]}"

    if header is None:
        return "error", None, last_error or "could not read IMAGE header"

    try:
        nx, ny = int(header["NAXIS1"]), int(header["NAXIS2"])
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            wcs = WCS(header, relax=True).celestial
            px = np.array([-0.5, nx - 0.5, nx - 0.5, -0.5], dtype=float)
            py = np.array([-0.5, -0.5, ny - 0.5, ny - 0.5], dtype=float)
            ra_c, dec_c = wcs.pixel_to_world_values(px, py)
            center_ra, center_dec = wcs.pixel_to_world_values((nx - 1) / 2, (ny - 1) / 2)

        if not np.all(np.isfinite(ra_c)) or not np.all(np.isfinite(dec_c)):
            raise ValueError("non-finite WCS footprint corners")
        if not (math.isfinite(float(center_ra)) and math.isfinite(float(center_dec))):
            raise ValueError("non-finite WCS image center")

        center = SkyCoord(float(center_ra) * u.deg, float(center_dec) * u.deg)
        corners_coord = SkyCoord(np.asarray(ra_c) * u.deg, np.asarray(dec_c) * u.deg)
        radius_deg = float(center.separation(corners_coord).max().deg) + 0.025
        corners = [[float(r) % 360.0, float(d)] for r, d in zip(ra_c, dec_c)]

        mjd_value = header.get("MJD-AVG", header.get("MJD-OBS"))
        if mjd_value is None:
            mjd_value = 0.0
        mjd = float(mjd_value)
        date_obs = str(header.get("DATE-OBS", ""))
        release = (row.get("release") or key.split("/", 1)[0]).strip().lower()
        product = (row.get("kind") or "l2b").strip().lower()
        detector = int(row.get("detector") or header.get("DETECTOR") or 0)
        obsid = (row.get("obsid") or header.get("OBSID") or key.rsplit("/", 1)[-1]).strip()
        obs = f"{obsid}D{detector}"

        record = {
            "uri": S3_BASE + key,
            "mjd": mjd,
            "date_obs": date_obs,
            "det": detector,
            "obs": obs,
            "release": release,
            "collection": f"spherex_{release}",
            "product": product,
            "center_ra": float(center_ra) % 360.0,
            "center_dec": float(center_dec),
            "radius_deg": radius_deg,
            "corners": corners,
            "image_width": nx,
            "image_height": ny,
            "s3_key": key,
        }
        return "ok", record, None
    except Exception as exc:
        return "error", None, f"{type(exc).__name__}: {str(exc)[:180]}"


def open_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA cache_size=-100000")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS source_files (
            s3_key TEXT PRIMARY KEY,
            release TEXT NOT NULL,
            period TEXT,
            obsid TEXT,
            detector INTEGER,
            product TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            error TEXT
        );
        CREATE TABLE IF NOT EXISTS observations (
            s3_key TEXT PRIMARY KEY,
            release TEXT NOT NULL,
            period TEXT,
            obsid TEXT,
            detector INTEGER NOT NULL,
            product TEXT NOT NULL,
            mjd REAL NOT NULL,
            date_obs TEXT,
            center_ra REAL NOT NULL,
            center_dec REAL NOT NULL,
            radius_deg REAL NOT NULL,
            corners_json TEXT NOT NULL,
            image_width INTEGER,
            image_height INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_obs_mjd ON observations(mjd);
        CREATE INDEX IF NOT EXISTS idx_obs_release ON observations(release);
    """)
    return conn


def load_source_csv(conn: sqlite3.Connection, csv_path: str) -> int:
    inserted = 0
    batch = []
    with open(csv_path, "r", newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        required = {"s3_key", "release", "period", "obsid", "detector", "kind"}
        if not required.issubset(set(reader.fieldnames or [])):
            raise ValueError(f"Audit CSV is missing required columns: {sorted(required - set(reader.fieldnames or []))}")
        for row in reader:
            batch.append((row["s3_key"], row["release"], row["period"], row["obsid"],
                          int(row["detector"]), row["kind"], "pending", None))
            if len(batch) >= 10000:
                before = conn.total_changes
                conn.executemany("INSERT OR IGNORE INTO source_files VALUES (?,?,?,?,?,?,?,?)", batch)
                inserted += conn.total_changes - before
                conn.commit()
                batch.clear()
    if batch:
        before = conn.total_changes
        conn.executemany("INSERT OR IGNORE INTO source_files VALUES (?,?,?,?,?,?,?,?)", batch)
        inserted += conn.total_changes - before
        conn.commit()
    return inserted


def scan_pending(conn: sqlite3.Connection, workers: int, retry_errors: bool) -> None:
    if retry_errors:
        conn.execute("UPDATE source_files SET status='pending', error=NULL WHERE status='error'")
        conn.commit()

    total_pending = conn.execute("SELECT COUNT(*) FROM source_files WHERE status='pending'").fetchone()[0]
    if not total_pending:
        print("No pending files. Existing completed scan will be reused.")
        return

    batch_size = max(24, workers * 4)
    processed = 0
    started = time.time()
    last_rowid = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        with tqdm(total=total_pending, desc="Indexing FITS headers", unit="file") as bar:
            while True:
                rows = conn.execute(
                    "SELECT rowid,s3_key,release,period,obsid,detector,product "
                    "FROM source_files WHERE status='pending' AND rowid>? ORDER BY rowid LIMIT ?",
                    (last_rowid, batch_size),
                ).fetchall()
                if not rows:
                    break
                last_rowid = rows[-1][0]
                row_dicts = [dict(zip(("rowid","s3_key","release","period","obsid","detector","kind"), r)) for r in rows]
                futures = {pool.submit(inspect_one, row): row for row in row_dicts}
                for future in as_completed(futures):
                    row = futures[future]
                    try:
                        status, rec, error = future.result()
                    except Exception as exc:
                        status, rec, error = "error", None, f"{type(exc).__name__}: {str(exc)[:180]}"

                    conn.execute(
                        "UPDATE source_files SET status=?, error=? WHERE s3_key=?",
                        (status, error, row["s3_key"]),
                    )
                    if status == "ok" and rec:
                        conn.execute("""INSERT OR REPLACE INTO observations
                            (s3_key,release,period,obsid,detector,product,mjd,date_obs,center_ra,center_dec,
                             radius_deg,corners_json,image_width,image_height)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                            (rec["s3_key"], rec["release"], row["period"], row["obsid"], rec["det"], rec["product"],
                             rec["mjd"], rec["date_obs"], rec["center_ra"], rec["center_dec"], rec["radius_deg"],
                             json.dumps(rec["corners"], separators=(",", ":")), rec["image_width"], rec["image_height"]))
                    processed += 1
                    bar.update(1)
                conn.commit()
    conn.commit()
    print(f"Scanned {processed:,} files in {time.time() - started:,.1f}s")


class HandleLRU:
    def __init__(self, directory: Path, max_open: int = 96):
        self.directory = directory
        self.max_open = max_open
        self.handles: OrderedDict[Tuple[int, int], object] = OrderedDict()

    def write(self, tile: Tuple[int, int], line: str):
        handle = self.handles.get(tile)
        if handle is None:
            if len(self.handles) >= self.max_open:
                _, old = self.handles.popitem(last=False)
                old.close()
            path = self.directory / f"tile_ra_{tile[0]:03d}_dec_{tile[1]:02d}.jsonl"
            handle = open(path, "a", encoding="utf-8")
            self.handles[tile] = handle
        else:
            self.handles.move_to_end(tile)
        handle.write(line + "\n")

    def close(self):
        for handle in self.handles.values():
            handle.close()
        self.handles.clear()


def export_tiles(conn: sqlite3.Connection, out_dir: str) -> Dict:
    out = Path(out_dir)
    raw_dir = out / "_raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    for p in raw_dir.glob("*.jsonl"):
        p.unlink()
    for p in out.glob("tile_ra_*_dec_*.jsonl.gz"):
        p.unlink()

    writer = HandleLRU(raw_dir)
    total = conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
    cursor = conn.execute("""SELECT s3_key,release,detector,product,mjd,obsid,center_ra,center_dec,
                             radius_deg,corners_json FROM observations ORDER BY s3_key""")
    with tqdm(total=total, desc="Writing spatial tiles", unit="image") as bar:
        while True:
            rows = cursor.fetchmany(5000)
            if not rows:
                break
            for (key, release, det, product, mjd, obsid, cra, cdec, radius, corners_json) in rows:
                corners = json.loads(corners_json)
                record = {
                    "uri": S3_BASE + key,
                    "mjd": float(mjd),
                    "det": int(det),
                    "obs": f"{obsid}D{det}",
                    "release": release,
                    "collection": f"spherex_{release}",
                    "product": product,
                    "center_ra": float(cra),
                    "center_dec": float(cdec),
                    "radius_deg": float(radius),
                }
                line = json.dumps(record, separators=(",", ":"), ensure_ascii=False)
                for tile in _tile_ids(corners, float(cra), float(cdec)):
                    writer.write(tile, line)
                bar.update(1)
    writer.close()

    # Emit all tile files, including empty ones, so an empty shard is distinguishable
    # from a missing/incompletely-uploaded shard (which should return 404).
    for iy in range(N_DEC):
        for ix in range(N_RA):
            name = f"tile_ra_{ix:03d}_dec_{iy:02d}.jsonl.gz"
            raw_path = raw_dir / name.replace(".gz", "")
            gz_path = out / name
            with gzip.open(gz_path, "wb", compresslevel=6) as dst:
                if raw_path.exists():
                    with open(raw_path, "rb") as src:
                        shutil.copyfileobj(src, dst, length=1024 * 1024)

    # Remove temporary uncompressed shards after successful gzip export.
    shutil.rmtree(raw_dir, ignore_errors=True)
    obs_count = conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
    status_counts = dict(conn.execute("SELECT status,COUNT(*) FROM source_files GROUP BY status").fetchall())
    manifest = {
        "schema_version": 1,
        "generated_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "bucket": BUCKET,
        "tile_degrees": TILE_DEG,
        "ra_tiles": N_RA,
        "dec_tiles": N_DEC,
        "tile_files": N_RA * N_DEC,
        "indexed_observations": obs_count,
        "source_status_counts": status_counts,
        "complete": status_counts.get("pending", 0) == 0 and status_counts.get("error", 0) == 0,
        "tile_filename_pattern": "tile_ra_{ra:03d}_dec_{dec:02d}.jsonl.gz",
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="chronos102_retry_audit.csv")
    ap.add_argument("--db", default="chronos102_spherex_build.sqlite")
    ap.add_argument("--out", default="spherex_index_tiles")
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--retry-errors", action="store_true")
    args = ap.parse_args()

    if not os.path.exists(args.input):
        raise SystemExit(f"Input audit CSV not found: {args.input}")

    conn = open_db(args.db)
    try:
        added = load_source_csv(conn, args.input)
        print(f"Source rows newly added: {added:,}")
        source_count = conn.execute("SELECT COUNT(*) FROM source_files").fetchone()[0]
        print(f"Total source FITS records: {source_count:,}")
        scan_pending(conn, max(1, args.workers), args.retry_errors)

        counts = dict(conn.execute("SELECT status,COUNT(*) FROM source_files GROUP BY status").fetchall())
        print("Source status:", counts)
        if counts.get("pending", 0) or counts.get("error", 0):
            print("INDEX NOT EXPORTED: scan has pending/error records.")
            print("Rerun with --retry-errors after checking network errors.")
            return

        manifest = export_tiles(conn, args.out)
        print(json.dumps(manifest, indent=2))
        print(f"Index shards written to: {args.out}/")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
