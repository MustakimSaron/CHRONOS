"""
Shared citizen-science ledger for CHRONOS-102.

* Anyone can flag a candidate; flags within 6 arcsec of an existing candidate count as a "real" vote for it.
* Everyone (one vote per browser, changeable) votes: real / artifact / known object.
* Status follows simple, transparent consensus rules (see `_status`).
* Stored in data/ledger.json. On Hugging Face Spaces (whose disk resets on restart) it can also be mirrored to a
  Hugging Face *dataset* repo: set the Space secrets HF_TOKEN and CHRONOS_LEDGER_REPO (e.g. "you/chronos-ledger").
"""
from __future__ import annotations

import json
import math
import os
import threading
import time
from typing import Dict, List, Optional

VOTES = ("real", "artifact", "known")
WRITES_PER_HOUR = 40
MERGE_ARCSEC = 6.0


class LedgerError(Exception):
    pass


def _status(e: Dict) -> str:
    r, a, k = e["votes"]["real"], e["votes"]["artifact"], e["votes"]["known"]
    if e.get("known_object") or (k >= 2 and k >= r):
        return "Known object" + (f" ({e['known_object']})" if e.get("known_object") else "")
    if a >= 3 and a > r:
        return "Rejected: artifact"
    if r >= 3 and r >= 2 * a:
        return "Community confirmed"
    total = r + a + k
    if total >= 3:
        return f"Disputed ({r} real · {a} artifact · {k} known)"
    return f"Awaiting votes ({total}/3)"


class Ledger:
    def __init__(self, path: str, seed: Optional[List[Dict]] = None):
        self.path, self.lock = path, threading.Lock()
        self.rate: Dict[str, List[float]] = {}
        self.repo = os.environ.get("CHRONOS_LEDGER_REPO")
        self.token = os.environ.get("HF_TOKEN")
        self._sync_timer = None
        self.items: List[Dict] = self._load()
        if not self.items and seed:
            for s in seed:
                self.items.append(self._normalise(dict(s, example=True)))
            self._save()

    # ---------------- storage
    def storage_info(self) -> str:
        return f"Hugging Face dataset {self.repo}" if (self.repo and self.token) else "server disk"

    def _load(self) -> List[Dict]:
        if self.repo and self.token:
            try:
                from huggingface_hub import hf_hub_download  # optional dependency
                p = hf_hub_download(self.repo, "ledger.json", repo_type="dataset", token=self.token, force_download=True)
                with open(p, encoding="utf-8") as fh:
                    return [self._normalise(x) for x in json.load(fh)]
            except Exception:
                pass
        try:
            with open(self.path, encoding="utf-8") as fh:
                return [self._normalise(x) for x in json.load(fh)]
        except Exception:
            return []

    def _save(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(self.items, fh, indent=1)
        os.replace(tmp, self.path)
        if self.repo and self.token:  # mirror to the dataset repo, at most every 20 s
            if self._sync_timer is None:
                self._sync_timer = threading.Timer(20.0, self._push)
                self._sync_timer.daemon = True
                self._sync_timer.start()

    def _push(self):
        self._sync_timer = None
        try:
            from huggingface_hub import HfApi
            HfApi(token=self.token).upload_file(path_or_fileobj=self.path, path_in_repo="ledger.json", repo_id=self.repo,
                                                repo_type="dataset", commit_message="CHRONOS-102 ledger update")
        except Exception:
            pass

    @staticmethod
    def _normalise(e: Dict) -> Dict:
        e.setdefault("votes", {"real": int(e.get("verifications", 1) or 1), "artifact": 0, "known": 0})
        e.setdefault("voters", {})
        e.setdefault("created", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
        e["status"] = _status(e)
        return e

    # ---------------- rules
    def _rate_ok(self, ip: str):
        now = time.time()
        hits = [t for t in self.rate.get(ip, []) if now - t < 3600]
        if len(hits) >= WRITES_PER_HOUR:
            raise LedgerError("too many submissions from this address; try again later")
        hits.append(now)
        self.rate[ip] = hits

    def list(self) -> List[Dict]:
        with self.lock:
            out = []
            for e in self.items:
                x = {k: v for k, v in e.items() if k != "voters"}
                x["n_voters"] = len(e.get("voters", {}))
                out.append(x)
            return out

    def submit(self, s: Dict, ip: str) -> Dict:
        if not (0 <= s["ra"] < 360 and -90 <= s["dec"] <= 90):
            raise LedgerError("RA/Dec out of range")
        cls = str(s.get("classification", ""))[:120]
        who = (str(s.get("discoverer") or "Citizen Scientist").strip() or "Citizen Scientist")[:60]
        voter = str(s.get("voter", ""))[:64] or ip
        with self.lock:
            self._rate_ok(ip)
            for e in self.items:  # same object flagged again -> counts as a confirming vote
                d = math.hypot((e["ra"] - s["ra"]) * math.cos(math.radians(s["dec"])), e["dec"] - s["dec"]) * 3600
                if d <= MERGE_ARCSEC and not e.get("example"):
                    self._apply_vote(e, "real", voter)
                    self._save()
                    return dict(e, merged=True)
            n = 1 + max([int(str(e["id"]).split("-")[-1]) for e in self.items if str(e.get("id", "")).count("-") == 2] or [0])
            e = {"id": f"CAND-{time.strftime('%Y')}-{n:03d}", "ra": round(s["ra"], 5), "dec": round(s["dec"], 5),
                 "snr": round(float(s.get("snr", 0)), 1), "velocity_arcsec_yr": round(float(s.get("velocity_arcsec_yr", 0)), 3),
                 "classification": cls, "discoverer": who, "field": str(s.get("field", ""))[:80],
                 "data_mode": "real" if s.get("data_mode") == "real" else "sim", "known_object": str(s.get("known_object", ""))[:60],
                 "votes": {"real": 0, "artifact": 0, "known": 0}, "voters": {},
                 "created": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
            self._apply_vote(e, "real", voter)
            self.items.insert(0, e)
            self._save()
            return e

    def _apply_vote(self, e: Dict, vote: str, voter: str):
        prev = e["voters"].get(voter)
        if prev:
            e["votes"][prev] = max(0, e["votes"][prev] - 1)
        e["voters"][voter] = vote
        e["votes"][vote] += 1
        e["status"] = _status(e)

    def vote(self, cand_id: str, vote: str, voter: str, ip: str) -> Dict:
        if vote not in VOTES:
            raise LedgerError("vote must be real, artifact or known")
        voter = (voter or ip)[:64]
        with self.lock:
            self._rate_ok(ip)
            for e in self.items:
                if e["id"] == cand_id:
                    self._apply_vote(e, vote, voter)
                    self._save()
                    return {k: v for k, v in e.items() if k != "voters"}
        raise LedgerError("unknown candidate")
