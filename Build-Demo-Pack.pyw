"""
CHRONOS-102 - build the demo pack (run this ONCE on your own computer).

Double-click this file. It
  1. installs the few Python packages it needs (first run only),
  2. starts a small private server that only your own computer can reach,
  3. opens CHRONOS-102 in your browser, which downloads real NASA / ESA archive data for the six
     demonstration targets and saves it into  website/demo/ ,
  4. tells you when it is finished.

Afterwards, copy the WHOLE folder to GitHub. Nothing else has to be run.
Works on Windows (double-click), macOS and Linux (python3 Build-Demo-Pack.pyw).
"""
import json
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.request
import webbrowser

HERE = os.path.dirname(os.path.abspath(__file__))
BUILDER = os.path.join(HERE, "local-builder")
DEMO = os.path.join(HERE, "website", "demo")
NO_WINDOW = 0x08000000 if os.name == "nt" else 0
PACKAGES = (("fastapi", "fastapi"), ("uvicorn", "uvicorn[standard]"), ("numpy", "numpy"), ("requests", "requests"), ("pydantic", "pydantic"))

if sys.stdout is None:  # pythonw has no console
    sys.stdout = open(os.devnull, "w")
if sys.stderr is None:
    sys.stderr = open(os.devnull, "w")

try:
    import tkinter as tk
    from tkinter import messagebox
except Exception:  # no tkinter: plain console mode
    tk = None


class UI:
    """A tiny status window (or console lines when tkinter is not available)."""

    def __init__(self):
        self.root = None
        if tk:
            try:
                self.root = tk.Tk()
                self.root.title("CHRONOS-102 demo pack builder")
                self.root.geometry("560x230")
                self.root.resizable(False, False)
                self.head = tk.Label(self.root, text="CHRONOS-102 demo pack builder", font=("Segoe UI", 14, "bold"), anchor="w")
                self.head.pack(fill="x", padx=18, pady=(16, 4))
                self.msg = tk.Label(self.root, text="Starting…", justify="left", anchor="nw", wraplength=520, font=("Segoe UI", 10))
                self.msg.pack(fill="both", expand=True, padx=18)
                self.btn = tk.Button(self.root, text="Stop and close", command=self.close, width=16)
                self.btn.pack(pady=(0, 16))
                self.root.protocol("WM_DELETE_WINDOW", self.close)
            except Exception:
                self.root = None

    def say(self, text):
        if self.root:
            self.msg.config(text=text)
            self.root.update_idletasks()
        else:
            print(text, flush=True)

    def pump(self):
        if self.root:
            try:
                self.root.update()
            except Exception:
                raise SystemExit
        else:
            time.sleep(0.2)

    def done(self, title, text):
        if self.root:
            self.head.config(text=title)
            self.msg.config(text=text)
            self.btn.config(text="Close")
        else:
            print(title + "\n" + text, flush=True)

    def close(self):
        raise SystemExit


def missing():
    out = []
    for mod, pkg in PACKAGES:
        try:
            __import__(mod)
        except Exception:
            out.append(pkg)
    return out


def free_port(start=8765):
    for p in range(start, start + 40):
        with socket.socket() as s:
            if s.connect_ex(("127.0.0.1", p)) != 0:
                return p
    raise RuntimeError("No free local port found.")


def get_json(url, timeout=2):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def main():
    ui = UI()
    try:
        ui.say("Checking the internet connection…")
        try:
            urllib.request.urlopen("https://irsa.ipac.caltech.edu/", timeout=15).close()
        except Exception:
            ui.say("Warning: NASA's IRSA archive did not answer. The download needs internet access (a VPN may help or block it).\nContinuing anyway…")
            for _ in range(15):
                ui.pump()

        need = missing()
        if need:
            ui.say("First run: installing " + ", ".join(need) + " (about a minute)…")
            p = subprocess.Popen([sys.executable, "-m", "pip", "install", "-q", "-r", os.path.join(BUILDER, "requirements.txt")],
                                 stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, creationflags=NO_WINDOW)
            while p.poll() is None:
                ui.pump()
                time.sleep(0.1)
            if p.returncode != 0 or missing():
                err = (p.stderr.read() or b"").decode("utf-8", "replace")[-600:]
                ui.done("Could not install the packages", "pip failed (no internet for pip, or a proxy blocks it).\n\n" + err)
                while True:
                    ui.pump()

        sys.path.insert(0, BUILDER)
        os.chdir(BUILDER)
        import uvicorn
        import app as chronos

        port = free_port()
        server = uvicorn.Server(uvicorn.Config(chronos.app, host="127.0.0.1", port=port, log_level="warning", log_config=None))
        threading.Thread(target=server.run, daemon=True).start()
        base = f"http://127.0.0.1:{port}"
        for _ in range(100):
            try:
                get_json(base + "/api/health")
                break
            except Exception:
                ui.pump()
                time.sleep(0.2)
        else:
            raise RuntimeError("The local server did not start.")

        webbrowser.open(base + "/tool/?buildpack=1&boot=0")
        ui.say("Your browser opened CHRONOS-102 and is downloading the demo data.\n\nKeep the browser tab open and this window running. It can take 20–60 minutes on a slow connection. You can use the computer meanwhile.")
        t0 = time.time()
        while True:
            ui.pump()
            time.sleep(0.5)
            try:
                st = get_json(base + "/api/demo/state")
            except Exception:
                continue
            if st.get("finished"):
                server.should_exit = True
                if st.get("failed"):
                    ui.done("Finished with problems", "Not saved: " + ", ".join(st["failed"]) + "\n\nRun this file again (targets that were saved are kept), ideally with a better connection.")
                else:
                    ui.done("Demo pack ready", f"Saved {len(st.get('saved', []))} items into:\n{DEMO}\n\nNow copy the WHOLE folder to GitHub.")
                while True:
                    ui.pump()
            if int(time.time() - t0) % 30 == 0 and ui.root:
                ui.msg.config(text=ui.msg.cget("text").split("\n\nElapsed")[0] + f"\n\nElapsed: {int((time.time() - t0) // 60)} min")
    except SystemExit:
        pass
    except Exception as exc:  # show the problem instead of closing silently
        ui.done("Something went wrong", f"{type(exc).__name__}: {exc}")
        try:
            while True:
                ui.pump()
        except SystemExit:
            pass


if __name__ == "__main__":
    main()
