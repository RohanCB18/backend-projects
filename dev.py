"""Start the whole thing with one command:

    python dev.py            all 5 backends + the frontend  -> http://127.0.0.1:5173
    python dev.py 1 4        only backends 01 and 04 (+ frontend)
    python dev.py --no-ui    backends only

Ctrl+C stops everything (whole process trees, including uvicorn workers and
cache nodes).
"""
from __future__ import annotations

import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time

ROOT = os.path.dirname(os.path.abspath(__file__))
WIN = sys.platform == "win32"
VENV_PY = os.path.join(ROOT, ".venv", "Scripts" if WIN else "bin", "python.exe" if WIN else "python")
PY = VENV_PY if os.path.exists(VENV_PY) else sys.executable

SERVICES = {
    1: ("01-rate-limiter", 9101, []),
    2: ("02-job-queue", 9102, []),
    3: ("03-ledger", 9103, []),
    4: ("04-collab-sync", 9104, []),
    5: ("05-distributed-cache", 9105, []),  # the controller; it starts nodes on 9201+
}
COLORS = {1: "33", 2: "35", 3: "32", 4: "36", 5: "31", "ui": "34"}


def port_busy(port: int) -> bool:
    with socket.socket() as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


def pump(name, color, stream):
    tag = f"\033[{color}m[{name:>3}]\033[0m " if sys.stdout.isatty() or WIN else f"[{name:>3}] "
    for line in iter(stream.readline, b""):
        sys.stdout.write(tag + line.decode(errors="replace"))
        sys.stdout.flush()


def start(name, color, cmd, cwd, env=None):
    kw = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP} if WIN else {"start_new_session": True}
    p = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                         env={**os.environ, "PYTHONUNBUFFERED": "1", **(env or {})}, **kw)
    threading.Thread(target=pump, args=(name, color, p.stdout), daemon=True).start()
    return p


def kill_tree(p):
    if p.poll() is not None:
        return
    if WIN:
        subprocess.run(["taskkill", "/T", "/F", "/PID", str(p.pid)], capture_output=True)
    else:
        try:
            os.killpg(p.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass


def main() -> int:
    # Windows consoles default to cp1252; Vite prints characters like "➜"
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    picked = [int(a) for a in args] or list(SERVICES)
    ui = "--no-ui" not in sys.argv

    if PY == sys.executable and not os.path.exists(VENV_PY):
        print("note: no .venv found, using this Python. Create one with:\n"
              "  python -m venv .venv && .venv\\Scripts\\pip install -r requirements.txt")
    procs = []
    for n in picked:
        folder, port, extra = SERVICES[n]
        if port_busy(port):
            print(f"!! port {port} is already in use: is project 0{n} already running? skipping it")
            continue
        cmd = [PY, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", str(port),
               "--log-level", "warning", *extra]
        procs.append(start(f"0{n}", COLORS[n], cmd, os.path.join(ROOT, folder)))
        print(f"   0{n} {folder:<22} http://127.0.0.1:{port}/docs")

    if ui:
        fe = os.path.join(ROOT, "frontend")
        npm = shutil.which("npm.cmd" if WIN else "npm") or "npm"
        if not os.path.isdir(os.path.join(fe, "node_modules")):
            print("   installing frontend dependencies (first run only)...")
            subprocess.run([npm, "install", "--no-audit", "--no-fund"], cwd=fe, check=True)
        procs.append(start("ui", COLORS["ui"], [npm, "run", "dev", "--", "--host", "127.0.0.1"], fe))
        # 127.0.0.1, not "localhost": on Windows "localhost" tries IPv6 first and
        # every new connection pays ~200 ms before falling back to IPv4.
        print("\n   >>> open http://127.0.0.1:5173  (keys 1-5 switch projects)\n")

    try:
        while True:  # if one service dies, its log says why; the others keep running
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nstopping...")
    finally:
        for p in procs:
            kill_tree(p)
    return 0


if __name__ == "__main__":
    sys.exit(main())
