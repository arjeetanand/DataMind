import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Dict


API = "http://localhost:8000"
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def get_json(path: str) -> Dict[str, Any]:
    with urllib.request.urlopen(API + path, timeout=15) as r:
        return json.loads(r.read().decode())


def post_json(path: str, payload: Dict[str, Any], retries: int = 8, sleep_s: float = 0.6) -> Dict[str, Any]:
    last_err = None
    for _ in range(retries):
        req = urllib.request.Request(
            API + path,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="ignore")
            last_err = RuntimeError(f"HTTP {e.code}: {body}")
            if e.code >= 500:
                time.sleep(sleep_s)
                continue
            raise
        except Exception as e:
            last_err = e
            time.sleep(sleep_s)
    raise RuntimeError(f"POST {path} failed after retries: {last_err}")


def snapshot(tag: str) -> Dict[str, Any]:
    s = get_json("/live/status")
    k = get_json("/live/kpis")
    return {
        "tag": tag,
        "current_day": s.get("current_day"),
        "total_rows": s.get("total_rows"),
        "is_running": s.get("is_running"),
        "speed_mode": s.get("speed_mode"),
        "days_streamed": s.get("days_streamed"),
        "mape": s.get("mape"),
        "total_txns": k.get("total_txns"),
        "total_live_revenue": k.get("total_live_revenue"),
    }


def start_proc(args):
    return subprocess.Popen(
        args,
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def stop_proc(p: subprocess.Popen):
    if not p:
        return
    if p.poll() is None:
        p.terminate()
        try:
            p.wait(timeout=8)
        except Exception:
            p.kill()


def main():
    out: Dict[str, Any] = {"tests": []}

    # smoke
    out["health"] = get_json("/health")

    # Test 1: producer only
    post_json("/live/reset", {"confirm": True})
    t1 = {"name": "producer_only", "before": snapshot("before")}
    p_prod = start_proc([sys.executable, "-m", "src.streaming.producer", "--speed", "normal"])
    time.sleep(8)
    t1["after_8s"] = snapshot("after_8s")
    stop_proc(p_prod)
    out["tests"].append(t1)

    # Test 2: consumer only
    post_json("/live/reset", {"confirm": True})
    t2 = {"name": "consumer_only", "before": snapshot("before")}
    p_cons = start_proc([sys.executable, "-m", "src.streaming.consumer"])
    time.sleep(8)
    t2["after_8s"] = snapshot("after_8s")
    stop_proc(p_cons)
    out["tests"].append(t2)

    # Test 3: both + speed modes
    post_json("/live/reset", {"confirm": True})
    p_cons = start_proc([sys.executable, "-m", "src.streaming.consumer"])
    p_prod = start_proc([sys.executable, "-m", "src.streaming.producer", "--speed", "normal"])
    t3 = {"name": "speed_modes"}
    t3["normal_t0"] = snapshot("normal_t0")
    time.sleep(6)
    t3["normal_t6"] = snapshot("normal_t6")
    time.sleep(6)
    t3["normal_t12"] = snapshot("normal_t12")

    post_json("/live/control/speed", {"speed_mode": "fast"})
    time.sleep(5)
    t3["fast_t5"] = snapshot("fast_t5")

    post_json("/live/control/speed", {"speed_mode": "burst"})
    time.sleep(4)
    t3["burst_t4"] = snapshot("burst_t4")

    stop_proc(p_prod)
    stop_proc(p_cons)
    out["tests"].append(t3)

    # Test 4: reset behavior
    t4 = {"name": "reset_effect", "before_reset": snapshot("before_reset")}
    post_json("/live/reset", {"confirm": True})
    time.sleep(1)
    t4["after_reset"] = snapshot("after_reset")
    out["tests"].append(t4)

    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
