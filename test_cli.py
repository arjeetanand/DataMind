"""
DataMind — Interactive Terminal Test Suite
Tests each feature of the DataMind API one-by-one.

Usage:
    python test_cli.py              # run with default API at localhost:8000
    python test_cli.py --port 8001  # custom port
"""

import sys
import time
import json
import argparse
import requests

# ── Colour helpers (no deps) ──────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
DIM    = "\033[2m"
RESET  = "\033[0m"

def ok(msg):   print(f"  {GREEN}✔{RESET}  {msg}")
def fail(msg): print(f"  {RED}✘{RESET}  {msg}")
def warn(msg): print(f"  {YELLOW}⚠{RESET}  {msg}")
def info(msg): print(f"  {CYAN}→{RESET}  {msg}")
def hdr(title):
    line = "─" * 60
    print(f"\n{BOLD}{CYAN}{line}{RESET}")
    print(f"{BOLD}{CYAN}  {title}{RESET}")
    print(f"{BOLD}{CYAN}{line}{RESET}")

def hr(): print(f"  {DIM}{'·' * 56}{RESET}")


# ── HTTP helpers ──────────────────────────────────────────────────────────────
def get(base, path, params=None, timeout=10):
    try:
        r = requests.get(f"{base}{path}", params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.ConnectionError:
        return None
    except Exception as e:
        err_msg = str(e)
        if hasattr(e, "response") and e.response is not None:
            try:
                err_msg = f"{e} | Response: {e.response.text}"
            except: pass
        return {"__error__": err_msg, "__status__": getattr(e, "response", None) and e.response.status_code}


def post(base, path, body=None, timeout=15):
    try:
        r = requests.post(f"{base}{path}", json=body or {}, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.ConnectionError:
        return None
    except Exception as e:
        err_msg = str(e)
        if hasattr(e, "response") and e.response is not None:
            try:
                err_msg = f"{e} | Response: {e.response.text}"
            except: pass
        return {"__error__": err_msg}


def pause():
    input(f"\n  {DIM}Press ENTER to continue to next test...{RESET}")


# ════════════════════════════════════════════════════════════════════════════
#  Individual Tests
# ════════════════════════════════════════════════════════════════════════════

def test_health(base):
    hdr("TEST 1 — API Health Check")
    data = get(base, "/health")
    if data is None:
        fail("Cannot reach API — is it running?")
        return False
    if data.get("status") == "ok":
        ok(f"API online  |  service={data.get('service')}  |  ts={data.get('timestamp'):.0f}")
        return True
    fail(f"Unexpected response: {data}")
    return False


def test_live_status(base):
    hdr("TEST 2 — Live Stream Status")
    data = get(base, "/live/status")
    if data is None:
        fail("No response from /live/status"); return

    err = data.get("status") == "maintenance"
    if err:
        warn("System is in maintenance mode (still resetting?)")
        return

    is_run = data.get("is_running")
    day    = data.get("current_day")
    rows   = data.get("total_rows", 0)
    days   = data.get("days_streamed", 0)
    mape   = data.get("mape")
    speed  = data.get("speed_mode", "normal")

    ok(f"is_running={is_run}")
    info(f"current_day   = {day}")
    info(f"total_rows    = {rows:,}")
    info(f"days_streamed = {days}")
    info(f"speed_mode    = {speed}")
    if mape is not None:
        info(f"MAPE accuracy = {mape:.1f}%")
        if mape < 20:
            ok(f"Model accuracy is GOOD (MAPE {mape:.1f}% < 20%)")
        elif mape < 40:
            warn(f"Model accuracy is FAIR (MAPE {mape:.1f}%)")
        else:
            fail(f"Model accuracy is POOR (MAPE {mape:.1f}% > 40%) — check scale correction")
    else:
        warn("MAPE not yet computed (need at least 1 finalized day)")


def test_live_kpis(base):
    hdr("TEST 3 — Live KPIs (Revenue / TPS / Orders)")
    data = get(base, "/live/kpis")
    if data is None:
        fail("No response from /live/kpis"); return

    if data.get("status") == "maintenance":
        warn("System is resetting — try again shortly"); return

    rev   = data.get("total_live_revenue", 0)
    txns  = data.get("total_txns", 0)
    units = data.get("total_units", 0)
    tps   = data.get("tps", 0)
    cust  = data.get("unique_customers", 0)
    avg   = data.get("avg_txn_value", 0)

    ok(f"total_live_revenue  = £{rev:>12,.2f}")
    info(f"total_transactions = {txns:,}")
    info(f"total_units        = {units:,}")
    info(f"unique_customers   = {cust:,}")
    info(f"avg_txn_value      = £{avg:,.2f}")
    hr()

    # TPS check (Bug 2 fix verification)
    status = get(base, "/live/status") or {}
    if status.get("is_running"):
        if tps > 0:
            ok(f"TPS (ingestion rate) = {tps} tx/s  — WORKING ✔")
        else:
            warn(f"TPS = 0 while stream is running — may update within 1s, re-run to confirm")
    else:
        if tps == 0:
            ok(f"TPS = 0 (stream paused) — CORRECT ✔")
        else:
            warn(f"TPS = {tps} but stream is paused — stale value? Try a Reset if this persists.")


def test_forecast_outlook(base):
    hdr("TEST 4 — Forecast Outlook (Bug 1 Fix Verification)")
    data = get(base, "/live/forecast-outlook")
    if data is None:
        fail("No response from /live/forecast-outlook"); return

    rows = data.get("data", [])
    if not rows:
        warn("No forecast data yet — need at least 1 completed simulated day")
        info("Tip: let the simulation run until 'Day Finalized:' appears in the consumer logs")
        return

    info(f"Forecast horizon: {len(rows)} days")
    hr()
    print(f"  {'Date':<14} {'Predicted':>12} {'Lower CI':>12} {'Upper CI':>12}")
    print(f"  {'────':<14} {'─────────':>12} {'────────':>12} {'────────':>12}")
    for row in rows:
        day  = row.get("day", "?")
        pred = row.get("predicted", 0)
        lo   = row.get("lower_ci", 0)
        hi   = row.get("upper_ci", 0)
        print(f"  {day:<14} £{pred:>11,.0f} £{lo:>11,.0f} £{hi:>11,.0f}")
    hr()

    # Scale check — daily live revenue should be >> £5,000 if simulation is running
    mean_pred = sum(r.get("predicted", 0) for r in rows) / len(rows)
    info(f"Mean predicted daily revenue = £{mean_pred:,.0f}")
    if mean_pred > 10_000:
        ok(f"Scale looks CORRECT (> £10,000/day) ✔")
    elif mean_pred > 1_000:
        warn(f"Scale seems LOW — may still be warming up, or scale correction not yet applied")
    else:
        fail(f"Scale is WRONG (<£1,000/day) — check consumer logs for 'scale correction' lines")


def test_forecast_vs_actual(base):
    hdr("TEST 5 — Forecast vs Actual")
    data = get(base, "/live/forecast-vs-actual")
    if data is None:
        fail("No response from /live/forecast-vs-actual"); return

    rows = data.get("data", [])
    if not rows:
        warn("No data yet — at least 1 day must be both forecasted AND finalized")
        return

    info(f"{len(rows)} completed day(s) in comparison")
    hr()
    print(f"  {'Date':<14} {'Predicted':>12} {'Actual':>12} {'APE %':>8}")
    print(f"  {'────':<14} {'─────────':>12} {'──────':>12} {'─────':>8}")
    for row in rows:
        day  = row.get("day", "?")
        pred = row.get("predicted", 0)
        act  = row.get("actual", 0)
        ape  = row.get("ape_pct")
        ape_str = f"{ape:.1f}%" if ape is not None else "n/a"
        color = GREEN if ape is not None and ape < 20 else (YELLOW if ape is not None and ape < 40 else RED)
        print(f"  {day:<14} £{pred:>11,.0f} £{act:>11,.0f} {color}{ape_str:>8}{RESET}")


def test_start_stop(base):
    hdr("TEST 6 — Start / Stop Simulation")
    print(f"  {DIM}Testing start → stop → start cycle{RESET}")

    # Stop first
    data = post(base, "/live/stop")
    if data and not data.get("__error__"):
        ok("POST /live/stop → OK")
    else:
        fail(f"Stop failed: {data}")
        return

    time.sleep(1)
    status = get(base, "/live/status") or {}
    if not status.get("is_running"):
        ok("Status confirmed: is_running = False")
    else:
        warn("Status still shows is_running = True, check control file")

    # Start
    data = post(base, "/live/start")
    if data and not data.get("__error__"):
        ok("POST /live/start → OK")
    else:
        fail(f"Start failed: {data}")
        return

    time.sleep(1)
    status = get(base, "/live/status") or {}
    if status.get("is_running"):
        ok("Status confirmed: is_running = True")
    else:
        warn("Status still shows is_running = False, check if producer is running")


def test_reset(base):
    hdr("TEST 7 — Reset (Bug 3 Fix Verification)")
    warn("This will WIPE all live data, Kafka, ClickHouse, Redis and DuckDB live tables!")
    choice = input(f"  {YELLOW}Proceed with reset? [y/N]: {RESET}").strip().lower()
    if choice != "y":
        info("Skipped reset test.")
        return

    info("Sending POST /live/reset with confirm=true ...")
    t0 = time.time()
    data = post(base, "/live/reset", {"confirm": True}, timeout=30)
    elapsed = time.time() - t0

    if data is None:
        fail("No response — API may have crashed OR timed out"); return

    err = data.get("__error__")
    if err:
        fail(f"Reset returned error: {err}")
        if "NameError" in str(err) or "_clear_clickhouse" in str(err):
            fail("NameError: _clear_clickhouse_live_data is missing — ensure you are running the patched main.py!")
        return

    status = data.get("status")
    if status == "ok":
        ok(f"Reset succeeded in {elapsed:.1f}s")
        info(f"message: {data.get('message')}")
        ch_ok = data.get("clickhouse_cleared")
        if ch_ok:
            ok("ClickHouse table cleared ✔")
        else:
            warn("ClickHouse clear failed — hot table data may persist (ClickHouse down?)")
    else:
        fail(f"Unexpected reset response: {data}")
        return

    # Verify KPIs are zero
    time.sleep(1.5)
    kpis = get(base, "/live/kpis") or {}
    rev  = kpis.get("total_live_revenue", -1)
    tps  = kpis.get("tps", -1)
    if rev == 0:
        ok("Post-reset: total_live_revenue = 0 ✔")
    else:
        warn(f"Post-reset: total_live_revenue = {rev} (may take a moment to propagate)")
    if tps == 0:
        ok("Post-reset: tps = 0 ✔  (Bug 2 fix confirmed)")
    else:
        warn(f"Post-reset: tps = {tps} (should be 0 — live:tps not cleared?)")


def test_transactions(base):
    hdr("TEST 8 — Recent Transactions Feed")
    data = get(base, "/live/transactions", {"n": 5})
    if data is None:
        fail("No response from /live/transactions"); return

    rows = data.get("data", [])
    if not rows:
        warn("No transactions yet — start simulation first"); return

    info(f"Showing {len(rows)} recent transactions:")
    hr()
    for i, row in enumerate(rows, 1):
        desc = str(row.get("description", ""))[:30]
        rev  = row.get("revenue", 0)
        ctry = row.get("country", "?")
        inv  = row.get("invoice", "?")
        print(f"  [{i}] {inv:<12} {desc:<32} £{float(rev):>8.2f}  {ctry}")


def test_top_products(base):
    hdr("TEST 9 — Live Top Products")
    data = get(base, "/live/top-products", {"n": 5})
    if data is None:
        fail("No response from /live/top-products"); return

    rows = data.get("data", [])
    if not rows:
        warn("No product data yet"); return

    print(f"  {'Rank':<5} {'SKU':<10} {'Description':<35} {'Revenue':>12} {'Units':>8}")
    print(f"  {'────':<5} {'───':<10} {'───────────':<35} {'───────':>12} {'─────':>8}")
    for i, row in enumerate(rows, 1):
        sku  = str(row.get("stock_code", "?"))[:10]
        desc = str(row.get("description", "?"))[:34]
        rev  = row.get("total_revenue", 0)
        unit = row.get("total_units", 0)
        print(f"  {i:<5} {sku:<10} {desc:<35} £{float(rev):>11,.0f} {int(unit):>8,}")


# ════════════════════════════════════════════════════════════════════════════
#  MENU
# ════════════════════════════════════════════════════════════════════════════

TESTS = [
    ("Health Check",             test_health),
    ("Live Stream Status",       test_live_status),
    ("Live KPIs + TPS check",    test_live_kpis),
    ("Forecast Outlook",         test_forecast_outlook),
    ("Forecast vs Actual",       test_forecast_vs_actual),
    ("Start / Stop Simulation",  test_start_stop),
    ("Reset (destructive)",      test_reset),
    ("Recent Transactions Feed", test_transactions),
    ("Live Top Products",        test_top_products),
]


def menu(base):
    while True:
        print(f"\n{BOLD}{'═'*60}{RESET}")
        print(f"{BOLD}  DataMind — Terminal Test Suite   {DIM}[{base}]{RESET}")
        print(f"{BOLD}{'═'*60}{RESET}")
        for i, (name, _) in enumerate(TESTS, 1):
            print(f"  {CYAN}{i:>2}{RESET}.  {name}")
        print(f"  {CYAN} A{RESET}.  Run ALL tests (skips Reset)")
        print(f"  {CYAN} Q{RESET}.  Quit")
        print(f"{DIM}{'─'*60}{RESET}")

        choice = input(f"  {BOLD}Select test: {RESET}").strip().lower()

        if choice == "q":
            print(f"\n  {DIM}Goodbye.{RESET}\n")
            break
        elif choice == "a":
            for name, fn in TESTS:
                if "reset" in name.lower():
                    continue
                fn(base)
                pause()
        else:
            try:
                idx = int(choice) - 1
                if 0 <= idx < len(TESTS):
                    TESTS[idx][1](base)
                else:
                    warn("Invalid choice")
            except ValueError:
                warn("Invalid choice")


# ════════════════════════════════════════════════════════════════════════════
#  Entry Point
# ════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DataMind Terminal Test Suite")
    parser.add_argument("--host", default="localhost", help="API host (default: localhost)")
    parser.add_argument("--port", default=8000, type=int, help="API port (default: 8000)")
    args = parser.parse_args()

    BASE = f"http://{args.host}:{args.port}"

    # Quick reachability check
    print(f"\n  {DIM}Connecting to {BASE} ...{RESET}")
    probe = get(BASE, "/health")
    if probe is None:
        print(f"\n  {RED}✘ Cannot reach API at {BASE}{RESET}")
        print(f"  {DIM}Make sure the API is running:  .\\venv\\Scripts\\python.exe -m src.api.main{RESET}\n")
        sys.exit(1)

    menu(BASE)
