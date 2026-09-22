#!/usr/bin/env python3
"""
bybit_verify_groups.py
Verifies symbol group classification for all 4 Bybit downloaders against
live server data. Reads regex patterns and parse_directory_links() directly
from the script files — no changes to the downloader scripts are required.

After run(), the global variable ALL_SYMBOLS contains sorted symbols per
group for each script:
    ALL_SYMBOLS = {
        "futures_trades":    {"symbols": [...], "groups": {"USDT": [...], ...}, ...},
        "futures_orderbook": {"symbols": [...], "groups": {"USDT": [...], ...}, ...},
        "spot_trades":       {"symbols": [...], "groups": {"USDT": [...], ...}, ...},
        "spot_orderbook":    {"symbols": [...], "groups": {"USDT": [...], ...}, ...},
    }

Usage:
  python bybit_verify_groups.py              # all 4 scripts
  python bybit_verify_groups.py --trades     # futures_trades + spot_trades
  python bybit_verify_groups.py --orderbook  # futures_orderbook + spot_orderbook
  python bybit_verify_groups.py --full       # print all symbols per group
  python bybit_verify_groups.py --group STABLE     # show STABLE group across all scripts
  python bybit_verify_groups.py --group USDC       # show USDC group across all scripts
  python bybit_verify_groups.py --group OTHER      # show OTHER group across all scripts
  python bybit_verify_groups.py --group UNSORTED   # show UNSORTED group across all scripts
"""
import argparse
import os
import re
from collections import OrderedDict
from typing import Dict, List, Tuple

import requests

# =============================================================================
# Shared classification sets — imported by the 4 downloader scripts
# =============================================================================
stablecoins = {'BUSD', 'FDUSD', 'USDQ', 'USDR', 'DAI', 'RLUSD', 'USD1', 'USDC', 'USDE',
               'USDT', 'USTC', 'UST', 'XUSD', 'USDD', 'TUSD', 'FRAX', 'CUSD', 'PYUSD',
               'USDY', 'USDTB', 'MUSD', 'STABLE', 'BRZ'}

fiatcoins = {'IDR', 'KZT', 'AED', 'BRL', 'EUR', 'GBP', 'PLN', 'TRY', 'EGP1'}


# =============================================================================
# Script paths — relative to this file
# =============================================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

SCRIPTS = OrderedDict([
    ("futures_trades", {
        "path": os.path.join(SCRIPT_DIR, "bybit_futures_trades.py"),
        "url": "https://public.bybit.com/trading/",
        "var_name": "FUTURES_TRADES_GROUPS",
        "parser": "trades",  # parse_directory_links from trades scripts
    }),
    ("futures_orderbook", {
        "path": os.path.join(SCRIPT_DIR, "bybit_futures_orderbook.py"),
        "url": "https://quote-saver.bycsi.com/orderbook/linear/",
        "var_name": "FUTURES_ORDERBOOK_GROUPS",
        "parser": "orderbook",  # parse_directory_links from orderbook scripts
    }),
    ("futures_orderbook_inverse", {
        "path": os.path.join(SCRIPT_DIR, "bybit_futures_orderbook.py"),
        "url": "https://quote-saver.bycsi.com/orderbook/inverse/",
        "var_name": "FUTURES_ORDERBOOK_GROUPS",
        "parser": "orderbook",
        "label": "futures_orderbook (inverse)",  # display name in output
    }),
    ("spot_trades", {
        "path": os.path.join(SCRIPT_DIR, "bybit_spot_trades.py"),
        "url": "https://public.bybit.com/spot/",
        "var_name": "SPOT_TRADES_GROUPS",
        "parser": "orderbook",  # spot server uses <a href="SYMBOL"> without trailing /
    }),
    ("spot_orderbook", {
        "path": os.path.join(SCRIPT_DIR, "bybit_spot_orderbook.py"),
        "url": "https://quote-saver.bycsi.com/orderbook/spot/",
        "var_name": "SPOT_ORDERBOOK_GROUPS",
        "parser": "orderbook",
    }),
])


# =============================================================================
# parse_directory_links — copied from the downloader scripts
# =============================================================================
# Two variants exist in the scripts:

# Trades scripts (bybit_futures_trades.py, bybit_spot_trades.py):
def _parse_trades(html: str) -> List[str]:
    """Copied from bybit_futures_trades.parse_directory_links"""
    return sorted(re.findall(r'href="([^"]+)/"', html))

# Orderbook scripts (bybit_futures_orderbook.py, bybit_spot_orderbook.py):
def _parse_orderbook(html: str) -> List[str]:
    """Copied from bybit_futures_orderbook.parse_directory_links"""
    links = re.findall(r'href="([^"]+)"', html)
    dirs = []
    for link in links:
        name = link.rstrip("/")
        if name and name != ".." and "/" not in name and "." not in name:
            dirs.append(name)
    return sorted(set(dirs))

_PARSERS = {"trades": _parse_trades, "orderbook": _parse_orderbook}


# =============================================================================
# Extract group dict from a Python script file (ast.literal_eval)
# =============================================================================
def _extract_groups(filepath: str, var_name: str) -> Dict[str, str]:
    with open(filepath, "r", encoding="utf-8") as f:
        text = f.read()
    pattern = re.escape(var_name) + r"""\s*=\s*\{"""
    m = re.search(pattern, text)
    if not m:
        raise ValueError(f"Variable '{var_name}' not found in {filepath}")
    start = m.end() - 1  # include opening {
    depth = 0
    for i in range(start, len(text)):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                return eval(text[start:i + 1],
                             {"__builtins__": {},
                              "stablecoins": stablecoins,
                              "fiatcoins": fiatcoins})
    raise ValueError(f"Could not find closing brace for '{var_name}'")


# =============================================================================
# Global result — populated by run()
# =============================================================================
ALL_SYMBOLS: Dict[str, dict] = {}


# =============================================================================
# Core
# =============================================================================
def run(selected: List[str]) -> None:
    """Fetches symbols from servers, classifies them, populates ALL_SYMBOLS."""
    ALL_SYMBOLS.clear()

    for name in selected:
        cfg = SCRIPTS[name]
        groups = _extract_groups(cfg["path"], cfg["var_name"])
        parse_fn = _PARSERS[cfg["parser"]]

        print(f"Fetching {cfg.get('label', name)}...", end=" ", flush=True)
        resp = requests.get(cfg["url"], timeout=30)
        resp.raise_for_status()
        symbols = parse_fn(resp.text)

        # Dynamically compute INVERSE and CRYPTO patterns from USDT pairs
        usdt_pat = groups.get('USDT')
        if usdt_pat:
            basecoins = {sym[:-4] for sym in symbols
                         if re.match(usdt_pat, sym) and sym.endswith('USDT')
                         and sym[:-4] not in stablecoins}
            if 'INVERSE' in groups and groups['INVERSE'] is None:
                if basecoins:
                    groups['INVERSE'] = rf'^({"|".join(basecoins)})USD$'
                else:
                    groups['INVERSE'] = r'(?!.*)'
            if 'CRYPTO' in groups and groups['CRYPTO'] is None:
                if basecoins:
                    groups['CRYPTO'] = rf'^({"|".join(basecoins)})+({"|".join(basecoins)})$'
                else:
                    groups['CRYPTO'] = r'(?!.*)'

        # Classify: first match wins
        classified: Dict[str, List[str]] = {g: [] for g in groups}
        assigned: Dict[str, str] = {}
        overlaps: List[Tuple[str, str, str]] = []

        for sym in symbols:
            for gname, pat in groups.items():
                if re.match(pat, sym):
                    classified[gname].append(sym)
                    if sym in assigned:
                        overlaps.append((sym, assigned[gname], gname))
                    else:
                        assigned[sym] = gname
                    break

        unmatched = sorted(s for s in symbols if s not in assigned)
        for g in classified:
            classified[g].sort()

        ALL_SYMBOLS[name] = {
            "symbols": symbols,
            "var_name": cfg["var_name"],
            "label": cfg.get("label", name),
            "groups": classified,
            "unmatched": unmatched,
            "overlaps": overlaps,
        }
        matched = len(symbols) - len(unmatched)
        print(f"{len(symbols)} symbols, {matched} classified, "
              f"{len(unmatched)} unmatched, {len(overlaps)} overlaps")


def print_results() -> None:
    """Prints ALL_SYMBOLS summary to terminal."""
    all_ok = True
    for name, data in ALL_SYMBOLS.items():
        total = len(data["symbols"])
        matched = total - len(data["unmatched"])
        pct = 100 * matched / total if total else 0
        overlaps_n = len(data["overlaps"])
        unmatched_n = len(data["unmatched"])
        if unmatched_n or overlaps_n:
            all_ok = False

        print(f"\n{'='*70}")
        print(f"  {data['label']}  —  {data['var_name']}")
        print(f"{'='*70}")
        print(f"  Total: {total}  |  Matched: {matched} ({pct:.1f}%)  |  "
              f"Unmatched: {unmatched_n}  |  Overlaps: {overlaps_n}")

        col_w = max(18, max(len(g) for g in data["groups"]))
        for gname, syms in sorted(data["groups"].items(), key=lambda x: len(x[1]), reverse=True):
            bar_len = int(60 * len(syms) / total) if total else 0
            bar = "#" * bar_len
            print(f"    {gname:<{col_w}}  {len(syms):>5}  {bar}")

        if unmatched_n:
            print(f"\n  Unmatched ({unmatched_n}):")
            for sym in data["unmatched"]:
                print(f"    {sym}")

        if overlaps_n:
            print(f"\n  Overlaps ({overlaps_n}):")
            for sym, g1, g2 in data["overlaps"]:
                print(f"    {sym}  ->  {g1} + {g2}")

    print(f"\n{'='*70}")
    if all_ok:
        print("  ALL OK")
    else:
        print("  ISSUES FOUND")
    print(f"{'='*70}")


def print_full() -> None:
    """Prints ALL_SYMBOLS with all symbol names listed per group."""
    for name, data in ALL_SYMBOLS.items():
        print(f"\n{'='*70}")
        print(f"  {data['label']}  —  {data['var_name']}")
        print(f"{'='*70}")
        print(f"  Total: {len(data['symbols'])}  |  "
              f"Unmatched: {len(data['unmatched'])}  |  "
              f"Overlaps: {len(data['overlaps'])}")

        for gname, syms in data["groups"].items():
            print(f"\n  [{gname}]  ({len(syms)} symbols)")
            for sym in syms:
                print(f"    {sym}")

        if data["unmatched"]:
            print(f"\n  [UNMATCHED]  ({len(data['unmatched'])} symbols)")
            for sym in data["unmatched"]:
                print(f"    {sym}")

        if data["overlaps"]:
            print(f"\n  [OVERLAPS]  ({len(data['overlaps'])})")
            for sym, g1, g2 in data["overlaps"]:
                print(f"    {sym}  ->  {g1} + {g2}")


def print_group(group_name: str) -> None:
    """Prints only the specified group across selected scripts."""
    found = False
    for name, data in ALL_SYMBOLS.items():
        syms = data["groups"].get(group_name)
        if syms is None:
            continue
        if not found:
            print()
            found = True
        print(f"  {name:<22} {len(syms):>5} symbols")
        for sym in syms:
            print(f"    {sym}")
    if not found:
        print(f"  Group '{group_name}' not found in selected scripts")
        # Show available groups
        available = set()
        for data in ALL_SYMBOLS.values():
            available.update(data["groups"].keys())
        if available:
            print(f"  Available: {', '.join(sorted(available))}")


# =============================================================================
# CLI
# =============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Verify symbol group classification for all 4 Bybit downloaders.",
    )
    parser.add_argument("--trades", action="store_true",
                        help="futures_trades + spot_trades only")
    parser.add_argument("--orderbook", action="store_true",
                        help="futures_orderbook + spot_orderbook only")
    parser.add_argument("--full", action="store_true",
                        help="print all symbols per group")
    parser.add_argument("--group", metavar="NAME",
                        help="show only this group (e.g. USDT, USDC, STABLE, OTHER, CRYPTO, FIAT, LEVERAGED, UNSORTED)")
    args = parser.parse_args()

    if args.trades and args.orderbook:
        selected = list(SCRIPTS.keys())
    elif args.trades:
        selected = ["futures_trades", "spot_trades"]
    elif args.orderbook:
        selected = ["futures_orderbook", "spot_orderbook"]
    else:
        selected = list(SCRIPTS.keys())

    run(selected)

    if args.group:
        print_group(args.group)
    elif args.full:
        print_full()
    else:
        print_results()


if __name__ == "__main__":
    main()
