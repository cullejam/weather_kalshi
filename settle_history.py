import csv
import os
import requests

from config import USER_AGENT

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"


def warn(msg: str):
    print(f"[WARN] {msg}")


def http_get_json(url: str, params: dict | None = None):
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    }
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def load_history(path="history/market_history.csv"):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def save_history(rows, path="history/market_history.csv"):
    if not rows:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def get_market_by_ticker(ticker: str) -> dict:
    return http_get_json(f"{KALSHI_BASE}/markets/{ticker}")["market"]


def inspect_settled_market(ticker: str):
    market = get_market_by_ticker(ticker)
    print('ticker', ticker)
    print('keys', market.keys())
    print('payload', market)
    return market


def resolve_market_outcome(market: dict):
    status = market.get("status")
    result = market.get("result")
    expiration_value = market.get("expiration_value")

    if status not in ("settled", "expired"):
        return None

    # Conservative: fill only if we see explicit yes/no
    if result == "yes":
        return 1
    if result == "no":
        return 0

    if expiration_value is not None:
        try:
            value = float(expiration_value)
            return 1 if value >= 0.5 else 0
        except Exception:
            pass
    return None


def main():
    rows = load_history()
    updated = 0

    for row in rows:
        if row.get("actual_outcome_yes") not in ("", None):
            continue

        ticker = row.get("market_ticker")
        if not ticker:
            continue

        try:
            market = get_market_by_ticker(ticker)
        except Exception as e:
            warn(f"Failed {ticker}: {e}")
            continue

        outcome = resolve_market_outcome(market)
        if outcome is None:
            continue

        row["actual_outcome_yes"] = outcome
        row["settled_status"] = market.get("status")
        row["actual_expiration_value"] = market.get("expiration_value")

        best_side = row.get("best_side")
        if best_side == "YES":
            row["model_win"] = 1 if int(row["actual_outcome_yes"]) == 1 else 0
        elif best_side == "NO":
            row["model_win"] = 1 if int(row["actual_outcome_yes"]) == 0 else 0
        else:
            row["model_win"] = ""

        market_yes_mid = row.get("market_yes_mid")
        if market_yes_mid not in ("", None):
            p = float(market_yes_mid)
            if row["model_win"] == "":
                row["model_pnl_per_contract"] = ""
            elif row["model_win"] == 1:
                if best_side == "YES":
                    row["model_pnl_per_contract"] = round((1 - p), 4)
                else:
                    no_price = 1 - p
                    row["model_pnl_per_contract"] = round((1 - no_price), 4)
            else:
                if best_side == "YES":
                    row["model_pnl_per_contract"] = round(-p, 4)
                else:
                    no_price = 1 - p
                    row["model_pnl_per_contract"] = round(-no_price, 4)

        updated += 1

    save_history(rows)
    print(f"Updated {updated} rows")


if __name__ == "__main__":
    main()
