import json
from pprint import pprint

FILE = "output/nyc_markets_sample.json"  # change if needed

with open(FILE, "r", encoding="utf-8") as f:
    data = json.load(f)

markets = data["markets"]

for i, m in enumerate(markets, 1):
    print(f"\n--- MARKET {i} ---")
    keep = {
        "ticker": m.get("ticker"),
        "title": m.get("title"),
        "subtitle": m.get("subtitle"),
        "yes_bid_dollars": m.get("yes_bid_dollars"),
        "yes_ask_dollars": m.get("yes_ask_dollars"),
        "no_bid_dollars": m.get("no_bid_dollars"),
        "no_ask_dollars": m.get("no_ask_dollars"),
        "last_price_dollars": m.get("last_price_dollars"),
        "strike_type": m.get("strike_type"),
        "floor_strike": m.get("floor_strike"),
        "cap_strike": m.get("cap_strike"),
        "rules_primary": m.get("rules_primary"),
        "close_time": m.get("close_time"),
    }
    pprint(keep)