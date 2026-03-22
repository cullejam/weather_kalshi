import json
import requests

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
NWS_BASE = "https://api.weather.gov"
USER_AGENT = "JamesWeatherResearch/1.0 (your_email@example.com)"


def get_json(url, params=None, accept="application/json"):
    headers = {"User-Agent": USER_AGENT, "Accept": accept}
    r = requests.get(url, headers=headers, params=params, timeout=30)
    print(f"{url} -> {r.status_code}")
    r.raise_for_status()
    return r.json()


print("\n--- KALSHI TEST ---")
kalshi = get_json(
    f"{KALSHI_BASE}/markets",
    params={"series_ticker": "KXHIGHNY", "status": "open", "limit": 5},
)
print("keys:", kalshi.keys())
print("market count:", len(kalshi.get("markets", [])))
if kalshi.get("markets"):
    print("first market keys:", kalshi["markets"][0].keys())

print("\n--- NWS POINTS TEST ---")
points = get_json(
    f"{NWS_BASE}/points/40.7829,-73.9654",
    accept="application/geo+json",
)
print("points keys:", points.keys())
print("forecastHourly:", points["properties"].get("forecastHourly"))
print("observationStations:", points["properties"].get("observationStations"))

print("\n--- NWS HOURLY TEST ---")
hourly = get_json(points["properties"]["forecastHourly"], accept="application/geo+json")
periods = hourly.get("properties", {}).get("periods", [])
print("hourly periods:", len(periods))
if periods:
    print("first hourly period:")
    print(json.dumps(periods[0], indent=2)[:1200])