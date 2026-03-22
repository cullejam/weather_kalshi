import csv
import json
import os
import re
from datetime import datetime, UTC, date, timedelta
from statistics import NormalDist
from zoneinfo import ZoneInfo

import requests

from config import CITIES, USER_AGENT
from weather_sources import (
    aggregate_by_date,
    extract_nws_metric_values_by_date,
    fetch_open_meteo_archive_daily_fields,
    fetch_open_meteo_daily_fields,
    summarize_metric_sources,
)

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
NWS_BASE = "https://api.weather.gov"

DEBUG = True

TARGET_MODE = "today"   # options: "today", "tomorrow", "all"
USE_HISTORY_WEIGHTS = False
USE_HISTORY_CALIBRATION = False
STRATEGY_VERSION = "weather_v2.3.0"
MIN_HOURLY_PERIODS = 12
MAX_OBS_STALENESS_MINUTES = 180
EXTREME_DISAGREEMENT_F = 6.0

# Aggressiveness settings (medium)
MEDIUM_CONFIDENCE_CUTOFF = 0.05
MEDIUM_EDGE_CUTOFF = 0.07
MEDIUM_SPREAD_CUTOFF = 0.08

# Extra guardrails for monthly/multi-day cumulative precipitation/snow contracts.
CUMULATIVE_MIN_COVERAGE = 0.85
CUMULATIVE_MAX_SPREAD = 0.05
CUMULATIVE_MIN_EDGE = 0.10
CUMULATIVE_MIN_CONFIDENCE = 0.10
CUMULATIVE_MARKET_PROB_MIN = 0.05
CUMULATIVE_MARKET_PROB_MAX = 0.95
CUMULATIVE_FAIR_PROB_MIN = 0.05
CUMULATIVE_FAIR_PROB_MAX = 0.95

MONTH_MAP = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}

DEFAULT_SOURCE_WEIGHTS = (0.5, 0.3, 0.2)

def local_today(city_timezone: str) -> date:
    return datetime.now(ZoneInfo(city_timezone)).date()


def should_include_market(market_date: date, city_timezone: str) -> bool:
    today = local_today(city_timezone)

    if TARGET_MODE == "all":
        return True
    if TARGET_MODE == "today":
        return market_date == today
    if TARGET_MODE == "tomorrow":
        return market_date == today.fromordinal(today.toordinal() + 1)

    return True


def _month_end(d: date) -> date:
    if d.month == 12:
        return date(d.year, 12, 31)
    return date.fromordinal(date(d.year, d.month + 1, 1).toordinal() - 1)


def parse_market_period(market: dict) -> tuple[date | None, date | None]:
    """
    Parse market effective period from event_ticker/title.
    Supports:
      - daily: KXHIGHNY-26MAR22
      - monthly: KXRAINNYCM-26MAR
    """
    event_ticker = market.get("event_ticker") or ""

    m_daily = re.search(r"-(\d{2})([A-Z]{3})(\d{2})$", event_ticker)
    if m_daily:
        yy = int(m_daily.group(1))
        mon = MONTH_MAP[m_daily.group(2)]
        dd = int(m_daily.group(3))
        d = date(2000 + yy, mon, dd)
        return d, d

    m_monthly = re.search(r"-(\d{2})([A-Z]{3})$", event_ticker)
    if m_monthly:
        yy = int(m_monthly.group(1))
        mon = MONTH_MAP[m_monthly.group(2)]
        start = date(2000 + yy, mon, 1)
        end = _month_end(start)
        return start, end

    title = market.get("title") or ""
    m_title_daily = re.search(r"on\s+([A-Z][a-z]{2})\s+(\d{1,2}),\s+(\d{4})", title)
    if m_title_daily:
        mon = MONTH_MAP[m_title_daily.group(1).upper()]
        dd = int(m_title_daily.group(2))
        yy = int(m_title_daily.group(3))
        d = date(yy, mon, dd)
        return d, d

    m_title_month = re.search(r"in\s+([A-Z][a-z]{2})\s+(\d{4})", title)
    if m_title_month:
        mon = MONTH_MAP[m_title_month.group(1).upper()]
        yy = int(m_title_month.group(2))
        start = date(yy, mon, 1)
        end = _month_end(start)
        return start, end

    return None, None


def should_include_market_period(start: date | None, end: date | None, city_timezone: str) -> bool:
    if start is None or end is None:
        return False

    today = local_today(city_timezone)

    if TARGET_MODE == "all":
        return True
    if TARGET_MODE == "today":
        return start <= today <= end
    if TARGET_MODE == "tomorrow":
        tomorrow = today.fromordinal(today.toordinal() + 1)
        return start <= tomorrow <= end
    return True


def log(msg: str):
    if DEBUG:
        print(msg)


def warn(msg: str):
    print(f"[WARN] {msg}")


def http_get_json(url: str, params: dict | None = None, accept: str = "application/json"):
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": accept,
    }

    log(f"\nGET {url}")
    if params:
        log(f"params={params}")

    r = requests.get(url, headers=headers, params=params, timeout=30)
    log(f"status={r.status_code}")
    r.raise_for_status()
    return r.json()


def safe_float(x):
    if x is None or x == "":
        return None
    try:
        return float(x)
    except Exception:
        return None


def get_history_forecast_value(row: dict, source: str) -> float | None:
    source_to_fields = {
        "nws": ("forecast_nws_temp_f", "forecast_nws_max_f"),
        "alt": ("forecast_alt_temp_f", "forecast_alt_max_f"),
        "open_meteo": ("forecast_open_meteo_temp_f", "forecast_open_meteo_max_f"),
    }

    for field in source_to_fields.get(source, ()):
        value = safe_float(row.get(field))
        if value is not None:
            return value
    return None


def get_yes_bid_ask(market: dict):
    yes_bid = safe_float(market.get("yes_bid_dollars"))
    yes_ask = safe_float(market.get("yes_ask_dollars"))
    return yes_bid, yes_ask


def midpoint_yes_prob(market: dict) -> float | None:
    yes_bid = safe_float(market.get("yes_bid_dollars"))
    yes_ask = safe_float(market.get("yes_ask_dollars"))
    no_bid = safe_float(market.get("no_bid_dollars"))
    no_ask = safe_float(market.get("no_ask_dollars"))
    last_price = safe_float(market.get("last_price_dollars"))

    if yes_bid is not None and yes_ask is not None:
        return (yes_bid + yes_ask) / 2.0

    if last_price is not None:
        return last_price

    if no_bid is not None and no_ask is not None:
        return 1.0 - ((no_bid + no_ask) / 2.0)

    return None


def dynamic_sigma(base_sigma: float, hourly_temps: list[tuple[datetime, float]]) -> float:
    if not hourly_temps:
        return base_sigma

    temps = [t for _, t in hourly_temps]
    temp_range = max(temps) - min(temps)

    sigma = base_sigma

    # Bigger daily swings usually mean more room for forecast error
    if temp_range >= 20:
        sigma += 0.4
    elif temp_range <= 10:
        sigma -= 0.2

    return max(1.5, sigma)


def load_history_rows(path="history/market_history.csv") -> list[dict]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def compute_calibration_from_history(path="history/market_history.csv", bins:int=10, market_type: str | None = None) -> dict:
    rows = load_history_rows(path)
    points = []

    for row in rows:
        # If market_type filter specified, skip non-matching rows
        if market_type and row.get("market_type") != market_type:
            continue
        
        actual = row.get("actual_outcome_yes")
        if actual in (None, "", "None"):
            continue

        fair_yes = safe_float(row.get("fair_yes"))
        if fair_yes is None:
            continue

        actual_val = safe_float(actual)
        if actual_val is None:
            continue

        actual_bool = 1.0 if actual_val >= 0.5 else 0.0
        points.append((fair_yes, actual_bool))

    if len(points) < 20:
        return {"bins": bins, "bin_stats": [], "count": len(points)}

    points.sort(key=lambda p: p[0])
    bin_stats = [{"count": 0, "sum_actual": 0.0, "sum_pred": 0.0} for _ in range(bins)]

    for pred, actual_val in points:
        index = min(int(pred * bins), bins - 1)
        bin_stats[index]["count"] += 1
        bin_stats[index]["sum_actual"] += actual_val
        bin_stats[index]["sum_pred"] += pred

    calibration = {"bins": bins, "bin_stats": [], "count": len(points)}
    for i in range(bins):
        info = bin_stats[i]
        low = i / bins
        high = (i + 1) / bins
        if info["count"] > 0:
            calibrate = info["sum_actual"] / info["count"]
            avg_pred = info["sum_pred"] / info["count"]
        else:
            calibrate = None
            avg_pred = None

        calibration["bin_stats"].append({
            "bin": i,
            "p_lo": low,
            "p_hi": high,
            "count": info["count"],
            "avg_pred": avg_pred,
            "observed": info["sum_actual"] if info["count"] > 0 else None,
            "calibrated": calibrate,
        })

    # Use market-type-specific calibration file if type specified
    cal_filename = "history/calibration.json"
    if market_type:
        cal_filename = f"history/calibration_{market_type}.json"
    
    with open(cal_filename, "w", encoding="utf-8") as f:
        json.dump(calibration, f, indent=2)

    return calibration


def apply_probability_calibration(p: float, calibration: dict | None) -> float:
    if calibration is None or not calibration.get("bin_stats"):
        return max(0.0, min(1.0, p))

    bins = calibration.get("bins", 10)
    index = min(int(p * bins), bins - 1)
    binfo = calibration["bin_stats"][index]

    if binfo.get("calibrated") is None or binfo.get("count",0) < 10:
        return max(0.0, min(1.0, p))

    return max(0.0, min(1.0, binfo["calibrated"]))


def get_open_markets_for_series(series_ticker: str) -> list[dict]:
    url = f"{KALSHI_BASE}/markets"
    cursor = None
    all_markets = []

    while True:
        params = {
            "series_ticker": series_ticker,
            "status": "open",
            "limit": 1000,
        }
        if cursor:
            params["cursor"] = cursor

        data = http_get_json(url, params=params)
        markets = data.get("markets", [])
        all_markets.extend(markets)
        cursor = data.get("cursor")

        log(f"received {len(markets)} markets; next cursor={cursor}")

        if not cursor:
            break

    return all_markets


def get_points_metadata(lat: float, lon: float) -> dict:
    return http_get_json(f"{NWS_BASE}/points/{lat},{lon}", accept="application/geo+json")


def get_hourly_forecast_from_points(points_data: dict) -> dict:
    hourly_url = points_data["properties"]["forecastHourly"]
    return http_get_json(hourly_url, accept="application/geo+json")


def get_daily_forecast_from_points(points_data: dict) -> dict | None:
    daily_url = points_data["properties"].get("forecast")
    if not daily_url:
        return None
    try:
        return http_get_json(daily_url, accept="application/geo+json")
    except Exception as e:
        log(f"Daily forecast fetch failed: {e}")
        return None


def get_daily_highs_by_date(daily_data: dict) -> dict:
    by_date: dict[date, list[tuple[datetime, float]]] = {}
    if not daily_data:
        return by_date

    periods = daily_data.get("properties", {}).get("periods", [])
    for p in periods:
        start = p.get("startTime")
        if not start:
            continue
        dt = datetime.fromisoformat(start)
        temp = p.get("temperature")
        unit = p.get("temperatureUnit")
        if temp is None or unit is None:
            continue
        if unit == "C":
            temp = c_to_f(float(temp))
        else:
            temp = float(temp)

        d = dt.date()
        by_date.setdefault(d, []).append((dt, temp))

    return by_date


def get_daily_lows_by_date(daily_data: dict) -> dict:
    by_date: dict[date, list[tuple[datetime, float]]] = {}
    if not daily_data:
        return by_date

    periods = daily_data.get("properties", {}).get("periods", [])
    for p in periods:
        start = p.get("startTime")
        if not start:
            continue
        dt = datetime.fromisoformat(start)
        # Look for temperatureLow or temperature field (varies by period in NWS forecast)
        temp = p.get("temperatureLow")
        if temp is None:
            temp = p.get("temperature")
        if temp is None:
            continue
        unit = p.get("temperatureUnit")
        if unit is None:
            continue
        if unit == "C":
            temp = c_to_f(float(temp))
        else:
            temp = float(temp)

        d = dt.date()
        by_date.setdefault(d, []).append((dt, temp))

    return by_date


def get_open_meteo_daily_max(lat: float, lon: float, start_date: date, end_date: date, timezone: str = "UTC") -> dict:
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "temperature_2m_max",
        "timezone": timezone,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }

    try:
        data = http_get_json(url, params=params, accept="application/json")
    except Exception as e:
        warn(f"Open-Meteo fetch failed for {lat},{lon}: {e}")
        return {}

    by_date: dict[date, float] = {}
    daily = data.get("daily", {})
    times = daily.get("time", [])
    max_temps = daily.get("temperature_2m_max", [])

    for dt_str, tmp in zip(times, max_temps):
        try:
            d = date.fromisoformat(dt_str)
            if tmp is None:
                by_date[d] = None
            else:
                # Open-Meteo daily max is returned in Â°C by default; normalize to Â°F
                by_date[d] = c_to_f(float(tmp))
        except Exception:
            continue

    return by_date


def get_open_meteo_daily_min(lat: float, lon: float, start_date: date, end_date: date, timezone: str = "UTC") -> dict:
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "temperature_2m_min",
        "timezone": timezone,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }

    try:
        data = http_get_json(url, params=params, accept="application/json")
    except Exception as e:
        warn(f"Open-Meteo fetch failed for {lat},{lon}: {e}")
        return {}

    by_date: dict[date, float] = {}
    daily = data.get("daily", {})
    times = daily.get("time", [])
    min_temps = daily.get("temperature_2m_min", [])

    for dt_str, tmp in zip(times, min_temps):
        try:
            d = date.fromisoformat(dt_str)
            if tmp is None:
                by_date[d] = None
            else:
                # Open-Meteo daily min is returned in Â°C by default; normalize to Â°F
                by_date[d] = c_to_f(float(tmp))
        except Exception:
            continue

    return by_date


def get_metric_inputs_from_free_sources(
    hourly_data: dict,
    daily_data: dict | None,
    tz_name: str,
    lat: float,
    lon: float,
    start_date: date,
    end_date: date,
    metric_key: str,
) -> tuple[dict, dict, dict, str]:
    """
    Returns:
      primary_by_date, alt_by_date, open_meteo_by_date, unit
    """
    # Keep return contract simple for process_markets_for_type.
    if metric_key == "temp_high_f":
        temps_by_date = get_hourly_temps_by_date(hourly_data, tz_name)
        primary_by_date = {d: estimate_daily_max_temp_f(vals) for d, vals in temps_by_date.items()}
        daily_highs_by_date = get_daily_highs_by_date(daily_data) if daily_data else {}
        alt_by_date = {d: estimate_daily_max_temp_f(vals) for d, vals in daily_highs_by_date.items()}
        open_meteo = get_open_meteo_daily_max(lat, lon, start_date, end_date, tz_name)
        return primary_by_date, alt_by_date, open_meteo, "F"

    if metric_key == "temp_low_f":
        temps_by_date = get_hourly_temps_by_date(hourly_data, tz_name)
        primary_by_date = {d: estimate_daily_min_temp_f(vals) for d, vals in temps_by_date.items()}
        daily_lows_by_date = get_daily_lows_by_date(daily_data) if daily_data else {}
        alt_by_date = {d: estimate_daily_min_temp_f(vals) for d, vals in daily_lows_by_date.items()}
        open_meteo = get_open_meteo_daily_min(lat, lon, start_date, end_date, tz_name)
        return primary_by_date, alt_by_date, open_meteo, "F"

    if metric_key == "rain_total_in":
        nws_vals = extract_nws_metric_values_by_date(hourly_data, tz_name, "precip_in")
        primary_by_date = aggregate_by_date(nws_vals, "sum")
        alt_by_date = {}
        open_fields = fetch_open_meteo_daily_fields(
            http_get_json, lat, lon, start_date, end_date, tz_name, ["precipitation_sum"]
        )
        open_meteo = open_fields.get("precipitation_sum", {})
        return primary_by_date, alt_by_date, open_meteo, "in"

    if metric_key == "snow_total_in":
        nws_vals = extract_nws_metric_values_by_date(hourly_data, tz_name, "snow_in")
        primary_by_date = aggregate_by_date(nws_vals, "sum")
        alt_by_date = {}
        open_fields = fetch_open_meteo_daily_fields(
            http_get_json, lat, lon, start_date, end_date, tz_name, ["snowfall_sum"]
        )
        open_meteo = open_fields.get("snowfall_sum", {})
        return primary_by_date, alt_by_date, open_meteo, "in"

    if metric_key == "wind_gust_mph":
        gust_vals = extract_nws_metric_values_by_date(hourly_data, tz_name, "wind_gust_mph")
        speed_vals = extract_nws_metric_values_by_date(hourly_data, tz_name, "wind_speed_mph")
        primary_by_date = aggregate_by_date(gust_vals, "max")
        alt_by_date = aggregate_by_date(speed_vals, "max")
        open_fields = fetch_open_meteo_daily_fields(
            http_get_json, lat, lon, start_date, end_date, tz_name, ["wind_gusts_10m_max"]
        )
        open_meteo = open_fields.get("wind_gusts_10m_max", {})
        return primary_by_date, alt_by_date, open_meteo, "mph"

    raise ValueError(f"Unsupported metric_key={metric_key}")


def compute_source_weights_from_history(
    path="history/market_history.csv",
    market_type: str | None = None,
) -> tuple[float, float, float]:
    if not os.path.exists(path):
        return DEFAULT_SOURCE_WEIGHTS

    with open(path, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    nws_errors = []
    alt_errors = []
    open_errors = []

    for row in rows:
        if market_type and row.get("market_type") != market_type:
            continue

        actual = row.get("actual_outcome_yes")
        if actual in (None, "", "None"):
            continue

        try:
            actual = float(actual)
            actual = 1.0 if actual >= 0.5 else 0.0
        except Exception:
            continue

        strike_type = row.get("strike_type")
        floor_strike = safe_float(row.get("floor_strike"))
        cap_strike = safe_float(row.get("cap_strike"))

        mkt = {
            "strike_type": strike_type,
            "floor_strike": floor_strike,
            "cap_strike": cap_strike,
            "title": row.get("title", ""),
        }

        sigma = safe_float(row.get("sigma_f"))
        if sigma is None or sigma <= 0:
            sigma = 2.5

        for source, err_list in [("nws", nws_errors), ("alt", alt_errors), ("open_meteo", open_errors)]:
            fval = get_history_forecast_value(row, source)
            if fval is None:
                continue

            p = parse_market_probability(mkt, fval, sigma)
            if p is None:
                continue

            err_list.append((p - actual) ** 2)

    def score(errors):
        if not errors:
            return None
        mse = sum(errors) / len(errors)
        return 1.0 / (mse + 1e-6)

    nws_score = score(nws_errors)
    alt_score = score(alt_errors)
    open_score = score(open_errors)

    if nws_score is None and alt_score is None and open_score is None:
        return DEFAULT_SOURCE_WEIGHTS

    nws_score = nws_score or 0.0
    alt_score = alt_score or 0.0
    open_score = open_score or 0.0
    total_score = nws_score + alt_score + open_score

    if total_score <= 0:
        return DEFAULT_SOURCE_WEIGHTS

    return nws_score / total_score, alt_score / total_score, open_score / total_score


def get_source_weights(market_type: str) -> tuple[float, float, float]:
    if not USE_HISTORY_WEIGHTS:
        return DEFAULT_SOURCE_WEIGHTS
    return compute_source_weights_from_history(market_type=market_type)


def get_calibration_data(market_type: str) -> dict:
    if not USE_HISTORY_CALIBRATION:
        return {"bins": 10, "bin_stats": [], "count": 0}
    return compute_calibration_from_history(market_type=market_type)


def local_day_bounds_utc(target_date: date, tz_name: str) -> tuple[datetime, datetime]:
    tz = ZoneInfo(tz_name)
    start_local = datetime(target_date.year, target_date.month, target_date.day, tzinfo=tz)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(UTC), end_local.astimezone(UTC)


def get_station_observations(station_url: str, start_utc: datetime, end_utc: datetime) -> list[dict]:
    obs_url = f"{station_url}/observations"
    params = {
        "start": start_utc.isoformat(),
        "end": end_utc.isoformat(),
        "limit": 500,
    }
    try:
        data = http_get_json(obs_url, params=params, accept="application/geo+json")
    except Exception as e:
        log(f"station observations fetch failed for {station_url}: {e}")
        return []
    return data.get("features", [])


def get_observed_extrema_by_date(observations: list[dict], tz_name: str) -> dict[date, dict[str, float]]:
    tz = ZoneInfo(tz_name)
    by_date: dict[date, dict[str, float]] = {}

    for obs in observations:
        props = obs.get("properties", {})
        ts = props.get("timestamp")
        temp_c = props.get("temperature", {}).get("value")
        if ts is None or temp_c is None:
            continue

        try:
            dt_local = datetime.fromisoformat(ts).astimezone(tz)
            temp_f = c_to_f(float(temp_c))
        except Exception:
            continue

        d = dt_local.date()
        if d not in by_date:
            by_date[d] = {"max_so_far_f": temp_f, "min_so_far_f": temp_f}
            continue

        by_date[d]["max_so_far_f"] = max(by_date[d]["max_so_far_f"], temp_f)
        by_date[d]["min_so_far_f"] = min(by_date[d]["min_so_far_f"], temp_f)

    return by_date


def process_markets_for_type(
    city,
    series_ticker: str,
    market_type: str,
    metric_key: str,
    metric_unit: str,
    forecast_temps_by_date: dict,
    other_source_temps_by_date: dict,
    open_meteo_temps_by_date: dict,
    temps_by_date_for_sigma: dict,
    nws_weight: float,
    alt_weight: float,
    open_meteo_weight: float,
    calibration_data: dict,
    obs_temp_f: float | None,
    obs_summary: str,
    observed_extrema_by_date: dict[date, dict[str, float]] | None = None,
    observed_primary_by_date: dict[date, float] | None = None,
    observed_alt_by_date: dict[date, float] | None = None,
    observed_open_by_date: dict[date, float] | None = None,
    base_sigma: float | None = None,
    use_dynamic_sigma: bool = True,
) -> tuple[list[dict], int, int, list[str]]:
    """
    Process markets for a given type (high/low) and return ranked market rows.
    Returns: (rows, total_scanned, total_passed, skip_reasons)
    market_type is agnostic - works with high temps, low temps, or any other forecast type.
    forecast_temps_by_date, other_source_temps_by_date, open_meteo_temps_by_date are dicts mapping date -> temp value
    """
    markets = get_open_markets_for_series(series_ticker)
    log(f"open markets for {series_ticker}: {len(markets)}")

    if markets:
        dump_debug_json(
            f"{city.name.lower().replace(' ', '_')}_markets_sample_{market_type}.json",
            {
                "sample_count": min(5, len(markets)),
                "markets": markets[:5],
            },
        )

    rows = []
    skip_reasons = []
    total_markets_scanned = 0
    total_markets_passed = 0
    today_local = local_today(city.timezone)
    cumulative_metric = metric_key in ("rain_total_in", "snow_total_in")

    def period_value_and_coverage(
        values_by_date: dict[date, float],
        start: date,
        end: date,
        key: str,
    ) -> tuple[float | None, float]:
        if start > end:
            return None, 0.0

        days_total = (end.toordinal() - start.toordinal()) + 1
        if days_total <= 1:
            v = values_by_date.get(start)
            return (float(v), 1.0) if v is not None else (None, 0.0)

        vals: list[float] = []
        covered_days = 0
        for ord_day in range(start.toordinal(), end.toordinal() + 1):
            d = date.fromordinal(ord_day)
            v = values_by_date.get(d)
            if v is None:
                continue
            covered_days += 1
            vals.append(float(v))

        coverage = covered_days / days_total
        if not vals:
            return None, coverage

        if key in ("rain_total_in", "snow_total_in"):
            return sum(vals), coverage
        if key == "wind_gust_mph":
            return max(vals), coverage
        if key == "temp_high_f":
            return max(vals), coverage
        if key == "temp_low_f":
            return min(vals), coverage
        return vals[-1], coverage

    def period_sum_and_coverage(
        values_by_date: dict[date, float] | None,
        start: date,
        end: date,
    ) -> tuple[float, float, int]:
        if values_by_date is None or start > end:
            return 0.0, 1.0, 0
        total_days = (end.toordinal() - start.toordinal()) + 1
        if total_days <= 0:
            return 0.0, 1.0, 0
        vals = []
        covered = 0
        for ord_day in range(start.toordinal(), end.toordinal() + 1):
            d = date.fromordinal(ord_day)
            v = values_by_date.get(d)
            if v is None:
                continue
            covered += 1
            vals.append(float(v))
        return sum(vals), (covered / total_days), total_days

    for mkt in markets:
        total_markets_scanned += 1
        try:
            market_start, market_end = parse_market_period(mkt)
            market_date = market_start
            if market_start is None or market_end is None or market_date is None:
                reason = "Could not parse market period"
                log(f"[SKIP] {reason} for {mkt.get('ticker')}")
                skip_reasons.append(f"{mkt.get('ticker')}: {reason}")
                continue

            if not should_include_market_period(market_start, market_end, city.timezone):
                skip_reasons.append(f"{mkt.get('ticker')} ignored by mode")
                continue

            period_days = (market_end.toordinal() - market_start.toordinal()) + 1
            is_multi_day_period = period_days > 1
            has_pre_period = cumulative_metric and market_start < today_local and market_end >= today_local
            forecast_start = today_local if has_pre_period else market_start

            mean_temp_nws, nws_forecast_coverage = period_value_and_coverage(
                forecast_temps_by_date, forecast_start, market_end, metric_key
            )
            mean_temp_alt, alt_forecast_coverage = period_value_and_coverage(
                other_source_temps_by_date, forecast_start, market_end, metric_key
            )
            mean_temp_open, open_forecast_coverage = period_value_and_coverage(
                open_meteo_temps_by_date, forecast_start, market_end, metric_key
            )

            nws_observed, nws_observed_coverage, observed_days = (0.0, 1.0, 0)
            alt_observed, alt_observed_coverage, _ = (0.0, 1.0, 0)
            open_observed, open_observed_coverage, _ = (0.0, 1.0, 0)
            if has_pre_period:
                observed_end = today_local.fromordinal(today_local.toordinal() - 1)
                nws_observed, nws_observed_coverage, observed_days = period_sum_and_coverage(
                    observed_primary_by_date, market_start, observed_end
                )
                alt_observed, alt_observed_coverage, _ = period_sum_and_coverage(
                    observed_alt_by_date, market_start, observed_end
                )
                open_observed, open_observed_coverage, _ = period_sum_and_coverage(
                    observed_open_by_date, market_start, observed_end
                )
                if observed_days > 0 and max(nws_observed_coverage, open_observed_coverage, alt_observed_coverage) < 0.80:
                    skip_reasons.append(
                        f"{mkt.get('ticker')}: insufficient observed coverage for elapsed period "
                        f"({max(nws_observed_coverage, open_observed_coverage, alt_observed_coverage):.2f})"
                    )
                    continue

            forecast_days = (market_end.toordinal() - forecast_start.toordinal()) + 1
            forecast_days = max(0, forecast_days)

            if cumulative_metric and has_pre_period:
                if mean_temp_nws is not None:
                    mean_temp_nws += nws_observed
                if mean_temp_alt is not None:
                    mean_temp_alt += alt_observed
                if mean_temp_open is not None:
                    mean_temp_open += open_observed

            if period_days > 0:
                nws_coverage = ((nws_forecast_coverage * forecast_days) + (nws_observed_coverage * observed_days)) / period_days
                alt_coverage = ((alt_forecast_coverage * forecast_days) + (alt_observed_coverage * observed_days)) / period_days
                open_coverage = ((open_forecast_coverage * forecast_days) + (open_observed_coverage * observed_days)) / period_days
            else:
                nws_coverage = nws_forecast_coverage
                alt_coverage = alt_forecast_coverage
                open_coverage = open_forecast_coverage

            coverage_best = max(nws_coverage, alt_coverage, open_coverage)

            if is_multi_day_period and coverage_best < 0.60:
                skip_reasons.append(
                    f"{mkt.get('ticker')}: insufficient forecast coverage for period "
                    f"({coverage_best:.2f})"
                )
                continue
            if cumulative_metric and is_multi_day_period and coverage_best < CUMULATIVE_MIN_COVERAGE:
                skip_reasons.append(
                    f"{mkt.get('ticker')}: insufficient cumulative coverage "
                    f"({coverage_best:.2f} < {CUMULATIVE_MIN_COVERAGE:.2f})"
                )
                continue

            if mean_temp_nws is None and mean_temp_alt is None and mean_temp_open is None:
                reason = "No forecast temps from any source"
                log(f"[SKIP] {reason} for {mkt.get('ticker')}")
                skip_reasons.append(f"{mkt.get('ticker')}: {reason}")
                continue

            source_vals = []
            combined_weight = 0.0

            if mean_temp_nws is not None and nws_weight > 0:
                source_vals.append((mean_temp_nws, nws_weight))
                combined_weight += nws_weight
            if mean_temp_alt is not None and alt_weight > 0:
                source_vals.append((mean_temp_alt, alt_weight))
                combined_weight += alt_weight
            if mean_temp_open is not None and open_meteo_weight > 0:
                source_vals.append((mean_temp_open, open_meteo_weight))
                combined_weight += open_meteo_weight

            if combined_weight > 0:
                mean_temp = sum(v * w for v, w in source_vals) / combined_weight
            else:
                fallback_vals = [x for x in (mean_temp_nws, mean_temp_alt, mean_temp_open) if x is not None]
                mean_temp = sum(fallback_vals) / len(fallback_vals)

            if observed_extrema_by_date:
                obs_extrema = observed_extrema_by_date.get(market_date)
                if obs_extrema:
                    if market_type == "high" and obs_extrema.get("max_so_far_f") is not None:
                        mean_temp = max(mean_temp, obs_extrema["max_so_far_f"])
                    elif market_type == "low" and obs_extrema.get("min_so_far_f") is not None:
                        mean_temp = min(mean_temp, obs_extrema["min_so_far_f"])

            open_disc = None
            if mean_temp_nws is not None and mean_temp_open is not None:
                open_disc = abs(mean_temp_nws - mean_temp_open)

            alt_disc = None
            if mean_temp_nws is not None and mean_temp_alt is not None:
                alt_disc = abs(mean_temp_nws - mean_temp_alt)

            forecast_disc = open_disc if open_disc is not None else (alt_disc or 0.0)

            log(f"open_meteo={mean_temp_open} | nws={mean_temp_nws} | alt={mean_temp_alt} | open_disc={open_disc} | alt_disc={alt_disc}")

            # Get hourly temps for this date (for sigma calculation)
            hourly_temps = temps_by_date_for_sigma.get(market_date, [])
            sigma_base = city.sigma_f if base_sigma is None else base_sigma
            sigma_f = dynamic_sigma(sigma_base, hourly_temps) if use_dynamic_sigma else sigma_base
            temp_metric = metric_key in ("temp_high_f", "temp_low_f")

            # city-specific tuning
            if use_dynamic_sigma and city.name == "NYC":
                sigma_f *= 1.0
            elif use_dynamic_sigma and city.name == "Los Angeles":
                sigma_f *= 1.05

            # disagreement-based uncertainty
            if temp_metric:
                if open_disc is not None:
                    if open_disc >= 5:
                        sigma_f += 0.9
                    elif open_disc >= 3:
                        sigma_f += 0.6
                    elif open_disc >= 2:
                        sigma_f += 0.4
                    elif open_disc >= 1:
                        sigma_f += 0.2
                elif forecast_disc >= 4:
                    sigma_f += 0.5
                elif forecast_disc >= 2:
                    sigma_f += 0.2
            else:
                if open_disc is not None:
                    sigma_f += min(open_disc, 1.0) * 0.25

            same_day = market_date == today_local
            if temp_metric and same_day:
                obs_adj = 0.0
                if obs_temp_f is not None and mean_temp is not None:
                    obs_diff = abs(obs_temp_f - mean_temp)
                    if obs_diff <= 2:
                        obs_adj = -0.2
                    elif obs_diff >= 5:
                        obs_adj = 0.3
                sigma_f = max(1.0, sigma_f + obs_adj)
            elif temp_metric:
                sigma_f = max(1.5, sigma_f + 0.2)
            else:
                sigma_f = max(0.05, sigma_f)

            # Multi-day periods carry extra uncertainty when only part of the period is forecast-covered.
            if is_multi_day_period and coverage_best < 1.0:
                sigma_f += (1.0 - coverage_best) * 1.25

            market_yes = midpoint_yes_prob(mkt)
            fair_yes = parse_market_probability(mkt, mean_temp, sigma_f)
            fair_yes_calibrated = apply_probability_calibration(fair_yes, calibration_data) if fair_yes is not None else None
            yes_bid, yes_ask = get_yes_bid_ask(mkt)

            log(
                f"[AUDIT] {mkt.get('ticker')} | date={market_date} | {mkt.get('title')} | "
                f"strike_type={mkt.get('strike_type')} floor={mkt.get('floor_strike')} "
                f"cap={mkt.get('cap_strike')} | forecast_temp={mean_temp} | "
                f"market_yes={market_yes} fair_yes={fair_yes} fair_yes_cal={fair_yes_calibrated} yes_bid={yes_bid} yes_ask={yes_ask}"
            )

            if market_yes is None or fair_yes is None:
                continue

            if yes_bid is None or yes_ask is None:
                continue

            spread = yes_ask - yes_bid
            if spread > 0.10:
                continue
            if cumulative_metric and is_multi_day_period and spread > CUMULATIVE_MAX_SPREAD:
                skip_reasons.append(
                    f"{mkt.get('ticker')}: spread too wide for cumulative market ({spread:.3f})"
                )
                continue

            if market_yes < 0.03 or market_yes > 0.97:
                continue

            fair_ref = fair_yes_calibrated if fair_yes_calibrated is not None else fair_yes
            if (
                cumulative_metric
                and is_multi_day_period
                and (fair_ref < CUMULATIVE_FAIR_PROB_MIN or fair_ref > CUMULATIVE_FAIR_PROB_MAX)
            ):
                skip_reasons.append(
                    f"{mkt.get('ticker')}: fair probability outside cumulative band ({fair_ref:.3f})"
                )
                continue
            if (
                cumulative_metric
                and is_multi_day_period
                and (market_yes < CUMULATIVE_MARKET_PROB_MIN or market_yes > CUMULATIVE_MARKET_PROB_MAX)
            ):
                skip_reasons.append(
                    f"{mkt.get('ticker')}: market probability outside cumulative band ({market_yes:.3f})"
                )
                continue

            edge_yes = fair_ref - market_yes
            edge_no = market_yes - fair_ref

            best_side = "YES" if edge_yes >= edge_no else "NO"
            best_edge = max(edge_yes, edge_no)

            effective_edge = best_edge - spread / 2.0
            if effective_edge < 0.05:
                continue
            if cumulative_metric and is_multi_day_period and effective_edge < CUMULATIVE_MIN_EDGE:
                skip_reasons.append(
                    f"{mkt.get('ticker')}: effective_edge too low for cumulative market ({effective_edge:.3f})"
                )
                continue

            # market quality filters
            yes_bid_size = safe_float(mkt.get("yes_bid_size"))
            yes_ask_size = safe_float(mkt.get("yes_ask_size"))
            volume = safe_float(mkt.get("volume"))
            open_interest = safe_float(mkt.get("open_interest"))

            if yes_bid_size is not None and yes_bid_size < 20:
                skip_reasons.append(f"{mkt.get('ticker')}: yes_bid_size too low ({yes_bid_size})")
                continue
            if yes_ask_size is not None and yes_ask_size < 20:
                skip_reasons.append(f"{mkt.get('ticker')}: yes_ask_size too low ({yes_ask_size})")
                continue
            if volume is not None and volume < 50:
                skip_reasons.append(f"{mkt.get('ticker')}: volume too low ({volume})")
                continue
            if open_interest is not None and open_interest < 20:
                skip_reasons.append(f"{mkt.get('ticker')}: open_interest too low ({open_interest})")
                continue

            spread_penalty = spread * 2
            disagreement_penalty = min(forecast_disc / 10, 0.2)
            daytype_penalty = 0.0 if same_day else 0.05
            disagreement_penalty = min((open_disc if open_disc is not None else forecast_disc) / 10, 0.3)
            confidence_score = max(0.0, effective_edge - spread_penalty - disagreement_penalty - daytype_penalty)

            no_trade_flag = (
                spread > MEDIUM_SPREAD_CUTOFF
                or effective_edge < (MEDIUM_EDGE_CUTOFF - 0.01)
                or (open_disc if open_disc is not None else forecast_disc) > 6
                or confidence_score < MEDIUM_CONFIDENCE_CUTOFF
            )
            if cumulative_metric and is_multi_day_period:
                no_trade_flag = (
                    no_trade_flag
                    or spread > CUMULATIVE_MAX_SPREAD
                    or effective_edge < CUMULATIVE_MIN_EDGE
                    or confidence_score < CUMULATIVE_MIN_CONFIDENCE
                )

            rows.append(
                {
                    "city": city.name,
                    "series_ticker": series_ticker,
                    "market_type": market_type,
                    "forecast_metric_key": metric_key,
                    "forecast_unit": metric_unit,
                    "event_ticker": mkt.get("event_ticker"),
                    "market_ticker": mkt.get("ticker"),
                    "forecast_date": market_date.isoformat(),
                    "market_period_start": market_start.isoformat(),
                    "market_period_end": market_end.isoformat(),
                    "forecast_period_days": period_days,
                    "forecast_coverage_best": round(coverage_best, 4),
                    "forecast_coverage_nws": round(nws_coverage, 4),
                    "forecast_coverage_alt": round(alt_coverage, 4),
                    "forecast_coverage_open_meteo": round(open_coverage, 4),
                    "title": mkt.get("title"),
                    "close_time": mkt.get("close_time"),
                    "forecast_daily_temp_f": round(mean_temp, 2),
                    "forecast_nws_temp_f": round(mean_temp_nws, 2) if mean_temp_nws is not None else None,
                    "forecast_alt_temp_f": round(mean_temp_alt, 2) if mean_temp_alt is not None else None,
                    "forecast_open_meteo_temp_f": round(mean_temp_open, 2) if mean_temp_open is not None else None,
                    "forecast_weight_nws": round(nws_weight, 4),
                    "forecast_weight_alt": round(alt_weight, 4),
                    "forecast_weight_open_meteo": round(open_meteo_weight, 4),
                    "forecast_disagreement_open_nws": round(open_disc, 3) if open_disc is not None else None,
                    "forecast_disagreement_nws_alt": round(alt_disc, 3) if alt_disc is not None else None,
                    "forecast_disagreement": round(forecast_disc, 3),
                    "sigma_f": round(sigma_f, 4),
                    "market_yes_mid": round(market_yes, 4),
                    "fair_yes": round(fair_yes, 4),
                    "fair_yes_calibrated": round(fair_yes_calibrated, 4) if fair_yes_calibrated is not None else None,
                    "fair_no": round(1.0 - fair_yes, 4),
                    "edge_yes": round(edge_yes, 4),
                    "edge_no": round(edge_no, 4),
                    "best_side": best_side,
                    "best_edge": round(best_edge, 4),
                    "effective_edge": round(effective_edge, 4),
                    "spread": round(spread, 4),
                    "confidence_score": round(confidence_score, 4),
                    "same_day": same_day,
                    "no_trade_flag": no_trade_flag,
                    "run_hour": datetime.now(UTC).hour,
                    "yes_bid": round(yes_bid, 4),
                    "yes_ask": round(yes_ask, 4),
                    "run_ts_utc": datetime.now(UTC).isoformat(),
                    "strategy_version": STRATEGY_VERSION,
                    "obs_temp_f": round(obs_temp_f, 2) if obs_temp_f is not None else None,
                    "actual_high_temp_f": None,
                    "actual_low_temp_f": None,
                    "actual_outcome_yes": None,
                    "obs_context": obs_summary,
                    "rules_primary": mkt.get("rules_primary", ""),
                    "strike_type": mkt.get("strike_type"),
                    "floor_strike": mkt.get("floor_strike"),
                    "cap_strike": mkt.get("cap_strike"),
                    "forecast_nws_max_f": round(mean_temp_nws, 2) if (market_type == "high" and mean_temp_nws is not None) else None,
                    "forecast_alt_max_f": round(mean_temp_alt, 2) if (market_type == "high" and mean_temp_alt is not None) else None,
                    "forecast_open_meteo_max_f": round(mean_temp_open, 2) if (market_type == "high" and mean_temp_open is not None) else None,
                }
            )
            total_markets_passed += 1

        except Exception as e:
            import traceback
            warn(f"{city.name} market {mkt.get('ticker')} failed: {e}")
            log(f"[EXCEPTION] Traceback: {traceback.format_exc()}")
            continue

    rows.sort(key=lambda x: x["effective_edge"], reverse=True)

    if skip_reasons:
        print("\nSkipped markets summary (top 20):")
        for s in skip_reasons[:20]:
            print("-", s)

    return rows, total_markets_scanned, total_markets_passed, skip_reasons


def get_observation_stations(points_data: dict) -> list[dict]:
    stations_url = points_data["properties"].get("observationStations")
    if not stations_url:
        return []

    data = http_get_json(stations_url, accept="application/geo+json")
    return data.get("features", [])


def get_latest_obs_from_station_url(station_url: str) -> dict | None:
    latest_obs_url = f"{station_url}/observations/latest"
    try:
        return http_get_json(latest_obs_url, accept="application/geo+json")
    except Exception as e:
        log(f"latest obs fetch failed for {station_url}: {e}")
        return None


def c_to_f(c):
    if c is None:
        return None
    return (c * 9.0 / 5.0) + 32.0


def parse_hour_temp_f(period: dict) -> float | None:
    temp = period.get("temperature")
    unit = period.get("temperatureUnit")

    if temp is None:
        return None

    if unit == "F":
        return float(temp)

    if unit == "C":
        return c_to_f(float(temp))

    return None


def get_hourly_temps_by_date(hourly_data: dict, tz_name: str) -> dict[date, list[tuple[datetime, float]]]:
    tz = ZoneInfo(tz_name)
    periods = hourly_data.get("properties", {}).get("periods", [])
    by_date: dict[date, list[tuple[datetime, float]]] = {}

    for p in periods:
        start = p.get("startTime")
        if not start:
            continue

        dt = datetime.fromisoformat(start)
        dt_local = dt.astimezone(tz)

        temp_f = parse_hour_temp_f(p)
        if temp_f is None:
            continue

        d = dt_local.date()
        by_date.setdefault(d, []).append((dt_local, temp_f))

    return by_date


def estimate_daily_max_temp_f(hourly_temps: list[tuple[datetime, float]]) -> float | None:
    if not hourly_temps:
        return None
    return max(t for _, t in hourly_temps)


def estimate_daily_min_temp_f(hourly_temps: list[tuple[datetime, float]]) -> float | None:
    if not hourly_temps:
        return None
    return min(t for _, t in hourly_temps)


def parse_market_date(market: dict) -> date | None:
    # Backward-compatible helper: use period start.
    start, _ = parse_market_period(market)
    return start


def parse_market_probability(market: dict, mean_temp_f: float, sigma_f: float) -> float | None:
    nd = NormalDist(mu=mean_temp_f, sigma=sigma_f)

    strike_type = (market.get("strike_type") or "").strip().lower()
    floor_strike = safe_float(market.get("floor_strike"))
    cap_strike = safe_float(market.get("cap_strike"))
    title = (market.get("title") or "").lower().strip()

    log(
        f"parse_market_probability: ticker={market.get('ticker')} "
        f"title={market.get('title')} strike_type={strike_type} "
        f"floor={floor_strike} cap={cap_strike}"
    )

    if strike_type == "between":
        if floor_strike is not None and cap_strike is not None:
            lo = floor_strike
            hi = cap_strike
            return max(0.0, min(1.0, nd.cdf(hi + 0.5) - nd.cdf(lo - 0.5)))

    if strike_type in ("greater", "greater_than", "greater_or_equal"):
        x = floor_strike if floor_strike is not None else cap_strike
        if x is not None:
            if strike_type == "greater_or_equal":
                return 1.0 - nd.cdf(x - 0.5)
            return 1.0 - nd.cdf(x)

    if strike_type in ("less", "below", "less_than", "less_or_equal"):
        x = cap_strike if cap_strike is not None else floor_strike
        if x is not None:
            if strike_type == "less_or_equal":
                return nd.cdf(x + 0.5)
            return nd.cdf(x)

    m = re.search(r"(\d+)\s*(?:°|Â°)?\s*or below", title)
    if m:
        x = float(m.group(1))
        return nd.cdf(x + 0.5)

    m = re.search(r"(\d+)\s*(?:°|Â°)?\s*or above", title)
    if m:
        x = float(m.group(1))
        return 1.0 - nd.cdf(x - 0.5)

    m = re.search(r"<\s*(\d+)", title)
    if m:
        x = float(m.group(1))
        return nd.cdf(x)

    m = re.search(r">\s*(\d+)", title)
    if m:
        x = float(m.group(1))
        return 1.0 - nd.cdf(x)

    m = re.search(r"(\d+)\s*-\s*(\d+)\s*(?:°|Â°)", title)
    if m:
        lo = float(m.group(1))
        hi = float(m.group(2))
        return max(0.0, min(1.0, nd.cdf(hi + 0.5) - nd.cdf(lo - 0.5)))

    return None


def summarize_observation(obs: dict | None) -> str:
    if not obs:
        return ""

    props = obs.get("properties", {})
    temp_c = props.get("temperature", {}).get("value")
    timestamp = props.get("timestamp")
    station = props.get("station")
    temp_f = c_to_f(temp_c) if temp_c is not None else None

    parts = []
    if station:
        parts.append(f"station={station}")
    if timestamp:
        parts.append(f"obs_time={timestamp}")
    if temp_f is not None:
        parts.append(f"obs_temp_f={temp_f:.1f}")

    return "; ".join(parts)


def extract_obs_temp_f(obs: dict | None) -> float | None:
    if not obs:
        return None
    props = obs.get("properties", {})
    temp_c = props.get("temperature", {}).get("value")
    return c_to_f(temp_c) if temp_c is not None else None


def parse_obs_timestamp(obs: dict | None) -> datetime | None:
    if not obs:
        return None
    ts = obs.get("properties", {}).get("timestamp")
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts)
    except Exception:
        return None


def minutes_since_utc(ts: datetime | None) -> float | None:
    if ts is None:
        return None
    try:
        return (datetime.now(UTC) - ts.astimezone(UTC)).total_seconds() / 60.0
    except Exception:
        return None


def dump_debug_json(filename: str, obj: dict):
    os.makedirs("output", exist_ok=True)
    safe_obj = obj
    if isinstance(obj, dict):
        safe_obj = {(
            k.isoformat() if isinstance(k, date) else k
        ): v for k, v in obj.items()}

    with open(os.path.join("output", filename), "w", encoding="utf-8") as f:
        json.dump(safe_obj, f, indent=2)


def rank_city(city) -> tuple[list[dict], int, int, list[str], list[str]]:
    log(f"\n{'=' * 80}\nCITY: {city.name}\n{'=' * 80}")
    quality_alerts: list[str] = []

    points = get_points_metadata(city.lat, city.lon)
    dump_debug_json(f"{city.name.lower().replace(' ', '_')}_points.json", points)

    point_props = points.get("properties", {})
    forecast_hourly_url = point_props.get("forecastHourly")
    obs_stations_url = points["properties"].get("observationStations")

    if not forecast_hourly_url:
        alert = f"{city.name}: missing forecastHourly in NWS points metadata; city run skipped"
        warn(alert)
        quality_alerts.append(alert)
        return [], 0, 0, [], quality_alerts

    log(f"forecastHourly={forecast_hourly_url}")
    log(f"observationStations={obs_stations_url}")

    hourly = get_hourly_forecast_from_points(points)
    dump_debug_json(f"{city.name.lower().replace(' ', '_')}_hourly.json", hourly)
    hourly_periods = hourly.get("properties", {}).get("periods", [])
    if len(hourly_periods) < MIN_HOURLY_PERIODS:
        alert = (
            f"{city.name}: only {len(hourly_periods)} hourly periods returned "
            f"(min expected {MIN_HOURLY_PERIODS}); city run skipped"
        )
        warn(alert)
        quality_alerts.append(alert)
        return [], 0, 0, [], quality_alerts

    daily = get_daily_forecast_from_points(points)
    if daily:
        dump_debug_json(f"{city.name.lower().replace(' ', '_')}_daily.json", daily)
    else:
        alert = f"{city.name}: daily forecast unavailable; running with hourly + open-meteo only"
        warn(alert)
        quality_alerts.append(alert)

    stations = get_observation_stations(points)
    log(f"station count from NWS={len(stations)}")
    if not stations:
        alert = f"{city.name}: no observation stations returned; obs-based adjustments disabled"
        warn(alert)
        quality_alerts.append(alert)

    obs = None
    observed_extrema_by_date: dict[date, dict[str, float]] = {}
    if stations:
        first_station_id = stations[0].get("id")
        log(f"first observation station id={first_station_id}")
        obs = get_latest_obs_from_station_url(first_station_id)
        if obs:
            dump_debug_json(f"{city.name.lower().replace(' ', '_')}_latest_obs.json", obs)
            obs_age_min = minutes_since_utc(parse_obs_timestamp(obs))
            if obs_age_min is not None and obs_age_min > MAX_OBS_STALENESS_MINUTES:
                alert = (
                    f"{city.name}: latest observation is stale ({obs_age_min:.1f} minutes old); "
                    "obs-based sigma/temperature adjustments disabled"
                )
                warn(alert)
                quality_alerts.append(alert)
                obs = None
        else:
            alert = f"{city.name}: latest observation missing; obs-based adjustments disabled"
            warn(alert)
            quality_alerts.append(alert)
        if first_station_id:
            today_local = local_today(city.timezone)
            start_utc, end_utc = local_day_bounds_utc(today_local, city.timezone)
            observations = get_station_observations(first_station_id, start_utc, end_utc)
            observed_extrema_by_date = get_observed_extrema_by_date(observations, city.timezone)
            if not observations:
                alert = f"{city.name}: no intraday station observations for today"
                warn(alert)
                quality_alerts.append(alert)
            if observations:
                dump_debug_json(
                    f"{city.name.lower().replace(' ', '_')}_obs_day_sample.json",
                    {"sample_count": min(20, len(observations)), "observations": observations[:20]},
                )
            log(
                f"Observed extrema for {today_local.isoformat()} ({city.name}): "
                f"{observed_extrema_by_date.get(today_local)}"
            )

    temps_by_date = get_hourly_temps_by_date(hourly, city.timezone)
    daily_highs_by_date = get_daily_highs_by_date(daily) if daily else {}

    all_dates = set(temps_by_date.keys()) | set(daily_highs_by_date.keys())
    if all_dates:
        start_date = min(all_dates)
        end_date = max(all_dates)
    else:
        start_date = local_today(city.timezone)
        end_date = start_date

    open_meteo_by_date = get_open_meteo_daily_max(city.lat, city.lon, start_date, end_date, city.timezone)
    dump_debug_json(f"{city.name.lower().replace(' ', '_')}_open_meteo.json", open_meteo_by_date)
    if not open_meteo_by_date:
        alert = f"{city.name}: open-meteo max forecast unavailable; continuing with remaining sources"
        warn(alert)
        quality_alerts.append(alert)

    obs_summary = summarize_observation(obs)
    obs_temp_f = extract_obs_temp_f(obs)

    # Process high temperature markets
    nws_weight, alt_weight, open_meteo_weight = get_source_weights("high")
    log(
        f"Source weights ({'history' if USE_HISTORY_WEIGHTS else 'fixed'}) "
        f"for high: NWS={nws_weight:.3f}, ALT={alt_weight:.3f}, OPEN={open_meteo_weight:.3f}"
    )

    calibration_high = get_calibration_data("high")
    log(
        f"Calibration ({'history' if USE_HISTORY_CALIBRATION else 'disabled'}) "
        f"for high sample count: {calibration_high.get('count', 0)}"
    )

    for d, vals in sorted(temps_by_date.items()):
        max_temp = estimate_daily_max_temp_f(vals)
        log(f"forecast date={d.isoformat()} hourly_count={len(vals)} estimated_max_f={max_temp}")

    # Extract high temps for each date
    high_temps_by_date = {}
    for market_date, hourly_list in temps_by_date.items():
        est_high = estimate_daily_max_temp_f(hourly_list)
        obs_high = observed_extrema_by_date.get(market_date, {}).get("max_so_far_f")
        if est_high is None:
            high_temps_by_date[market_date] = obs_high
        elif obs_high is None:
            high_temps_by_date[market_date] = est_high
        else:
            high_temps_by_date[market_date] = max(est_high, obs_high)

    high_alt_by_date = {}
    for market_date, daily_list in daily_highs_by_date.items():
        high_alt_by_date[market_date] = estimate_daily_max_temp_f(daily_list)

    for d in sorted(high_temps_by_date.keys()):
        nws_val = high_temps_by_date.get(d)
        open_val = open_meteo_by_date.get(d)
        if nws_val is not None and open_val is not None and abs(nws_val - open_val) >= EXTREME_DISAGREEMENT_F:
            alert = (
                f"{city.name} high forecast disagreement {abs(nws_val - open_val):.1f}F on {d.isoformat()} "
                f"(NWS={nws_val:.1f}, OpenMeteo={open_val:.1f})"
            )
            warn(alert)
            quality_alerts.append(alert)

    log(f"DEBUG: Processing {city.series_ticker} with {len(high_temps_by_date)} dates in forecast")
    log(f"DEBUG: high_temps_by_date keys: {list(high_temps_by_date.keys())}")
    
    high_rows, high_scanned, high_passed, high_skipped = process_markets_for_type(
        city,
        city.series_ticker,
        "high",
        "temp_high_f",
        "F",
        high_temps_by_date,
        high_alt_by_date,
        open_meteo_by_date,
        temps_by_date,
        nws_weight,
        alt_weight,
        open_meteo_weight,
        calibration_high,
        obs_temp_f,
        obs_summary,
        observed_extrema_by_date=observed_extrema_by_date,
    )
    log(f"DEBUG: High markets result: scanned={high_scanned}, passed={high_passed}, rows_returned={len(high_rows)}")

    # Process low temperature markets if configured
    all_rows = high_rows
    total_scanned = high_scanned
    total_passed = high_passed
    all_skipped = high_skipped

    if city.low_series_ticker:
        log(f"\n{'=' * 40}\nProcessing LOW temperatures\n{'=' * 40}")

        # Fetch low temperature data
        daily_lows_by_date = get_daily_lows_by_date(daily) if daily else {}
        open_meteo_daily_min = get_open_meteo_daily_min(city.lat, city.lon, start_date, end_date, city.timezone)
        dump_debug_json(f"{city.name.lower().replace(' ', '_')}_open_meteo_min.json", open_meteo_daily_min)
        if not open_meteo_daily_min:
            alert = f"{city.name}: open-meteo min forecast unavailable for low market processing"
            warn(alert)
            quality_alerts.append(alert)

        nws_weight_low, alt_weight_low, open_meteo_weight_low = get_source_weights("low")
        log(
            f"Source weights ({'history' if USE_HISTORY_WEIGHTS else 'fixed'}) "
            f"for low: NWS={nws_weight_low:.3f}, ALT={alt_weight_low:.3f}, OPEN={open_meteo_weight_low:.3f}"
        )

        calibration_low = get_calibration_data("low")
        log(
            f"Calibration ({'history' if USE_HISTORY_CALIBRATION else 'disabled'}) "
            f"for low sample count: {calibration_low.get('count', 0)}"
        )

        for d, vals in sorted(temps_by_date.items()):
            min_temp = estimate_daily_min_temp_f(vals)
            log(f"forecast date={d.isoformat()} hourly_count={len(vals)} estimated_min_f={min_temp}")

        # Extract low temps for each date
        low_temps_by_date = {}
        for market_date, hourly_list in temps_by_date.items():
            est_low = estimate_daily_min_temp_f(hourly_list)
            obs_low = observed_extrema_by_date.get(market_date, {}).get("min_so_far_f")
            if est_low is None:
                low_temps_by_date[market_date] = obs_low
            elif obs_low is None:
                low_temps_by_date[market_date] = est_low
            else:
                low_temps_by_date[market_date] = min(est_low, obs_low)

        low_alt_by_date = {}
        for market_date, daily_list in daily_lows_by_date.items():
            low_alt_by_date[market_date] = estimate_daily_min_temp_f(daily_list)

        for d in sorted(low_temps_by_date.keys()):
            nws_val = low_temps_by_date.get(d)
            open_val = open_meteo_daily_min.get(d)
            if nws_val is not None and open_val is not None and abs(nws_val - open_val) >= EXTREME_DISAGREEMENT_F:
                alert = (
                    f"{city.name} low forecast disagreement {abs(nws_val - open_val):.1f}F on {d.isoformat()} "
                    f"(NWS={nws_val:.1f}, OpenMeteo={open_val:.1f})"
                )
                warn(alert)
                quality_alerts.append(alert)

        low_rows, low_scanned, low_passed, low_skipped = process_markets_for_type(
            city,
            city.low_series_ticker,
            "low",
            "temp_low_f",
            "F",
            low_temps_by_date,
            low_alt_by_date,
            open_meteo_daily_min,
            temps_by_date,
            nws_weight_low,
            alt_weight_low,
            open_meteo_weight_low,
            calibration_low,
            obs_temp_f,
            obs_summary,
            observed_extrema_by_date=observed_extrema_by_date,
        )

        all_rows.extend(low_rows)
        total_scanned += low_scanned
        total_passed += low_passed
        all_skipped.extend(low_skipped)

    # Optional precipitation / snowfall / wind-gust markets
    additional_market_specs = [
        ("rain", city.rain_series_ticker, "rain_total_in", city.rain_sigma, False),
        ("snow", city.snow_series_ticker, "snow_total_in", city.snow_sigma, False),
        ("wind", city.wind_series_ticker, "wind_gust_mph", city.wind_sigma, False),
    ]

    for market_type_name, series_ticker, metric_key, metric_sigma, dynamic_sigma_enabled in additional_market_specs:
        if not series_ticker:
            continue

        try:
            primary_by_date, alt_by_date, open_by_date, metric_unit = get_metric_inputs_from_free_sources(
                hourly_data=hourly,
                daily_data=daily,
                tz_name=city.timezone,
                lat=city.lat,
                lon=city.lon,
                start_date=start_date,
                end_date=end_date,
                metric_key=metric_key,
            )
            observed_primary_by_date: dict[date, float] = {}
            observed_alt_by_date: dict[date, float] = {}
            observed_open_by_date: dict[date, float] = {}

            if metric_key in ("rain_total_in", "snow_total_in"):
                today_local = local_today(city.timezone)
                month_start = date(today_local.year, today_local.month, 1)
                archive_end = today_local.fromordinal(today_local.toordinal() - 1)
                if archive_end >= month_start:
                    archive_field = "precipitation_sum" if metric_key == "rain_total_in" else "snowfall_sum"
                    observed_fields = fetch_open_meteo_archive_daily_fields(
                        http_get_json,
                        city.lat,
                        city.lon,
                        month_start,
                        archive_end,
                        city.timezone,
                        [archive_field],
                    )
                    observed_series = observed_fields.get(archive_field, {})
                    observed_primary_by_date = dict(observed_series)
                    observed_open_by_date = dict(observed_series)

            dump_debug_json(
                f"{city.name.lower().replace(' ', '_')}_{market_type_name}_metric_sources.json",
                {
                    "metric": summarize_metric_sources(metric_key, metric_unit),
                    "primary_count": len(primary_by_date),
                    "alt_count": len(alt_by_date),
                    "open_count": len(open_by_date),
                    "observed_primary_count": len(observed_primary_by_date),
                    "observed_alt_count": len(observed_alt_by_date),
                    "observed_open_count": len(observed_open_by_date),
                },
            )

            w_nws, w_alt, w_open = get_source_weights(market_type_name)
            calibration_metric = get_calibration_data(market_type_name)

            rows_metric, scanned_metric, passed_metric, skipped_metric = process_markets_for_type(
                city,
                series_ticker,
                market_type_name,
                metric_key,
                metric_unit,
                primary_by_date,
                alt_by_date,
                open_by_date,
                {},  # no temperature-based sigma dynamics for these metrics
                w_nws,
                w_alt,
                w_open,
                calibration_metric,
                obs_temp_f,
                obs_summary,
                observed_extrema_by_date=None,
                observed_primary_by_date=observed_primary_by_date,
                observed_alt_by_date=observed_alt_by_date,
                observed_open_by_date=observed_open_by_date,
                base_sigma=metric_sigma,
                use_dynamic_sigma=dynamic_sigma_enabled,
            )
            all_rows.extend(rows_metric)
            total_scanned += scanned_metric
            total_passed += passed_metric
            all_skipped.extend(skipped_metric)
        except Exception as e:
            alert = f"{city.name}: {market_type_name} market processing failed: {e}"
            warn(alert)
            quality_alerts.append(alert)

    # Sort all rows by effective edge
    all_rows.sort(key=lambda x: x["effective_edge"], reverse=True)

    return all_rows, total_scanned, total_passed, all_skipped, quality_alerts


def append_history(rows):
    os.makedirs("history", exist_ok=True)
    hist_file = "history/market_history.csv"

    if not rows:
        return

    new_fieldnames = list(rows[0].keys())
    existing_rows = []
    existing_fieldnames = []

    if os.path.exists(hist_file):
        with open(hist_file, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            existing_rows = list(reader)
            existing_fieldnames = list(reader.fieldnames or [])

    merged_fieldnames = list(dict.fromkeys(existing_fieldnames + new_fieldnames))

    with open(hist_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=merged_fieldnames)
        writer.writeheader()
        if existing_rows:
            writer.writerows(existing_rows)
        writer.writerows(rows)


def build_snapshot_rows(rows: list[dict], limit: int) -> list[dict]:
    picked = []
    for row in rows[:limit]:
        picked.append(
            {
                "market_ticker": row.get("market_ticker"),
                "city": row.get("city"),
                "market_type": row.get("market_type"),
                "forecast_date": row.get("forecast_date"),
                "title": row.get("title"),
                "best_side": row.get("best_side"),
                "effective_edge": row.get("effective_edge"),
                "confidence_score": row.get("confidence_score"),
                "spread": row.get("spread"),
                "market_yes_mid": row.get("market_yes_mid"),
                "fair_yes": row.get("fair_yes"),
                "fair_yes_calibrated": row.get("fair_yes_calibrated"),
                "sigma_f": row.get("sigma_f"),
                "forecast_daily_temp_f": row.get("forecast_daily_temp_f"),
                "forecast_nws_temp_f": row.get("forecast_nws_temp_f"),
                "forecast_alt_temp_f": row.get("forecast_alt_temp_f"),
                "forecast_open_meteo_temp_f": row.get("forecast_open_meteo_temp_f"),
                "forecast_disagreement": row.get("forecast_disagreement"),
                "obs_temp_f": row.get("obs_temp_f"),
                "no_trade_flag": row.get("no_trade_flag"),
                "strategy_version": row.get("strategy_version"),
            }
        )
    return picked


def write_run_snapshot(
    ts: str,
    all_rows: list[dict],
    safe_recommended: list[dict],
    total_scanned: int,
    total_passed: int,
    skipped_summary: list[str],
    quality_alerts: list[str],
    rankings_csv_path: str,
) -> str:
    os.makedirs("output", exist_ok=True)
    snapshot_path = os.path.join("output", f"run_snapshot_{ts}.json")
    payload = {
        "strategy_version": STRATEGY_VERSION,
        "run_ts_utc": datetime.now(UTC).isoformat(),
        "target_mode": TARGET_MODE,
        "use_history_weights": USE_HISTORY_WEIGHTS,
        "use_history_calibration": USE_HISTORY_CALIBRATION,
        "totals": {
            "markets_scanned": total_scanned,
            "markets_passing_filters": total_passed,
            "rows_written": len(all_rows),
            "safe_recommended": len(safe_recommended),
        },
        "files": {
            "rankings_csv": rankings_csv_path,
        },
        "quality_alerts": quality_alerts,
        "skipped_summary_top20": skipped_summary[:20],
        "top_safe_recommended": build_snapshot_rows(safe_recommended, 5),
        "top_overall": build_snapshot_rows(all_rows, 10),
    }

    with open(snapshot_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)

    return snapshot_path


def main():
    all_rows = []
    total_scanned = 0
    total_passed = 0

    skipped_summary = []
    quality_alerts = []
    for city in CITIES:
        try:
            rows, scanned, passed, skipped, city_alerts = rank_city(city)
            total_scanned += scanned
            total_passed += passed
            all_rows.extend(rows)
            skipped_summary.extend(skipped)
            quality_alerts.extend(city_alerts)
        except Exception as e:
            warn(f"{city.name} failed: {e}")

    all_rows.sort(key=lambda x: x["effective_edge"], reverse=True)

    os.makedirs("output", exist_ok=True)
    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    out_file = f"output/weather_rankings_{ts}.csv"

    if not all_rows:
        print("No ranked markets produced.")
        return

    fieldnames = list(all_rows[0].keys())
    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    append_history(all_rows)

    safe_recommended = [
        r for r in all_rows
        if not r.get("no_trade_flag")
        and r["effective_edge"] >= MEDIUM_EDGE_CUTOFF
        and r["spread"] <= MEDIUM_SPREAD_CUTOFF
        and r.get("confidence_score", 0) >= MEDIUM_CONFIDENCE_CUTOFF
    ]

    snapshot_path = write_run_snapshot(
        ts=ts,
        all_rows=all_rows,
        safe_recommended=safe_recommended,
        total_scanned=total_scanned,
        total_passed=total_passed,
        skipped_summary=skipped_summary,
        quality_alerts=quality_alerts,
        rankings_csv_path=out_file,
    )

    same_day = [r for r in safe_recommended if r.get("same_day")]
    next_day = [r for r in safe_recommended if not r.get("same_day")]

    print(f"\nStrategy version: {STRATEGY_VERSION}")
    print(f"\nWrote {len(all_rows)} rows to {out_file}")
    print(f"Run snapshot written to {snapshot_path}")
    print(f"Markets scanned: {total_scanned}")
    print(f"Markets passing filters: {total_passed}")
    print(f"Safe recommended candidates: {len(safe_recommended)}")
    print(f"Same-day candidates: {len(same_day)} | Next-day candidates: {len(next_day)}")
    print(f"Data quality alerts: {len(quality_alerts)}")

    if quality_alerts:
        print("\nData quality alerts (top 20):")
        for a in quality_alerts[:20]:
            print("-", a)

    if not safe_recommended:
        print("No safe trade recommendations today (no_trade)")

    print("\nTop 10 safe recommended:")
    for row in safe_recommended[:10]:
        print(
            f"{row['city']:12s} | {row['forecast_date']} | {row['market_ticker']} | {row['best_side']:3s} | "
            f"eff={row['effective_edge']:.4f} | conf={row['confidence_score']:.4f} | "
            f"spread={row['spread']:.4f} | mkt={row['market_yes_mid']:.4f} | {row['title']}"
        )

    print("\nBest moves (iPhone-friendly):")
    for row in safe_recommended[:5]:
        action = "BUY YES" if row['best_side'] == 'YES' else 'BUY NO'
        print(
            f"{row['market_ticker']} - {action} - eff {row['effective_edge']:.3f}, spread {row['spread']:.3f}, "
            f"mkt {row['market_yes_mid']:.3f}, conf {row.get('confidence_score',0):.3f}"
        )


if __name__ == "__main__":
    main()
