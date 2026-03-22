from datetime import date, datetime
from zoneinfo import ZoneInfo


def mm_to_in(mm: float | None) -> float | None:
    if mm is None:
        return None
    return mm / 25.4


def cm_to_in(cm: float | None) -> float | None:
    if cm is None:
        return None
    return cm / 2.54


def c_to_f(c: float | None) -> float | None:
    if c is None:
        return None
    return (c * 9.0 / 5.0) + 32.0


def ms_to_mph(ms: float | None) -> float | None:
    if ms is None:
        return None
    return ms * 2.236936


def parse_wind_speed_text_to_mph(text: str | None) -> float | None:
    if not text:
        return None
    text = text.strip().lower()
    # Handles strings like "10 mph", "5 to 10 mph", "10 to 15 mph"
    nums = []
    cur = ""
    for ch in text:
        if ch.isdigit() or ch == ".":
            cur += ch
        elif cur:
            try:
                nums.append(float(cur))
            except Exception:
                pass
            cur = ""
    if cur:
        try:
            nums.append(float(cur))
        except Exception:
            pass
    if not nums:
        return None
    return sum(nums) / len(nums)


def extract_nws_metric_values_by_date(hourly_data: dict, tz_name: str, metric: str) -> dict[date, list[float]]:
    tz = ZoneInfo(tz_name)
    periods = hourly_data.get("properties", {}).get("periods", [])
    by_date: dict[date, list[float]] = {}

    for p in periods:
        start = p.get("startTime")
        if not start:
            continue
        try:
            dt_local = datetime.fromisoformat(start).astimezone(tz)
        except Exception:
            continue
        d = dt_local.date()

        value = None
        if metric == "temp_f":
            temp = p.get("temperature")
            unit = p.get("temperatureUnit")
            if temp is not None:
                if unit == "F":
                    value = float(temp)
                elif unit == "C":
                    value = c_to_f(float(temp))
        elif metric == "precip_in":
            qp = p.get("quantitativePrecipitation", {})
            raw = qp.get("value")
            unit_code = (qp.get("unitCode") or "").lower()
            if raw is not None:
                raw = float(raw)
                value = mm_to_in(raw) if "mm" in unit_code else raw
        elif metric == "snow_in":
            sf = p.get("snowfallAmount", {})
            raw = sf.get("value")
            unit_code = (sf.get("unitCode") or "").lower()
            if raw is not None:
                raw = float(raw)
                value = mm_to_in(raw) if "mm" in unit_code else raw
        elif metric == "wind_gust_mph":
            gust = p.get("windGust", {})
            raw = gust.get("value")
            unit_code = (gust.get("unitCode") or "").lower()
            if raw is not None:
                raw = float(raw)
                value = ms_to_mph(raw) if "m_s-1" in unit_code else raw
        elif metric == "wind_speed_mph":
            value = parse_wind_speed_text_to_mph(p.get("windSpeed"))
        elif metric == "precip_prob":
            pop = p.get("probabilityOfPrecipitation", {})
            raw = pop.get("value")
            if raw is not None:
                value = float(raw) / 100.0

        if value is None:
            continue
        by_date.setdefault(d, []).append(value)

    return by_date


def aggregate_by_date(values_by_date: dict[date, list[float]], agg: str) -> dict[date, float]:
    out: dict[date, float] = {}
    for d, vals in values_by_date.items():
        if not vals:
            continue
        if agg == "max":
            out[d] = max(vals)
        elif agg == "min":
            out[d] = min(vals)
        elif agg == "sum":
            out[d] = sum(vals)
        elif agg == "mean":
            out[d] = sum(vals) / len(vals)
        elif agg == "prob_any":
            # combine independent-ish hourly probabilities into daily event probability
            miss_prob = 1.0
            for p in vals:
                p = max(0.0, min(1.0, p))
                miss_prob *= (1.0 - p)
            out[d] = 1.0 - miss_prob
    return out


def fetch_open_meteo_daily_fields(
    http_get_json,
    lat: float,
    lon: float,
    start_date: date,
    end_date: date,
    timezone: str,
    daily_fields: list[str],
) -> dict[str, dict[date, float]]:
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": ",".join(daily_fields),
        "timezone": timezone,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "temperature_unit": "fahrenheit",
        "precipitation_unit": "inch",
        "wind_speed_unit": "mph",
    }
    data = http_get_json(url, params=params, accept="application/json")
    daily = data.get("daily", {})
    times = daily.get("time", [])
    by_field: dict[str, dict[date, float]] = {f: {} for f in daily_fields}

    for idx, dt_str in enumerate(times):
        try:
            d = date.fromisoformat(dt_str)
        except Exception:
            continue
        for field in daily_fields:
            vals = daily.get(field, [])
            if idx >= len(vals):
                continue
            raw = vals[idx]
            if raw is None:
                continue
            v = float(raw)
            # Open-Meteo snowfall_sum is commonly cm; normalize to inches.
            if field == "snowfall_sum":
                v = cm_to_in(v)
            by_field[field][d] = v

    return by_field


def fetch_open_meteo_archive_daily_fields(
    http_get_json,
    lat: float,
    lon: float,
    start_date: date,
    end_date: date,
    timezone: str,
    daily_fields: list[str],
) -> dict[str, dict[date, float]]:
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": ",".join(daily_fields),
        "timezone": timezone,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "temperature_unit": "fahrenheit",
        "precipitation_unit": "inch",
        "wind_speed_unit": "mph",
    }
    data = http_get_json(url, params=params, accept="application/json")
    daily = data.get("daily", {})
    times = daily.get("time", [])
    by_field: dict[str, dict[date, float]] = {f: {} for f in daily_fields}

    for idx, dt_str in enumerate(times):
        try:
            d = date.fromisoformat(dt_str)
        except Exception:
            continue
        for field in daily_fields:
            vals = daily.get(field, [])
            if idx >= len(vals):
                continue
            raw = vals[idx]
            if raw is None:
                continue
            v = float(raw)
            if field == "snowfall_sum":
                v = cm_to_in(v)
            by_field[field][d] = v

    return by_field


def summarize_metric_sources(metric_key: str, unit: str) -> dict:
    return {"metric_key": metric_key, "unit": unit}
