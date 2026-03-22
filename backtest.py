import csv
import os
from math import sqrt

HISTORY_FILE = "history/market_history.csv"
OUTPUT_FILE = "output/backtest_performance.csv"


def safe_float(x):
    if x is None or x == "":
        return None


def get_forecast_value(row: dict, source: str):
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
    try:
        return float(x)
    except Exception:
        return None


def load_history(path=HISTORY_FILE):
    if not os.path.exists(path):
        raise FileNotFoundError(f"History file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def compute_performance(rows):
    metrics = {
        "n": 0,
        "nws_sq_err": 0.0,
        "alt_sq_err": 0.0,
        "open_sq_err": 0.0,
        "fair_sq_err": 0.0,
        "fair_cal_sq_err": 0.0,
        "brier_fair": 0.0,
        "brier_fair_cal": 0.0,
        "wins": 0,
    }

    candidates = []
    for row in rows:
        actual = safe_float(row.get("actual_outcome_yes"))
        if actual is None:
            continue
        actual = 1.0 if actual >= 0.5 else 0.0

        fair = safe_float(row.get("fair_yes"))
        fair_cal = safe_float(row.get("fair_yes_calibrated"))
        market = safe_float(row.get("market_yes_mid"))
        nws_value = get_forecast_value(row, "nws")
        alt_value = get_forecast_value(row, "alt")
        open_value = get_forecast_value(row, "open_meteo")

        if fair is None:
            continue

        metrics["n"] += 1

        if fair_cal is None:
            fair_cal = fair

        metrics["fair_sq_err"] += (fair - actual) ** 2
        metrics["fair_cal_sq_err"] += (fair_cal - actual) ** 2
        metrics["brier_fair"] += (fair - actual) ** 2
        metrics["brier_fair_cal"] += (fair_cal - actual) ** 2

        if nws_value is not None:
            # project from source forecast error via fair signal approximation
            metrics["nws_sq_err"] += (nws_value - actual) ** 2
        if alt_value is not None:
            metrics["alt_sq_err"] += (alt_value - actual) ** 2
        if open_value is not None:
            metrics["open_sq_err"] += (open_value - actual) ** 2

        if row.get("model_win") not in (None, "", "None"):
            try:
                if int(row.get("model_win")) == 1:
                    metrics["wins"] += 1
            except Exception:
                pass

        candidates.append(row)

    return metrics, candidates


def format_metrics(metrics):
    n = metrics["n"]
    if n == 0:
        return None
    out = {
        "n": n,
        "rmse_fair": sqrt(metrics["fair_sq_err"] / n),
        "rmse_fair_calibrated": sqrt(metrics["fair_cal_sq_err"] / n),
        "brier_fair": metrics["brier_fair"] / n,
        "brier_fair_calibrated": metrics["brier_fair_cal"] / n,
        "win_rate": metrics["wins"] / n,
    }
    for k in ["nws_sq_err", "alt_sq_err", "open_sq_err"]:
        out[f"rmse_{k.replace('_sq_err', '')}"] = sqrt(metrics[k] / n) if metrics[k] > 0 else None
    return out


def persist_summary(stats, path=OUTPUT_FILE):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(stats.keys()))
        writer.writeheader()
        writer.writerow(stats)


def main():
    rows = load_history()
    metrics, _ = compute_performance(rows)
    stats = format_metrics(metrics)

    if not stats:
        print("No settle data available in history to backtest.")
        return

    print("Backtest summary:")
    for k, v in stats.items():
        print(f"{k}: {v}")

    persist_summary(stats)
    print(f"Backtest summary written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
