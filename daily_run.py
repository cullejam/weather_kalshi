import csv
import glob
import html as html_module
import os
import smtplib
import subprocess
import sys
from datetime import UTC, datetime
from email.message import EmailMessage

from execute_trades import execute_recommended_trades

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

if load_dotenv is not None:
    load_dotenv()


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        return float(raw)
    except Exception:
        return default


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        return int(raw)
    except Exception:
        return default


SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = env_int("SMTP_PORT", 587)
EMAIL_FROM = os.getenv("EMAIL_FROM", "jamescullen2019@gmail.com")
EMAIL_TO = os.getenv("EMAIL_TO", "jamescullen2019@gmail.com")
EMAIL_USER = os.getenv("EMAIL_USER", EMAIL_FROM)
EMAIL_PASS = os.getenv("EMAIL_PASS", "")

TOTAL_BUDGET_USD = env_float("TOTAL_BUDGET_USD", 100.0)
TOP_MOVE_COUNT = env_int("TOP_MOVE_COUNT", 5)
RISK_BUCKET_COUNT = env_int("RISK_BUCKET_COUNT", 3)
MAX_POSITION_PCT = env_float("MAX_POSITION_PCT", 0.40)
RISKY_ALLOCATION_MULTIPLIER = env_float("RISKY_ALLOCATION_MULTIPLIER", 0.0)
NO_TRADE_ALLOCATION_MULTIPLIER = env_float("NO_TRADE_ALLOCATION_MULTIPLIER", 0.0)
CONFIDENCE_TARGET = env_float("CONFIDENCE_TARGET", 0.10)
EDGE_TARGET = env_float("EDGE_TARGET", 0.10)

LEGEND_ITEMS = [
    ("Eff Edge", "Net model edge after basic spread cost. Higher is better."),
    ("Confidence", "Conservative score after spread/disagreement/day penalties."),
    ("Spread", "YES ask minus YES bid. Lower is generally safer to enter."),
    ("Mkt Yes", "Market-implied probability from midpoint price."),
    ("Fair Yes", "Model-estimated probability from forecast + sigma."),
    ("Sigma", "Temperature uncertainty in F used by the probability model."),
    ("Disagree", "Forecast source disagreement in F. Higher means less certainty."),
]

RISK_BAND_DESCRIPTIONS = {
    "Safe": "High hit probability and tighter execution quality. Lower upside per trade, higher consistency.",
    "Medium": "Balanced profile across hit probability and upside. Moderate uncertainty and reward.",
    "Risky": "Lower hit probability and/or wider market uncertainty. Higher upside potential if correct.",
}


def latest_run_snapshot() -> str | None:
    files = sorted(glob.glob("output/run_snapshot_*.json"), key=os.path.getmtime, reverse=True)
    return files[0] if files else None


def send_email(subject: str, text_body: str, html_body: str | None = None):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO
    msg.set_content(text_body)

    if html_body:
        msg.add_alternative(html_body, subtype="html")

    if not EMAIL_PASS:
        print("[WARN] EMAIL_PASS not set; skipping email")
        return False

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=30) as smtp:
            smtp.starttls()
            smtp.login(EMAIL_USER, EMAIL_PASS)
            smtp.send_message(msg)
        return True
    except Exception as e:
        print(f"[WARN] Unable to send email: {e}")
        return False


def validate_email_config() -> tuple[bool, list[str]]:
    missing = []
    if not EMAIL_PASS:
        missing.append("EMAIL_PASS")
    if not EMAIL_TO:
        missing.append("EMAIL_TO")
    if not EMAIL_FROM:
        missing.append("EMAIL_FROM")
    if not EMAIL_USER:
        missing.append("EMAIL_USER")
    return (len(missing) == 0), missing


def safe_float(value, default=None):
    if value in (None, ""):
        return default
    try:
        return float(value)
    except Exception:
        return default


def is_truthy(value) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes"}


def parse_rank_metrics(rank_output: str) -> dict:
    metrics = {"scanned": None, "passed": None, "safe_recommended": None}
    for line in rank_output.splitlines():
        stripped = line.strip()
        if stripped.startswith("Markets scanned:"):
            try:
                metrics["scanned"] = int(stripped.split(":", 1)[1].strip())
            except Exception:
                pass
        elif stripped.startswith("Markets passing filters:"):
            try:
                metrics["passed"] = int(stripped.split(":", 1)[1].strip())
            except Exception:
                pass
        elif stripped.startswith("Safe recommended candidates:"):
            try:
                metrics["safe_recommended"] = int(stripped.split(":", 1)[1].strip())
            except Exception:
                pass
    return metrics


def latest_rankings_csv() -> str | None:
    files = sorted(glob.glob("output/weather_rankings_*.csv"), key=os.path.getmtime, reverse=True)
    return files[0] if files else None


def load_rankings(csv_path: str) -> list[dict]:
    with open(csv_path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def to_move(row: dict) -> dict:
    edge = safe_float(row.get("effective_edge"), 0.0)
    conf = safe_float(row.get("confidence_score"), 0.0)
    score = 0.70 * edge + 0.30 * conf
    return {
        "city": row.get("city", ""),
        "market_type": row.get("market_type", "") or "high",
        "metric_key": row.get("forecast_metric_key", ""),
        "forecast_unit": row.get("forecast_unit", ""),
        "market_ticker": row.get("market_ticker", ""),
        "title": row.get("title", ""),
        "forecast_date": row.get("forecast_date", ""),
        "close_time": row.get("close_time", ""),
        "best_side": row.get("best_side", ""),
        "effective_edge": edge,
        "confidence_score": conf,
        "spread": safe_float(row.get("spread")),
        "yes_bid": safe_float(row.get("yes_bid")),
        "yes_ask": safe_float(row.get("yes_ask")),
        "market_yes_mid": safe_float(row.get("market_yes_mid")),
        "fair_yes": safe_float(row.get("fair_yes")),
        "fair_no": safe_float(row.get("fair_no")),
        "sigma_f": safe_float(row.get("sigma_f")),
        "forecast_disagreement": safe_float(row.get("forecast_disagreement")),
        "forecast_daily_temp_f": safe_float(row.get("forecast_daily_temp_f")),
        "forecast_nws_temp_f": safe_float(row.get("forecast_nws_temp_f")),
        "forecast_alt_temp_f": safe_float(row.get("forecast_alt_temp_f")),
        "forecast_open_meteo_temp_f": safe_float(row.get("forecast_open_meteo_temp_f")),
        "obs_temp_f": safe_float(row.get("obs_temp_f")),
        "no_trade_flag": is_truthy(row.get("no_trade_flag")),
        "score": score,
        "strategy_version": row.get("strategy_version", ""),
    }


def implied_hit_probability(best_side: str, fair_yes: float | None, fair_no: float | None) -> float | None:
    if best_side == "YES":
        return fair_yes
    if best_side == "NO":
        return fair_no
    return None


def classify_risk_band(move: dict) -> str:
    hit_prob = move.get("hit_probability")
    spread = move.get("spread")
    conf = move.get("confidence_score") or 0.0
    disagree = move.get("forecast_disagreement")
    no_trade = move.get("no_trade_flag", False)

    if hit_prob is None:
        return "Medium"

    if (
        not no_trade
        and hit_prob >= 0.70
        and (spread is None or spread <= 0.03)
        and conf >= 0.05
        and (disagree is None or disagree <= 2.5)
    ):
        return "Safe"

    if (
        hit_prob < 0.45
        or no_trade
        or (spread is not None and spread > 0.06)
        or conf < 0.03
        or (disagree is not None and disagree > 4.0)
    ):
        return "Risky"

    return "Medium"


def build_risk_buckets(rows: list[dict], per_bucket: int = RISK_BUCKET_COUNT) -> dict[str, list[dict]]:
    buckets = {"Safe": [], "Medium": [], "Risky": []}
    all_moves = sorted([to_move(r) for r in rows], key=lambda x: x["effective_edge"], reverse=True)

    for move in all_moves:
        move["hit_probability"] = implied_hit_probability(move["best_side"], move["fair_yes"], move["fair_no"])
        band = classify_risk_band(move)
        if len(buckets[band]) < per_bucket:
            buckets[band].append(move)

    return buckets


def select_top_moves(rows: list[dict], max_count: int = TOP_MOVE_COUNT) -> tuple[list[dict], str]:
    moves = [to_move(r) for r in rows]
    safe_moves = [m for m in moves if not m["no_trade_flag"]]

    if safe_moves:
        ranked = sorted(safe_moves, key=lambda x: x["effective_edge"], reverse=True)
        return ranked[:max_count], "safe"

    ranked = sorted(moves, key=lambda x: x["effective_edge"], reverse=True)
    return ranked[:max_count], "fallback_all"


def compute_allocations(moves: list[dict], total_budget: float) -> tuple[list[float], float]:
    if not moves:
        return [], total_budget

    def clamp(value: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, value))

    def raw_weight(move: dict) -> float:
        base = max(0.0, move.get("score", 0.0))
        conf = max(0.0, move.get("confidence_score") or 0.0)
        edge = max(0.0, move.get("effective_edge") or 0.0)
        hit_prob = move.get("hit_probability")
        no_trade = bool(move.get("no_trade_flag", False))

        conf_target = max(0.01, CONFIDENCE_TARGET)
        edge_target = max(0.01, EDGE_TARGET)
        conf_factor = clamp(conf / conf_target, 0.25, 1.5)
        edge_factor = clamp(edge / edge_target, 0.50, 1.5)
        weight = base * conf_factor * edge_factor

        # Keep risky/no-trade ideas in the email, but don't size them by default.
        if no_trade:
            weight *= max(0.0, NO_TRADE_ALLOCATION_MULTIPLIER)
        elif hit_prob is not None and hit_prob < 0.45:
            weight *= max(0.0, RISKY_ALLOCATION_MULTIPLIER)

        return max(0.0, weight)

    weights = [raw_weight(m) for m in moves]
    total_weight = sum(weights)
    if total_weight <= 0:
        zero_allocs = [0.0] * len(moves)
        return zero_allocs, round(total_budget, 2)

    amounts = [(w / total_weight) * total_budget for w in weights]
    cap = max(0.0, min(1.0, MAX_POSITION_PCT)) * total_budget

    if cap > 0:
        for _ in range(10):
            over = [i for i, a in enumerate(amounts) if a > cap + 1e-9]
            if not over:
                break
            excess = 0.0
            for i in over:
                excess += amounts[i] - cap
                amounts[i] = cap

            under = [i for i, a in enumerate(amounts) if a < cap - 1e-9 and weights[i] > 0]
            if not under or excess <= 0:
                break
            under_weight = sum(weights[i] for i in under)
            if under_weight <= 0:
                break
            for i in under:
                amounts[i] += excess * (weights[i] / under_weight)

    allocations = [round(a, 2) for a in amounts]
    rounded_total = round(sum(allocations), 2)
    if allocations:
        allocations[-1] = round(allocations[-1] + (total_budget - rounded_total), 2)
    remaining = round(total_budget - sum(allocations), 2)
    return allocations, remaining


def fmt(value, digits=3, na="n/a"):
    if value is None:
        return na
    return f"{value:.{digits}f}"


def build_execution_lines(summary: dict | None) -> list[str]:
    if not summary:
        return ["Execution: not run"]

    mode = "disabled"
    if summary.get("enabled"):
        mode = "dry_run" if summary.get("dry_run") else "live"

    lines = [
        f"- Mode: {mode}",
        f"- Attempted: {summary.get('attempted', 0)} | Eligible: {summary.get('eligible', 0)} | "
        f"Placed: {summary.get('placed', 0)} | Failed: {summary.get('failed', 0)} | Skipped: {summary.get('skipped', 0)}",
        f"- Intended notional: ${summary.get('intended_notional_usd', 0):.2f} | "
        f"Placed notional: ${summary.get('placed_notional_usd', 0):.2f}",
        f"- Ledger: {summary.get('ledger_path', 'n/a')}",
    ]
    notes = summary.get("notes") or []
    if notes:
        lines.append("- Notes: " + "; ".join(str(n) for n in notes))
    return lines


def build_text_email(
    metrics: dict,
    moves: list[dict],
    allocations: list[float],
    selection_mode: str,
    csv_path: str,
    risk_buckets: dict[str, list[dict]],
    strategy_version: str,
    snapshot_path: str | None,
    execution_summary: dict | None = None,
) -> str:
    lines = []
    lines.append("weather_kalshi daily trading brief")
    lines.append(f"Generated UTC: {datetime.now(UTC).isoformat()}")
    lines.append(f"Strategy version: {strategy_version}")
    lines.append("")
    lines.append("Legend")
    for term, meaning in LEGEND_ITEMS:
        lines.append(f"- {term}: {meaning}")
    lines.append("")
    lines.append("Run stats")
    lines.append(f"- Markets scanned: {metrics.get('scanned', 'n/a')}")
    lines.append(f"- Markets passing filters: {metrics.get('passed', 'n/a')}")
    lines.append(f"- Safe recommended candidates: {metrics.get('safe_recommended', 'n/a')}")
    lines.append(f"- Selection mode: {selection_mode}")
    lines.append(
        "- Allocation policy: confidence-weighted "
        f"(max_position_pct={MAX_POSITION_PCT:.0%}, risky_mult={RISKY_ALLOCATION_MULTIPLIER:.2f}, "
        f"no_trade_mult={NO_TRADE_ALLOCATION_MULTIPLIER:.2f})"
    )
    lines.append("")
    lines.append("Trade Execution")
    lines.extend(build_execution_lines(execution_summary))
    lines.append("")

    if not moves:
        lines.append("Top moves")
        lines.append("- No candidates found in latest rankings file.")
    else:
        lines.append(f"Top {len(moves)} moves")
        for idx, move in enumerate(moves, start=1):
            alloc = allocations[idx - 1] if idx - 1 < len(allocations) else 0.0
            lines.append(f"{idx}. {move['market_ticker']} | {move['city']} {move['market_type']} | BUY {move['best_side']}")
            lines.append(f"   Title: {move['title']}")
            lines.append(
                f"   Date: {move['forecast_date']} | Close: {move['close_time']} | "
                f"metric={move.get('metric_key') or move['market_type']} ({move.get('forecast_unit') or 'n/a'})"
            )
            lines.append(
                "   Tech: "
                f"eff={fmt(move['effective_edge'])}, conf={fmt(move['confidence_score'])}, spread={fmt(move['spread'])}, "
                f"mkt_yes={fmt(move['market_yes_mid'])}, fair_yes={fmt(move['fair_yes'])}, fair_no={fmt(move['fair_no'])}, "
                f"hit_prob={fmt(move.get('hit_probability'))}, sigma={fmt(move['sigma_f'], 2)}, disagree={fmt(move['forecast_disagreement'], 2)}"
            )
            lines.append(
                "   Temps(F): "
                f"blend={fmt(move['forecast_daily_temp_f'], 2)}, nws={fmt(move['forecast_nws_temp_f'], 2)}, "
                f"alt={fmt(move['forecast_alt_temp_f'], 2)}, open={fmt(move['forecast_open_meteo_temp_f'], 2)}, "
                f"obs={fmt(move['obs_temp_f'], 2)}"
            )
            lines.append(f"   Allocation: ${alloc:.2f} | Score={fmt(move['score'], 4)}")
            lines.append("")

    lines.append("Risk Ladder (3 rows)")
    for band in ("Safe", "Medium", "Risky"):
        items = risk_buckets.get(band, [])
        desc = RISK_BAND_DESCRIPTIONS.get(band, "")
        if not items:
            lines.append(f"- {band}: {desc}")
            lines.append("   none")
            continue
        lines.append(f"- {band}: {desc}")
        for move in items:
            lines.append(
                f"   {move['market_ticker']} | BUY {move['best_side']} | "
                f"hit_prob={fmt(move.get('hit_probability'))} | eff={fmt(move['effective_edge'])} | "
                f"conf={fmt(move['confidence_score'])} | spread={fmt(move['spread'])}"
            )
            lines.append(f"      Title: {move['title']}")
            lines.append(
                "      Detail: "
                f"date={move['forecast_date']}, mkt_yes={fmt(move['market_yes_mid'])}, "
                f"fair_yes={fmt(move['fair_yes'])}, fair_no={fmt(move['fair_no'])}, "
                f"sigma={fmt(move['sigma_f'], 2)}, disagree={fmt(move['forecast_disagreement'], 2)}"
            )
            lines.append(
                "      Temps(F): "
                f"blend={fmt(move['forecast_daily_temp_f'], 2)}, nws={fmt(move['forecast_nws_temp_f'], 2)}, "
                f"alt={fmt(move['forecast_alt_temp_f'], 2)}, open={fmt(move['forecast_open_meteo_temp_f'], 2)}, "
                f"obs={fmt(move['obs_temp_f'], 2)}"
            )
    lines.append("")

    lines.append("Files")
    lines.append(f"- Rankings CSV: {csv_path}")
    if snapshot_path:
        lines.append(f"- Run snapshot: {snapshot_path}")
    lines.append("- History CSV: history/market_history.csv")
    return "\n".join(lines)


def html_kv(label: str, value: str) -> str:
    return (
        '<div style="display:flex; gap:8px; font-size:13px; line-height:1.35;">'
        f'<div style="min-width:88px; color:#6b7280;">{html_module.escape(label)}</div>'
        f'<div style="color:#111827; font-weight:600;">{html_module.escape(value)}</div>'
        "</div>"
    )


def build_html_email(
    metrics: dict,
    moves: list[dict],
    allocations: list[float],
    selection_mode: str,
    csv_path: str,
    risk_buckets: dict[str, list[dict]],
    strategy_version: str,
    snapshot_path: str | None,
    execution_summary: dict | None = None,
) -> str:
    html_parts = [
        '<html><body style="margin:0; padding:24px; background:#f3f4f6; font-family:Segoe UI,Arial,sans-serif; color:#111827;">',
        '<div style="max-width:840px; margin:auto; background:#ffffff; border:1px solid #e5e7eb; border-radius:14px; overflow:hidden;">',
        '<div style="background:#0f172a; color:#f8fafc; padding:18px 20px;">',
        '<div style="font-size:22px; font-weight:700;">weather_kalshi daily trading brief</div>',
        f'<div style="margin-top:4px; font-size:12px; color:#cbd5e1;">Generated UTC: {html_module.escape(datetime.now(UTC).isoformat())}</div>',
        f'<div style="margin-top:2px; font-size:12px; color:#cbd5e1;">Strategy version: {html_module.escape(strategy_version)}</div>',
        "</div>",
        '<div style="padding:18px 20px;">',
        '<div style="font-size:16px; font-weight:700; margin-bottom:8px;">Legend</div>',
        '<div style="border:1px solid #e5e7eb; border-radius:10px; padding:12px 14px; background:#fafafa;">',
    ]

    for term, meaning in LEGEND_ITEMS:
        html_parts.append(
            f'<div style="font-size:13px; line-height:1.45; margin:4px 0;">'
            f'<span style="font-weight:700; color:#111827;">{html_module.escape(term)}</span>'
            f'<span style="color:#4b5563;"> - {html_module.escape(meaning)}</span>'
            "</div>"
        )

    html_parts.extend(
        [
            "</div>",
            '<div style="display:flex; gap:12px; flex-wrap:wrap; margin-top:14px;">',
            f'<div style="padding:8px 10px; border-radius:8px; background:#eef2ff; font-size:13px;"><strong>Scanned:</strong> {metrics.get("scanned", "n/a")}</div>',
            f'<div style="padding:8px 10px; border-radius:8px; background:#ecfeff; font-size:13px;"><strong>Passed:</strong> {metrics.get("passed", "n/a")}</div>',
            f'<div style="padding:8px 10px; border-radius:8px; background:#f0fdf4; font-size:13px;"><strong>Safe:</strong> {metrics.get("safe_recommended", "n/a")}</div>',
            f'<div style="padding:8px 10px; border-radius:8px; background:#fff7ed; font-size:13px;"><strong>Mode:</strong> {html_module.escape(selection_mode)}</div>',
            "</div>",
            (
                '<div style="margin-top:10px; font-size:12px; color:#4b5563;">'
                f'Allocation policy: confidence-weighted '
                f'(max_position_pct={MAX_POSITION_PCT:.0%}, risky_mult={RISKY_ALLOCATION_MULTIPLIER:.2f}, '
                f'no_trade_mult={NO_TRADE_ALLOCATION_MULTIPLIER:.2f})'
                "</div>"
            ),
            '<div style="margin-top:10px; padding:10px; border:1px solid #e5e7eb; border-radius:8px; background:#fafafa;">'
            '<div style="font-size:13px; font-weight:700; margin-bottom:4px;">Trade execution</div>'
            + "".join(
                f'<div style="font-size:12px; color:#374151; line-height:1.4;">{html_module.escape(line.lstrip("- ").strip())}</div>'
                for line in build_execution_lines(execution_summary)
            )
            + "</div>",
            f'<div style="margin-top:16px; font-size:16px; font-weight:700;">Top {len(moves)} moves</div>',
        ]
    )

    if not moves:
        html_parts.append('<div style="margin-top:8px; color:#6b7280;">No candidates found in latest rankings file.</div>')
    else:
        for idx, move in enumerate(moves, start=1):
            alloc = allocations[idx - 1] if idx - 1 < len(allocations) else 0.0
            html_parts.extend(
                [
                    '<div style="margin-top:12px; border:1px solid #e5e7eb; border-radius:10px; padding:12px 14px;">',
                    f'<div style="font-size:15px; font-weight:700;">{idx}. {html_module.escape(move["market_ticker"])} - BUY {html_module.escape(move["best_side"])}</div>',
                    f'<div style="margin-top:4px; color:#374151; font-size:13px;">{html_module.escape(move["city"])} {html_module.escape(move["market_type"])} | {html_module.escape(move["title"])}</div>',
                    '<div style="margin-top:8px; display:grid; grid-template-columns:repeat(auto-fit,minmax(240px,1fr)); gap:8px;">',
                    html_kv("Date", move["forecast_date"]),
                    html_kv("Close", move["close_time"]),
                    html_kv("Metric", f"{move.get('metric_key') or move['market_type']} ({move.get('forecast_unit') or 'n/a'})"),
                    html_kv("Eff Edge", fmt(move["effective_edge"])),
                    html_kv("Confidence", fmt(move["confidence_score"])),
                    html_kv("Spread", fmt(move["spread"])),
                    html_kv("Mkt Yes", fmt(move["market_yes_mid"])),
                    html_kv("Fair Yes", fmt(move["fair_yes"])),
                    html_kv("Fair No", fmt(move["fair_no"])),
                    html_kv("Hit Prob", fmt(move.get("hit_probability"))),
                    html_kv("Sigma(F)", fmt(move["sigma_f"], 2)),
                    html_kv("Disagree(F)", fmt(move["forecast_disagreement"], 2)),
                    html_kv("Blend Temp(F)", fmt(move["forecast_daily_temp_f"], 2)),
                    html_kv("NWS Temp(F)", fmt(move["forecast_nws_temp_f"], 2)),
                    html_kv("ALT Temp(F)", fmt(move["forecast_alt_temp_f"], 2)),
                    html_kv("Open Temp(F)", fmt(move["forecast_open_meteo_temp_f"], 2)),
                    html_kv("Obs Temp(F)", fmt(move["obs_temp_f"], 2)),
                    html_kv("Allocation", f"${alloc:.2f} | score={fmt(move['score'], 4)}"),
                    "</div>",
                    "</div>",
                ]
            )

    html_parts.extend(
        [
            '<div style="margin-top:18px; font-size:16px; font-weight:700;">Risk ladder</div>',
            '<div style="margin-top:8px; border:1px solid #e5e7eb; border-radius:10px; overflow:hidden;">',
            '<table style="width:100%; border-collapse:collapse; font-size:13px;">',
            '<thead><tr style="background:#f8fafc;"><th style="text-align:left; padding:10px; border-bottom:1px solid #e5e7eb; width:110px;">Band</th><th style="text-align:left; padding:10px; border-bottom:1px solid #e5e7eb;">Markets</th></tr></thead>',
            "<tbody>",
        ]
    )

    for band in ("Safe", "Medium", "Risky"):
        rows = risk_buckets.get(band, [])
        band_desc = RISK_BAND_DESCRIPTIONS.get(band, "")
        if not rows:
            details = '<span style="color:#6b7280;">none</span>'
        else:
            parts = []
            for move in rows:
                parts.append(
                    f"<div style=\"margin:4px 0;\">"
                    f"<strong>{html_module.escape(move['market_ticker'])}</strong> - BUY {html_module.escape(move['best_side'])} "
                    f"| hit={fmt(move.get('hit_probability'))} | eff={fmt(move['effective_edge'])} "
                    f"| conf={fmt(move['confidence_score'])} | spread={fmt(move['spread'])}"
                    f"<div style=\"margin-top:2px; color:#374151;\">{html_module.escape(move['title'])}</div>"
                    f"<div style=\"margin-top:2px; color:#4b5563;\">"
                    f"date={html_module.escape(move['forecast_date'])} | "
                    f"mkt_yes={fmt(move['market_yes_mid'])} | fair_yes={fmt(move['fair_yes'])} | fair_no={fmt(move['fair_no'])} | "
                    f"sigma={fmt(move['sigma_f'], 2)} | disagree={fmt(move['forecast_disagreement'], 2)}"
                    f"</div>"
                    f"<div style=\"margin-top:2px; color:#4b5563;\">"
                    f"temps(F): blend={fmt(move['forecast_daily_temp_f'], 2)}, nws={fmt(move['forecast_nws_temp_f'], 2)}, "
                    f"alt={fmt(move['forecast_alt_temp_f'], 2)}, open={fmt(move['forecast_open_meteo_temp_f'], 2)}, "
                    f"obs={fmt(move['obs_temp_f'], 2)}"
                    f"</div>"
                    "</div>"
                )
            details = "".join(parts)

        html_parts.append(
            "<tr>"
            f"<td style=\"padding:10px; border-bottom:1px solid #f1f5f9; font-weight:700;\">"
            f"{html_module.escape(band)}"
            f"<div style=\"margin-top:4px; font-weight:400; font-size:12px; color:#6b7280; line-height:1.35;\">{html_module.escape(band_desc)}</div>"
            f"</td>"
            f"<td style=\"padding:10px; border-bottom:1px solid #f1f5f9;\">{details}</td>"
            "</tr>"
        )

    html_parts.extend(["</tbody></table>", "</div>"])

    html_parts.extend(
        [
            '<div style="margin-top:14px; padding-top:12px; border-top:1px solid #e5e7eb; font-size:12px; color:#6b7280;">',
            f"Rankings CSV: {html_module.escape(csv_path)}<br/>"
            + (f"Run snapshot: {html_module.escape(snapshot_path)}<br/>" if snapshot_path else "")
            + "History CSV: history/market_history.csv",
            "</div>",
            "</div>",
            "</div>",
            "</body></html>",
        ]
    )
    return "".join(html_parts)


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    py_exec = sys.executable

    steps = [
        ("Rank markets and append history", [py_exec, os.path.join(script_dir, "rank_weather_markets.py")]),
        ("Settle history updates", [py_exec, os.path.join(script_dir, "settle_history.py")]),
    ]

    rank_stdout = ""
    for step_name, cmd in steps:
        print(f"\n--- {step_name} ---")
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=script_dir)
        print(f"{step_name}: exit {result.returncode}")
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print("[WARN] stderr:")
            print(result.stderr)
        if step_name == "Rank markets and append history":
            rank_stdout = result.stdout or ""

    metrics = parse_rank_metrics(rank_stdout)
    csv_path = latest_rankings_csv()

    if not csv_path:
        print("[WARN] No rankings CSV found in output/. Email not sent.")
    else:
        rows = load_rankings(csv_path)
        strategy_version = rows[0].get("strategy_version", "unknown") if rows else "unknown"
        snapshot_path = latest_run_snapshot()
        top_moves, selection_mode = select_top_moves(rows, max_count=TOP_MOVE_COUNT)
        for move in top_moves:
            move["hit_probability"] = implied_hit_probability(move["best_side"], move["fair_yes"], move["fair_no"])
        risk_buckets = build_risk_buckets(rows, per_bucket=RISK_BUCKET_COUNT)
        allocations, _ = compute_allocations(top_moves, TOTAL_BUDGET_USD)
        execution_summary = None
        try:
            execution_summary = execute_recommended_trades(top_moves, allocations, strategy_version)
        except Exception as e:
            print(f"[WARN] Trade execution step failed: {e}")
            execution_summary = {
                "enabled": True,
                "dry_run": True,
                "attempted": 0,
                "eligible": 0,
                "placed": 0,
                "failed": 1,
                "skipped": 0,
                "intended_notional_usd": 0.0,
                "placed_notional_usd": 0.0,
                "ledger_path": "history/trade_ledger.csv",
                "notes": [f"execution_exception={e}"],
            }

        text_summary = build_text_email(
            metrics,
            top_moves,
            allocations,
            selection_mode,
            csv_path,
            risk_buckets,
            strategy_version,
            snapshot_path,
            execution_summary=execution_summary,
        )
        html_summary = build_html_email(
            metrics,
            top_moves,
            allocations,
            selection_mode,
            csv_path,
            risk_buckets,
            strategy_version,
            snapshot_path,
            execution_summary=execution_summary,
        )

        email_ok, missing_vars = validate_email_config()
        if not email_ok:
            print(
                "[WARN] Email config incomplete. Missing: "
                + ", ".join(missing_vars)
                + " (set in environment or .env)"
            )
            sent = False
        else:
            subject = f"weather_kalshi daily brief ({strategy_version}) - {datetime.now(UTC).strftime('%Y-%m-%d')}"
            sent = send_email(subject, text_summary, html_summary)
        if sent:
            print("Email sent to", EMAIL_TO)
        else:
            print("Email not sent")

    print("\nDaily run complete")
