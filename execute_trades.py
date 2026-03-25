import csv
import math
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from broker_kalshi import BrokerError, KalshiBroker
from config import USER_AGENT


def _env_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val in (None, ""):
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "on"}


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v in (None, ""):
            return default
        return float(v)
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        if v in (None, ""):
            return default
        return int(v)
    except Exception:
        return default


@dataclass
class ExecutionConfig:
    trading_enabled: bool
    dry_run: bool
    max_daily_notional_usd: float
    max_per_trade_usd: float
    min_order_notional_usd: float
    max_open_orders: int
    min_contracts: int
    min_edge: float
    min_confidence: float
    max_spread: float
    allow_risky: bool
    allow_no_trade: bool
    allowed_market_types: set[str]
    order_ttl_seconds: int
    ledger_path: str
    price_in_cents: bool


def load_execution_config() -> ExecutionConfig:
    allowed_raw = os.getenv("TRADE_ALLOWED_MARKET_TYPES", "high,low,rain,snow,wind")
    allowed_types = {x.strip().lower() for x in allowed_raw.split(",") if x.strip()}
    return ExecutionConfig(
        trading_enabled=_env_bool("TRADING_ENABLED", False),
        dry_run=_env_bool("TRADING_DRY_RUN", True),
        max_daily_notional_usd=_safe_float(os.getenv("TRADE_MAX_DAILY_NOTIONAL_USD", "0"), 0.0),
        max_per_trade_usd=_safe_float(os.getenv("TRADE_MAX_PER_TRADE_USD", "20"), 20.0),
        min_order_notional_usd=_safe_float(os.getenv("TRADE_MIN_ORDER_NOTIONAL_USD", "1"), 1.0),
        max_open_orders=_safe_int(os.getenv("TRADE_MAX_OPEN_ORDERS", "10"), 10),
        min_contracts=_safe_int(os.getenv("TRADE_MIN_CONTRACTS", "1"), 1),
        min_edge=_safe_float(os.getenv("TRADE_MIN_EDGE", "0.0"), 0.0),
        min_confidence=_safe_float(os.getenv("TRADE_MIN_CONFIDENCE", "0.0"), 0.0),
        max_spread=_safe_float(os.getenv("TRADE_MAX_SPREAD", "0.08"), 0.08),
        allow_risky=_env_bool("TRADE_ALLOW_RISKY", False),
        allow_no_trade=_env_bool("TRADE_ALLOW_NO_TRADE_FLAG", False),
        allowed_market_types=allowed_types,
        order_ttl_seconds=_safe_int(os.getenv("TRADE_ORDER_TTL_SECONDS", "60"), 60),
        ledger_path=os.getenv("TRADE_LEDGER_PATH", "history/trade_ledger.csv"),
        price_in_cents=_env_bool("TRADE_PRICE_IN_CENTS", False),
    )


def _ensure_ledger(path: str):
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    if os.path.exists(path):
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "run_ts_utc",
                "strategy_version",
                "mode",
                "status",
                "reason",
                "market_ticker",
                "market_type",
                "forecast_date",
                "best_side",
                "allocation_usd",
                "limit_price",
                "contracts",
                "order_id",
                "client_order_id",
                "response_excerpt",
                "effective_edge",
                "confidence_score",
                "spread",
            ],
        )
        writer.writeheader()


def _append_ledger(path: str, row: dict[str, Any]):
    _ensure_ledger(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        writer.writerow(row)


def _is_truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes"}


def _price_for_entry(move: dict[str, Any]) -> float | None:
    side = (move.get("best_side") or "").upper()
    yes_ask = move.get("yes_ask")
    yes_bid = move.get("yes_bid")
    mid = move.get("market_yes_mid")

    if side == "YES":
        p = _safe_float(yes_ask, 0.0) or _safe_float(mid, 0.0)
    elif side == "NO":
        if yes_bid not in (None, ""):
            p = 1.0 - _safe_float(yes_bid, 0.0)
        elif yes_ask not in (None, ""):
            p = 1.0 - _safe_float(yes_ask, 0.0)
        else:
            p = 1.0 - _safe_float(mid, 0.0)
    else:
        return None

    if p <= 0 or p >= 1:
        return None
    return p


def _eligible_reason(move: dict[str, Any], cfg: ExecutionConfig) -> str | None:
    market_type = str(move.get("market_type", "")).strip().lower()
    if cfg.allowed_market_types and market_type not in cfg.allowed_market_types:
        return f"market_type_blocked:{market_type}"

    spread = move.get("spread")
    if spread is not None and _safe_float(spread, 999) > cfg.max_spread:
        return "spread_too_wide"

    if _safe_float(move.get("effective_edge"), -999) < cfg.min_edge:
        return "edge_below_min"
    if _safe_float(move.get("confidence_score"), -999) < cfg.min_confidence:
        return "confidence_below_min"

    if _is_truthy(move.get("no_trade_flag")) and not cfg.allow_no_trade:
        return "no_trade_flagged"

    hit_prob = move.get("hit_probability")
    if hit_prob is not None and _safe_float(hit_prob, 1.0) < 0.45 and not cfg.allow_risky:
        return "risky_blocked"

    return None


def _build_order_payload(
    move: dict[str, Any],
    contracts: int,
    limit_price: float,
    cfg: ExecutionConfig,
    client_order_id: str,
) -> dict[str, Any]:
    side = str(move.get("best_side", "")).lower()
    payload: dict[str, Any] = {
        "ticker": move.get("market_ticker"),
        "side": side,
        "action": "buy",
        "type": "limit",
        "count": contracts,
        "client_order_id": client_order_id,
        "expiration_ts": int(datetime.now(UTC).timestamp()) + cfg.order_ttl_seconds,
    }
    if cfg.price_in_cents:
        payload["price"] = int(round(limit_price * 100))
    else:
        payload["price"] = round(limit_price, 4)
    return payload


def execute_recommended_trades(
    moves: list[dict[str, Any]],
    allocations: list[float],
    strategy_version: str,
) -> dict[str, Any]:
    cfg = load_execution_config()
    now = datetime.now(UTC).isoformat()
    summary: dict[str, Any] = {
        "enabled": cfg.trading_enabled,
        "dry_run": cfg.dry_run,
        "attempted": 0,
        "eligible": 0,
        "placed": 0,
        "failed": 0,
        "skipped": 0,
        "intended_notional_usd": 0.0,
        "placed_notional_usd": 0.0,
        "ledger_path": cfg.ledger_path,
        "notes": [],
    }

    if not cfg.trading_enabled:
        summary["notes"].append("TRADING_ENABLED=false")
        return summary

    broker = KalshiBroker.from_env(USER_AGENT)
    if not cfg.dry_run and not broker.has_auth():
        summary["notes"].append("Missing auth env vars for live trading")
        return summary

    _ensure_ledger(cfg.ledger_path)

    daily_budget = cfg.max_daily_notional_usd
    if daily_budget <= 0:
        daily_budget = sum(max(0.0, _safe_float(a, 0.0)) for a in allocations)
    remaining = daily_budget

    for idx, move in enumerate(moves):
        summary["attempted"] += 1
        alloc = max(0.0, _safe_float(allocations[idx] if idx < len(allocations) else 0.0, 0.0))
        alloc = min(alloc, cfg.max_per_trade_usd, remaining)
        summary["intended_notional_usd"] += alloc

        reason = _eligible_reason(move, cfg)
        limit_price = _price_for_entry(move)
        ticker = str(move.get("market_ticker", ""))
        side = str(move.get("best_side", "")).upper()
        if not ticker or side not in {"YES", "NO"}:
            reason = reason or "invalid_ticker_or_side"
        if limit_price is None:
            reason = reason or "invalid_entry_price"

        contracts = 0
        notional = 0.0
        if not reason and limit_price:
            contracts = math.floor(alloc / limit_price)
            if contracts < cfg.min_contracts:
                reason = "contracts_below_min"
            else:
                notional = round(contracts * limit_price, 2)
                if notional < cfg.min_order_notional_usd:
                    reason = "order_notional_below_min"

        ledger_row = {
            "run_ts_utc": now,
            "strategy_version": strategy_version,
            "mode": "dry_run" if cfg.dry_run else "live",
            "status": "skipped",
            "reason": reason or "",
            "market_ticker": ticker,
            "market_type": move.get("market_type", ""),
            "forecast_date": move.get("forecast_date", ""),
            "best_side": side,
            "allocation_usd": f"{alloc:.2f}",
            "limit_price": f"{(limit_price or 0):.4f}",
            "contracts": contracts,
            "order_id": "",
            "client_order_id": "",
            "response_excerpt": "",
            "effective_edge": move.get("effective_edge"),
            "confidence_score": move.get("confidence_score"),
            "spread": move.get("spread"),
        }

        if reason:
            summary["skipped"] += 1
            _append_ledger(cfg.ledger_path, ledger_row)
            continue

        if summary["placed"] >= cfg.max_open_orders:
            ledger_row["reason"] = "max_open_orders_reached"
            summary["skipped"] += 1
            summary["notes"].append("max_open_orders_reached")
            _append_ledger(cfg.ledger_path, ledger_row)
            break

        summary["eligible"] += 1
        client_order_id = f"wx_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}_{idx}_{ticker}"
        payload = _build_order_payload(move, contracts, limit_price, cfg, client_order_id)
        ledger_row["client_order_id"] = client_order_id

        if cfg.dry_run:
            ledger_row["status"] = "dry_run"
            ledger_row["response_excerpt"] = f"payload={payload}"
            summary["placed"] += 1
            summary["placed_notional_usd"] += notional
            remaining = max(0.0, remaining - notional)
            _append_ledger(cfg.ledger_path, ledger_row)
            continue

        try:
            response = broker.place_order(payload)
            order_obj = response.get("order", {}) if isinstance(response, dict) else {}
            order_id = order_obj.get("id") or order_obj.get("order_id") or ""
            ledger_row["status"] = "placed"
            ledger_row["order_id"] = order_id
            ledger_row["response_excerpt"] = str(response)[:500]
            summary["placed"] += 1
            summary["placed_notional_usd"] += notional
            remaining = max(0.0, remaining - notional)
        except BrokerError as e:
            ledger_row["status"] = "failed"
            ledger_row["reason"] = str(e)[:240]
            summary["failed"] += 1
        _append_ledger(cfg.ledger_path, ledger_row)

        if remaining <= 0:
            summary["notes"].append("daily_notional_exhausted")
            break

    summary["intended_notional_usd"] = round(summary["intended_notional_usd"], 2)
    summary["placed_notional_usd"] = round(summary["placed_notional_usd"], 2)
    return summary
