"""Declarative cleaning rule engines and the built-in rule generations.

A generation's definition is an immutable JSON document. Every clean result
stores the rule trace, so a query can be expanded from its value back to the
exact rule ids, generation version and definition hash that produced it.
"""
from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

OUTPUT_FIELDS = (
    "order_id",
    "customer_email",
    "currency",
    "region",
    "status",
    "item_count",
    "amount_cents",
)

_EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z0-9-]{2,}$")
_INT_RE = re.compile(r"^-?\d+$")
_CURRENCIES = ("USD", "EUR", "GBP", "JPY")


def _s(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _parse_money_float(raw: str) -> int | None:
    text = raw.strip()
    for sym in ("$", "€", "£", "¥", " ", ","):
        text = text.replace(sym, "")
    try:
        return int(round(float(text) * 100))
    except ValueError:
        return None


def _parse_money_exact(raw: str) -> int | None:
    text = raw.strip().replace(" ", "").replace(" ", "")
    for iso in _CURRENCIES:
        text = text.replace(iso, "").replace(iso.lower(), "")
    for sym in ("$", "€", "£", "¥"):
        text = text.replace(sym, "")
    if not text or not re.match(r"^[+-]?[\d.,]+$", text):
        return None
    has_dot, has_comma = "." in text, "," in text
    if has_dot and has_comma:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif has_comma:
        head, _, tail = text.rpartition(",")
        # "1,50" is a decimal comma; "1,500" is a thousands separator.
        text = (head.replace(",", "") + "." + tail) if len(tail) in (1, 2) else text.replace(",", "")
    try:
        amount = Decimal(text)
    except InvalidOperation:
        return None
    cents = (amount * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    if abs(cents) > Decimal("999999999"):
        return None
    return int(cents)


class Ctx:
    def __init__(self, payload: dict):
        self.raw = payload
        self.out: dict[str, Any] = dict(payload)
        for f in OUTPUT_FIELDS:
            self.out.setdefault(f, None)
        self.flags: list[str] = []
        self.trace: list[dict] = []
        self.dropped: str | None = None
        self.touched = False

    def flag(self, code: str) -> None:
        if code not in self.flags:
            self.flags.append(code)

    def note(self, rule: dict, outcome: str, **detail) -> None:
        entry: dict[str, Any] = {"rule": rule["id"], "v": rule.get("v", 1), "outcome": outcome}
        if detail:
            entry["detail"] = detail
        self.trace.append(entry)
        if outcome in ("changed", "flagged", "dropped"):
            self.touched = True


def _apply_rule(ctx: Ctx, rule: dict) -> None:
    kind = rule["kind"]
    n0 = len(ctx.trace)

    if kind == "trim":
        for field in rule["fields"]:
            value = ctx.out.get(field)
            if value is None:
                continue
            before = _s(value)
            after = before.strip()
            if before != after:
                ctx.out[field] = after
                ctx.note(rule, "changed", field=field)

    elif kind == "case":
        mode = rule["mode"]
        for field in rule["fields"]:
            value = ctx.out.get(field)
            if value is None:
                continue
            before = _s(value)
            after = before.upper() if mode == "upper" else before.lower()
            if before != after:
                ctx.out[field] = after
                ctx.note(rule, "changed", field=field)

    elif kind == "money":
        field = rule["field"]
        target = rule.get("target", "amount_cents")
        if ctx.out.get(field) is None:
            if rule.get("flag_missing", False):
                ctx.flag("missing_amount")
                ctx.note(rule, "flagged", reason="missing")
            return
        raw = _s(ctx.out[field])
        cents = _parse_money_float(raw) if rule.get("mode", "float") == "float" else _parse_money_exact(raw)
        if cents is None:
            ctx.out[target] = None
            ctx.out.pop(field, None)
            ctx.flag("invalid_amount")
            ctx.note(rule, "flagged", reason="unparseable", raw=raw)
        else:
            original = ctx.raw.get(field)
            ctx.out[target] = cents
            if field != target:
                ctx.out.pop(field, None)
            if isinstance(original, (int, float)) and int(round(float(original) * 100)) == cents:
                return
            if original is not None and _s(original).strip() not in (f"{cents / 100:.2f}", _s(cents)):
                ctx.note(rule, "changed", field=target, amount_cents=cents)

    elif kind == "int":
        field = rule["field"]
        original = ctx.out.get(field)
        if original is None:
            return
        raw = _s(original).strip()
        value: int | None = None
        if rule.get("mode", "loose") == "loose":
            try:
                value = int(float(raw))
            except ValueError:
                value = None
        elif _INT_RE.match(raw):
            value = int(raw)
        if value is None:
            ctx.out[field] = None
            ctx.flag("invalid_item_count")
            ctx.note(rule, "flagged", field=field, reason="unparseable", raw=raw)
        elif original != value:
            ctx.out[field] = value
            ctx.note(rule, "changed", field=field, value=value)

    elif kind == "alias":
        field = rule["field"]
        mapping: dict[str, str] = rule["map"]
        on_unknown = rule.get("on_unknown", "flag")  # flag | keep | null
        value = ctx.out.get(field)
        if value is None:
            if on_unknown != "null":
                ctx.flag(f"missing_{field}")
            return
        raw = _s(value)
        key = raw.strip()
        if rule.get("lower", True):
            key = key.lower()
        if key in mapping:
            canonical = mapping[key]
            ctx.out[field] = canonical
            if raw != canonical:
                ctx.note(rule, "changed", field=field, value=canonical)
        else:
            if on_unknown == "null":
                ctx.out[field] = None
            else:
                ctx.out[field] = raw.strip()
            if on_unknown == "flag":
                ctx.flag(f"unknown_{field}")
            ctx.note(rule, "flagged", field=field, reason="unknown_value", raw=raw)

    elif kind == "email":
        field = rule["field"]
        value = ctx.out.get(field)
        if value is None:
            return
        raw = _s(value)
        normalized = raw.strip().lower()
        ctx.out[field] = normalized
        if raw != normalized:
            ctx.note(rule, "changed", field=field)
        if not _EMAIL_RE.match(normalized):
            ctx.flag("invalid_email")
            if rule.get("null_invalid", False):
                ctx.out[field] = None
            ctx.note(rule, "flagged", field=field, reason="invalid")

    elif kind == "required":
        missing = [f for f in rule["fields"] if ctx.out.get(f) in (None, "")]
        if missing:
            ctx.dropped = "missing_required:" + ",".join(missing)
            ctx.flag("missing_required")
            ctx.note(rule, "dropped", fields=missing)

    elif kind == "range":
        field = rule["field"]
        value = ctx.out.get(field)
        if isinstance(value, int) and not isinstance(value, bool):
            lo, hi = rule.get("min"), rule.get("max")
            if (lo is not None and value < lo) or (hi is not None and value > hi):
                ctx.flag(rule.get("flag", f"{field}_out_of_range"))
                ctx.note(rule, "flagged", field=field, value=value)

    else:
        raise ValueError(f"unknown rule kind {kind!r}")

    if len(ctx.trace) == n0:
        ctx.note(rule, "ok")


def apply(definition: dict, payload: dict) -> dict:
    ctx = Ctx(payload)
    for rule in definition["rules"]:
        _apply_rule(ctx, rule)
        if ctx.dropped:
            break
    status = "dropped" if ctx.dropped else ("corrected" if ctx.touched else "clean")
    output = {f: ctx.out.get(f) for f in OUTPUT_FIELDS}
    if ctx.dropped:
        output["drop_reason"] = ctx.dropped
    return {"status": status, "output": output, "trace": ctx.trace, "flags": ctx.flags}


STATUS_MAP_V1 = {
    "COMPLETED": "completed",
    "PENDING": "pending",
    "CANCELLED": "cancelled",
    "REFUNDED": "refunded",
}

STATUS_MAP_V2 = {
    "completed": "completed",
    "complete": "completed",
    "done": "completed",
    "pending": "pending",
    "awaiting": "pending",
    "cancelled": "cancelled",
    "canceled": "cancelled",
    "cancel": "cancelled",
    "refunded": "refunded",
    "refund": "refunded",
    "failed": "failed",
    "declined": "failed",
}

CURRENCY_MAP_V2 = {
    "usd": "USD",
    "us dollar": "USD",
    "dollar": "USD",
    "dollars": "USD",
    "us$": "USD",
    "$": "USD",
    "eur": "EUR",
    "euro": "EUR",
    "euros": "EUR",
    "€": "EUR",
    "gbp": "GBP",
    "pound": "GBP",
    "pounds": "GBP",
    "£": "GBP",
    "jpy": "JPY",
    "yen": "JPY",
    "¥": "JPY",
}

REGION_MAP_V2 = {
    "us-east": "us-east",
    "us east": "us-east",
    "east": "us-east",
    "na-east": "us-east",
    "us-west": "us-west",
    "us west": "us-west",
    "west": "us-west",
    "na-west": "us-west",
    "eu-central": "eu-central",
    "eu central": "eu-central",
    "central": "eu-central",
    "eu": "eu-central",
    "apac": "apac",
    "ap-southeast": "apac",
    "asia": "apac",
}

BUILTIN_GENERATIONS: dict[str, dict] = {
    "1.0.0": {
        "version": "1.0.0",
        "label": "Legacy normalizer",
        "rules": [
            {"id": "trim.basic", "v": 1, "kind": "trim",
             "fields": ["order_id", "customer_email", "region", "currency"]},
            {"id": "money.float", "v": 1, "kind": "money", "field": "amount",
             "target": "amount_cents", "mode": "float"},
            {"id": "currency.upper", "v": 1, "kind": "alias", "field": "currency", "lower": False,
             "map": {"USD": "USD", "EUR": "EUR", "GBP": "GBP", "JPY": "JPY"}, "on_unknown": "flag"},
            {"id": "status.exact", "v": 1, "kind": "alias", "field": "status", "lower": False,
             "map": STATUS_MAP_V1, "on_unknown": "flag"},
            {"id": "count.loose", "v": 1, "kind": "int", "field": "item_count", "mode": "loose"},
        ],
    },
    "2.0.0": {
        "version": "2.0.0",
        "label": "Exact-money normalizer",
        "rules": [
            {"id": "id.canonical", "v": 2, "kind": "trim", "fields": ["order_id"]},
            {"id": "id.upper", "v": 2, "kind": "case", "mode": "upper", "fields": ["order_id"]},
            {"id": "email.normalize", "v": 2, "kind": "email", "field": "customer_email"},
            {"id": "money.exact", "v": 2, "kind": "money", "field": "amount",
             "target": "amount_cents", "mode": "exact"},
            {"id": "currency.alias", "v": 2, "kind": "alias", "field": "currency", "lower": True,
             "map": CURRENCY_MAP_V2, "on_unknown": "flag"},
            {"id": "region.alias", "v": 2, "kind": "alias", "field": "region", "lower": True,
             "map": REGION_MAP_V2, "on_unknown": "flag"},
            {"id": "status.alias", "v": 2, "kind": "alias", "field": "status", "lower": True,
             "map": STATUS_MAP_V2, "on_unknown": "flag"},
            {"id": "count.strict", "v": 2, "kind": "int", "field": "item_count", "mode": "strict"},
            {"id": "required.core", "v": 2, "kind": "required",
             "fields": ["order_id", "customer_email", "currency", "amount_cents"]},
            {"id": "range.amount", "v": 2, "kind": "range", "field": "amount_cents",
             "min": -500000, "max": 500000, "flag": "amount_out_of_range"},
            {"id": "range.count", "v": 2, "kind": "range", "field": "item_count",
             "min": 1, "max": 999, "flag": "item_count_out_of_range"},
        ],
    },
}


def definition_for(version: str) -> dict:
    if version not in BUILTIN_GENERATIONS:
        raise KeyError(f"unknown built-in generation {version!r}; have {sorted(BUILTIN_GENERATIONS)}")
    return BUILTIN_GENERATIONS[version]
