
from typing import Dict, List, Any, Optional
from datetime import datetime



class Predicate:
    """Base Boolean predicate (mask-equivalent)."""

    def __and__(self, other: "Predicate") -> "LogicalPredicate":
        return LogicalPredicate("AND", [self, other])

    def __or__(self, other: "Predicate") -> "LogicalPredicate":
        return LogicalPredicate("OR", [self, other])

    def __invert__(self) -> "LogicalPredicate":
        return LogicalPredicate("NOT", [self])

    def __xor__(self, other: "Predicate") -> "LogicalPredicate":
        return LogicalPredicate("XOR", [self, other])

    def compile(self, ctx: "SQLContext") -> str:
        raise NotImplementedError

    def cache_key(self) -> str:
        # ponytail: structural signature so distinct predicates don't collide
        # in @record_call arg signatures (which JSON-encode op arguments).
        import json

        ctx = SQLContext()
        sql = self.compile(ctx)
        return f"{sql}|{json.dumps(ctx.params, default=str)}"

    def __repr__(self) -> str:
        ctx = SQLContext()
        sql = self.compile(ctx)
        params = ctx.params
        return f"SQL: {sql}\nParams: {params}"

# LogicalPredicate


class LogicalPredicate(Predicate):
    def __init__(self, op: str, children: List[Predicate]):
        self.op = op
        self.children = children

    def compile(self, ctx: "SQLContext") -> str:
        if self.op == "NOT":
            return f"(NOT {self.children[0].compile(ctx)})"

        if self.op == "XOR":
            # ponytail: compile per occurrence — reusing one compiled string
            # repeats $n/? markers without adding params, which breaks
            # positional (?-style: DuckDB/ClickHouse) backends.
            a1 = self.children[0].compile(ctx)
            b1 = self.children[1].compile(ctx)
            a2 = self.children[0].compile(ctx)
            b2 = self.children[1].compile(ctx)
            return f"(({a1} AND NOT {b1}) OR (NOT {a2} AND {b2}))"

        compiled = [c.compile(ctx) for c in self.children]

        joined = f" {self.op} ".join(compiled)
        return f"({joined})"

class SQLContext:
    def __init__(self):
        self.params: List[Any] = []

    def param(self, value: Any) -> str:
        self.params.append(value)
        return f"${len(self.params)}"

    def col(self, name: str) -> str:
        return f'"{name}"'


# ============================================================================
# FILTER API
# ============================================================================

class Filter:
    @staticmethod
    def where(predicate: Predicate) -> "FilterPlan":
        return FilterPlan(predicate)


class FilterPlan:
    def __init__(self, predicate: Predicate):
        self.predicate = predicate

    async def fetch(self, table_info: Dict[str, Any]):
        ctx = SQLContext()
        where_sql = self.predicate.compile(ctx)

        sql = f"""
            SELECT *
            FROM "{table_info['schema']}"."{table_info['table']}"
            WHERE {where_sql}
        """

        return await table_info["conn"].fetch(sql, *ctx.params)

    async def count(self, table_info: Dict[str, Any]) -> int:
        ctx = SQLContext()
        where_sql = self.predicate.compile(ctx)

        sql = f"""
            SELECT COUNT(*)
            FROM "{table_info['schema']}"."{table_info['table']}"
            WHERE {where_sql}
        """

        return await table_info["conn"].fetchval(sql, *ctx.params)


# ============================================================================
# CONVENIENCE API (Optional)
# ============================================================================

class FilterAPI:
    """Convenience class for accessing all predicate factories."""

    @property
    def num(self):
        return Num

    @property
    def cat(self):
        return Cat

    @property
    def time(self):
        return Time


# Create a global instance for easy access
F = FilterAPI()

# ============================================================================
# NUMERIC PREDICATES
# ============================================================================

class NumericPredicate(Predicate):
    def __init__(
        self,
        column: str,
        operator: str,
        value: Any = None,
        other_column: Optional[str] = None,
    ):
        self.column = column
        self.operator = operator
        self.value = value
        self.other_column = other_column

    def compile(self, ctx: "SQLContext") -> str:
        if any(op in self.column for op in ['-', '+', '*', '/', '(', ')']):
            left = self.column
        else:
            left = ctx.col(self.column)

        if self.other_column:
            right = ctx.col(self.other_column)
        else:
            right = ctx.param(self.value)

        return f"({left} {self.operator} {right})"



class Num:
    """Numeric predicate factory."""

    @staticmethod
    def gt(col, val): return NumericPredicate(col, ">", val)

    @staticmethod
    def gte(col, val): return NumericPredicate(col, ">=", val)

    @staticmethod
    def lt(col, val): return NumericPredicate(col, "<", val)

    @staticmethod
    def lte(col, val): return NumericPredicate(col, "<=", val)

    @staticmethod
    def eq(col, val): return NumericPredicate(col, "=", val)

    @staticmethod
    def ne(col, val): return NumericPredicate(col, "!=", val)

    @staticmethod
    def between(col, low, high):
        return (
            NumericPredicate(col, ">=", low)
            & NumericPredicate(col, "<=", high)
        )

    @staticmethod
    def outside(col, low, high):
        return (
            NumericPredicate(col, "<", low)
            | NumericPredicate(col, ">", high)
        )

    @staticmethod
    def gt_col(col1, col2):
        return NumericPredicate(col1, ">", other_column=col2)

    @staticmethod
    def lt_col(col1, col2):
        return NumericPredicate(col1, "<", other_column=col2)

    @staticmethod
    def delta_gt(col1, col2, threshold):
        return NumericPredicate(
            f"({col1} - {col2})", ">", threshold
        )


# ============================================================================
# CATEGORICAL PREDICATE - COMPLETE IMPLEMENTATION
# ============================================================================

class CategoricalPredicate(Predicate):
    """
    Atomic categorical (TEXT) predicate.
    Supports: equality, membership, pattern matching, regex, NULL handling.
    """

    def __init__(
        self,
        column: str,
        operator: str,
        value: Any = None,
        values: Optional[List[Any]] = None,
        case_sensitive: bool = True,
        is_null_check: bool = False,
    ):
        self.column = column
        self.operator = operator
        self.value = value
        self.values = values
        self.case_sensitive = case_sensitive
        self.is_null_check = is_null_check

    def compile(self, ctx: "SQLContext") -> str:
        col = ctx.col(self.column)
        is_ch = getattr(ctx, "is_clickhouse", False)

        # ---------------------------
        # NULL checks (no parameters)
        # ---------------------------
        if self.is_null_check:
            return f"({col} {self.operator})"

        # ---------------------------
        # IN / NOT IN (multiple values)
        # ---------------------------
        if self.values is not None:
            placeholders = [ctx.param(v) for v in self.values]
            return f"({col} {self.operator} ({', '.join(placeholders)}))"

        # ===========================================================
        # ClickHouse-specific operators
        # ===========================================================
        if is_ch:
            # --- Regex ---
            if self.operator in ("~", "~*"):
                pattern = self.value
                if self.operator == "~*" and isinstance(pattern, str):
                    pattern = f"(?i){pattern}"
                val = ctx.param(pattern)
                return f"match({col}, {val})"

            if self.operator in ("!~", "!~*"):
                pattern = self.value
                if self.operator == "!~*" and isinstance(pattern, str):
                    pattern = f"(?i){pattern}"
                val = ctx.param(pattern)
                return f"NOT match({col}, {val})"

            # --- Case-insensitive LIKE (ILIKE equivalent) ---
            if self.operator == "ILIKE" or (
                not self.case_sensitive and self.operator == "LIKE"
            ):
                val = ctx.param(self.value)
                return f"(lower({col}) LIKE lower({val}))"

        # ===========================================================
        # PostgreSQL / DuckDB operators
        # ===========================================================
        # ILIKE (PostgreSQL)
        if self.operator == "ILIKE":
            val = ctx.param(self.value)
            return f"({col} ILIKE {val})"

        # Case-insensitive LIKE → ILIKE
        if not self.case_sensitive and self.operator == "LIKE":
            val = ctx.param(self.value)
            return f"({col} ILIKE {val})"

        # Case-insensitive regex → ~*
        if not self.case_sensitive and self.operator in ("~", "~*"):
            val = ctx.param(self.value)
            return f"({col} ~* {val})"

        # ---------------------------
        # Standard comparison (works for all backends)
        # ---------------------------
        val = ctx.param(self.value)
        return f"({col} {self.operator} {val})"

    def __repr__(self) -> str:
        ctx = SQLContext()
        sql = self.compile(ctx)
        params = ctx.params
        return f"SQL: {sql}\nParams: {params}"

class Cat:
    """Categorical (TEXT) predicate factory."""

    # ---------------------------
    # Equality & Inequality
    # ---------------------------
    @staticmethod
    def eq(col, value):
        """Exact match: column = value"""
        return CategoricalPredicate(col, "=", value)

    @staticmethod
    def ne(col, value):
        """Not equal: column != value"""
        return CategoricalPredicate(col, "!=", value)

    # ---------------------------
    # Membership (IN / NOT IN)
    # ---------------------------
    @staticmethod
    def in_(col, values: List[Any]):
        """Column IN (val1, val2, val3, ...)"""
        return CategoricalPredicate(col, "IN", values=values)

    @staticmethod
    def not_in(col, values: List[Any]):
        """Column NOT IN (val1, val2, val3, ...)"""
        return CategoricalPredicate(col, "NOT IN", values=values)

    # ---------------------------
    # Pattern Matching (LIKE / ILIKE)
    # ---------------------------
    @staticmethod
    def contains(col, text, case_sensitive=True):
        """Pattern: %text% (case_sensitive controls LIKE vs ILIKE)"""
        return CategoricalPredicate(
            col,
            "LIKE",
            f"%{text}%",
            case_sensitive=case_sensitive,
        )

    @staticmethod
    def starts_with(col, text, case_sensitive=True):
        """Pattern: text% """
        return CategoricalPredicate(
            col,
            "LIKE",
            f"{text}%",
            case_sensitive=case_sensitive,
        )

    @staticmethod
    def ends_with(col, text, case_sensitive=True):
        """Pattern: %text"""
        return CategoricalPredicate(
            col,
            "LIKE",
            f"%{text}",
            case_sensitive=case_sensitive,
        )

    @staticmethod
    def ilike(col, pattern):
        """Case-insensitive LIKE (PostgreSQL ILIKE operator)"""
        return CategoricalPredicate(col, "ILIKE", pattern, case_sensitive=False)

    # ---------------------------
    # Regex (PostgreSQL)
    # ---------------------------
    @staticmethod
    def regex(col, pattern, case_sensitive=True):
        """PostgreSQL regex: ~ (case-sensitive) or ~* (case-insensitive)"""
        op = "~" if case_sensitive else "~*"
        return CategoricalPredicate(col, op, pattern, case_sensitive=case_sensitive)

    @staticmethod
    def not_regex(col, pattern, case_sensitive=True):
        """PostgreSQL NOT regex: !~ or !~*"""
        op = "!~" if case_sensitive else "!~*"
        return CategoricalPredicate(col, op, pattern, case_sensitive=case_sensitive)

    # ---------------------------
    # NULL handling
    # ---------------------------
    @staticmethod
    def is_null(col):
        """Column IS NULL"""
        return CategoricalPredicate(col, "IS NULL", is_null_check=True)

    @staticmethod
    def not_null(col):
        """Column IS NOT NULL"""
        return CategoricalPredicate(col, "IS NOT NULL", is_null_check=True)

    # ---------------------------
    # Length-based filters (string length)
    # ---------------------------
    @staticmethod
    def length_gt(col, length: int):
        """String length > N: LENGTH(column) > N"""
        return NumericPredicate(f"LENGTH({col})", ">", length)

    @staticmethod
    def length_lt(col, length: int):
        """String length < N"""
        return NumericPredicate(f"LENGTH({col})", "<", length)

    @staticmethod
    def length_eq(col, length: int):
        """String length = N (fixed length strings)"""
        return NumericPredicate(f"LENGTH({col})", "=", length)

    # ---------------------------
    # Additional pattern helpers
    # ---------------------------
    @staticmethod
    def matches_exact(col, value, case_sensitive=True):
        """Alias for eq() - exact match"""
        if case_sensitive:
            return Cat.eq(col, value)
        else:
            return CategoricalPredicate(
                col, "=", value, case_sensitive=False
            )

    @staticmethod
    def one_of(col, values: List[Any]):
        """Alias for in_()"""
        return Cat.in_(col, values)

    @staticmethod
    def not_one_of(col, values: List[Any]):
        """Alias for not_in()"""
        return Cat.not_in(col, values)

    # ---------------------------
    # Advanced: Multiple conditions
    # ---------------------------
    @staticmethod
    def matches_any_pattern(col, patterns: List[str], case_sensitive=True):
        """Match any of multiple patterns: (LIKE pattern1) OR (LIKE pattern2) OR ..."""
        if not patterns:
            raise ValueError("patterns list cannot be empty")

        predicates = [
            Cat.contains(col, pattern, case_sensitive=case_sensitive)
            for pattern in patterns
        ]

        result = predicates[0]
        for pred in predicates[1:]:
            result = result | pred
        return result



# ClickHouse uses functions instead of EXTRACT for some fields
_CH_EXTRACT_MAP = {
    "YEAR": "toYear",
    "MONTH": "toMonth",
    "DAY": "toDayOfMonth",
    "HOUR": "toHour",
    "MINUTE": "toMinute",
    "SECOND": "toSecond",
    "DOW": "toDayOfWeek",
}

class DatetimePredicate(Predicate):
    """
    Atomic DATE / TIMESTAMP predicate.
    Supports: absolute comparisons, ranges, relative time,
              date extraction, timezone handling.
    """

    def __init__(
        self,
        column: str,
        operator: str,
        value: Any = None,
        value2: Any = None,
        relative: bool = False,
        interval: Optional[str] = None,
        extract_field: Optional[str] = None,
        timezone: Optional[str] = None,
    ):
        self.column = column
        self.operator = operator
        self.value = self._parse_datetime(value) if value is not None else None
        self.value2 = self._parse_datetime(value2) if value2 is not None else None
        self.relative = relative
        self.interval = interval
        self.extract_field = extract_field
        self.timezone = timezone

    @staticmethod
    def _parse_datetime(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, (int, float)):
            return value
        if isinstance(value, str):
            try:
                if len(value) == 19:
                    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                elif len(value) == 10:
                    return datetime.strptime(value, "%Y-%m-%d")
            except ValueError as e:
                raise ValueError(f"Invalid datetime format: '{value}'") from e
        return value

    def _clickhouse_interval(self) -> str:
        """
        Convert interval stored as '5 days' → '5 DAY' for ClickHouse.
        ClickHouse expects: INTERVAL <number> <SINGULAR_UNIT>
        """
        parts = self.interval.strip().split()
        if len(parts) != 2:
            raise ValueError(f"Invalid interval format: {self.interval}")
        num, unit = parts
        ch_unit = unit.rstrip("s").upper()          # days→DAY, hours→HOUR
        if ch_unit not in ("DAY", "HOUR", "MINUTE", "SECOND", "MONTH", "YEAR"):
            ch_unit = unit.upper()                    # fallback
        return f"{num} {ch_unit}"

    def compile(self, ctx: "SQLContext") -> str:
        col = ctx.col(self.column)
        is_duckdb = getattr(ctx, "is_duckdb", False)
        is_clickhouse = getattr(ctx, "is_clickhouse", False)

        # ----------------------------------------------------------
        # Cast – DuckDB only (ClickHouse / PG handle types natively)
        # ----------------------------------------------------------
        if is_duckdb:
            col = f"CAST({col} AS TIMESTAMPTZ)"

        # ----------------------------------------------------------
        # Timezone conversion
        # ----------------------------------------------------------
        if self.timezone:
            if is_clickhouse:
                col = f"toTimezone({col}, '{self.timezone}')"
            else:
                col = f"({col} AT TIME ZONE '{self.timezone}')"

        # ----------------------------------------------------------
        # EXTRACT field
        # ----------------------------------------------------------
        if self.extract_field:
            if is_clickhouse:
                ch_func = _CH_EXTRACT_MAP.get(self.extract_field)
                if ch_func:
                    if self.extract_field == "DOW":
                        # toDayOfWeek returns 1-7 (Mon=1, Sun=7).
                        # PG EXTRACT(DOW FROM ...) returns 0-6 (Sun=0).
                        # Modulo 7 aligns them: Sun=7%7=0, Mon=1, …, Sat=6
                        col = f"({ch_func}({col}) % 7)"
                    else:
                        col = f"{ch_func}({col})"
                else:
                    col = f"EXTRACT({self.extract_field} FROM {col})"
            else:
                col = f"EXTRACT({self.extract_field} FROM {col})"

            v = ctx.param(self.value)
            return f"({col} {self.operator} {v})"

        # ----------------------------------------------------------
        # Relative time (NOW - INTERVAL)
        # ----------------------------------------------------------
        if self.relative:
            if is_clickhouse:
                ch_interval = self._clickhouse_interval()
                return f"({col} {self.operator} now() - INTERVAL {ch_interval})"
            else:
                return f"({col} {self.operator} NOW() - INTERVAL '{self.interval}')"

        # ----------------------------------------------------------
        # BETWEEN
        # ----------------------------------------------------------
        if self.operator == "BETWEEN":
            v1, v2 = ctx.param(self.value), ctx.param(self.value2)
            return f"({col} BETWEEN {v1} AND {v2})"

        # ----------------------------------------------------------
        # Standard comparison
        # ----------------------------------------------------------
        v = ctx.param(self.value)
        return f"({col} {self.operator} {v})"

    def __repr__(self) -> str:
        ctx = SQLContext()
        sql = self.compile(ctx)
        params = ctx.params
        param_types = [type(p).__name__ for p in params]
        return f"SQL: {sql}\nParams: {params}\nTypes: {param_types}"    
    


class Time:
    """Datetime predicate factory (DATE / TIMESTAMP)."""

    # ----------------------------
    # Absolute comparisons
    # ----------------------------
    @staticmethod
    def before(col, ts):
        return DatetimePredicate(col, "<", ts)

    @staticmethod
    def after(col, ts):
        return DatetimePredicate(col, ">", ts)

    @staticmethod
    def on_or_before(col, ts):
        return DatetimePredicate(col, "<=", ts)

    @staticmethod
    def on_or_after(col, ts):
        return DatetimePredicate(col, ">=", ts)

    @staticmethod
    def eq(col, ts):
        return DatetimePredicate(col, "=", ts)

    @staticmethod
    def ne(col, ts):
        return DatetimePredicate(col, "!=", ts)

    # ----------------------------
    # Ranges
    # ----------------------------
    @staticmethod
    def between(col, start, end):
        return DatetimePredicate(col, "BETWEEN", start, end)

    @staticmethod
    def outside(col, start, end):
        return (
            DatetimePredicate(col, "<", start)
            | DatetimePredicate(col, ">", end)
        )

    # ----------------------------
    # Relative time
    # ----------------------------
    @staticmethod
    def last_days(col, n):
        return DatetimePredicate(col, ">=", relative=True, interval=f"{n} days")

    @staticmethod
    def last_hours(col, n):
        return DatetimePredicate(col, ">=", relative=True, interval=f"{n} hours")

    @staticmethod
    def last_minutes(col, n):
        return DatetimePredicate(col, ">=", relative=True, interval=f"{n} minutes")

    @staticmethod
    def last_seconds(col, n):
        return DatetimePredicate(col, ">=", relative=True, interval=f"{n} seconds")

    @staticmethod
    def last_months(col, n):
        return DatetimePredicate(col, ">=", relative=True, interval=f"{n} months")

    @staticmethod
    def last_years(col, n):
        return DatetimePredicate(col, ">=", relative=True, interval=f"{n} years")

    @staticmethod
    def next_days(col, n):
        return DatetimePredicate(col, "<=", relative=True, interval=f"-{n} days")

    # ----------------------------
    # YEAR
    # ----------------------------
    @staticmethod
    def year_eq(col, v): return DatetimePredicate(col, "=", v, extract_field="YEAR")
    @staticmethod
    def year_lt(col, v): return DatetimePredicate(col, "<", v, extract_field="YEAR")
    @staticmethod
    def year_lte(col, v): return DatetimePredicate(col, "<=", v, extract_field="YEAR")
    @staticmethod
    def year_gt(col, v): return DatetimePredicate(col, ">", v, extract_field="YEAR")
    @staticmethod
    def year_gte(col, v): return DatetimePredicate(col, ">=", v, extract_field="YEAR")
    @staticmethod
    def year_ne(col, v): return DatetimePredicate(col, "!=", v, extract_field="YEAR")

    # ----------------------------
    # MONTH
    # ----------------------------
    @staticmethod
    def month_eq(col, v): return DatetimePredicate(col, "=", v, extract_field="MONTH")
    @staticmethod
    def month_lt(col, v): return DatetimePredicate(col, "<", v, extract_field="MONTH")
    @staticmethod
    def month_lte(col, v): return DatetimePredicate(col, "<=", v, extract_field="MONTH")
    @staticmethod
    def month_gt(col, v): return DatetimePredicate(col, ">", v, extract_field="MONTH")
    @staticmethod
    def month_gte(col, v): return DatetimePredicate(col, ">=", v, extract_field="MONTH")
    @staticmethod
    def month_ne(col, v): return DatetimePredicate(col, "!=", v, extract_field="MONTH")

    @staticmethod
    def month_between(col, start_month: int, end_month: int):
        """EXTRACT(MONTH FROM timestamp) BETWEEN start AND end"""
        print(f"Time.month_between('{col}', {start_month}, {end_month})")
        return (
            DatetimePredicate(col, ">=", start_month, extract_field="MONTH")
            & DatetimePredicate(col, "<=", end_month, extract_field="MONTH")
        )
    # ----------------------------
    # DAY
    # ----------------------------
    @staticmethod
    def first_days(col, n: int):
        """First N days (past lookback)"""
        print(f"Time.first_days('{col}', {n})")
        return DatetimePredicate(col, "<=", relative=True, interval=f"{n} days")
    @staticmethod
    def day_eq(col, v): return DatetimePredicate(col, "=", v, extract_field="DAY")
    @staticmethod
    def day_lt(col, v): return DatetimePredicate(col, "<", v, extract_field="DAY")
    @staticmethod
    def day_lte(col, v): return DatetimePredicate(col, "<=", v, extract_field="DAY")
    @staticmethod
    def day_gt(col, v): return DatetimePredicate(col, ">", v, extract_field="DAY")
    @staticmethod
    def day_gte(col, v): return DatetimePredicate(col, ">=", v, extract_field="DAY")
    @staticmethod
    def day_ne(col, v): return DatetimePredicate(col, "!=", v, extract_field="DAY")

    @staticmethod
    def day_between(col, a, b):
        return (
            DatetimePredicate(col, ">=", a, extract_field="DAY")
            & DatetimePredicate(col, "<=", b, extract_field="DAY")
        )

    # ----------------------------
    # HOUR
    # ----------------------------
    @staticmethod
    def hour_eq(col, v): return DatetimePredicate(col, "=", v, extract_field="HOUR")
    @staticmethod
    def hour_lt(col, v): return DatetimePredicate(col, "<", v, extract_field="HOUR")
    @staticmethod
    def hour_lte(col, v): return DatetimePredicate(col, "<=", v, extract_field="HOUR")
    @staticmethod
    def hour_gt(col, v): return DatetimePredicate(col, ">", v, extract_field="HOUR")
    @staticmethod
    def hour_gte(col, v): return DatetimePredicate(col, ">=", v, extract_field="HOUR")
    @staticmethod
    def hour_ne(col, v): return DatetimePredicate(col, "!=", v, extract_field="HOUR")

    @staticmethod
    def hour_between(col, a, b):
        return (
            DatetimePredicate(col, ">=", a, extract_field="HOUR")
            & DatetimePredicate(col, "<=", b, extract_field="HOUR")
        )

    # ----------------------------
    # MINUTE
    # ----------------------------
    @staticmethod
    def minute_eq(col, v): return DatetimePredicate(col, "=", v, extract_field="MINUTE")
    @staticmethod
    def minute_lt(col, v): return DatetimePredicate(col, "<", v, extract_field="MINUTE")
    @staticmethod
    def minute_lte(col, v): return DatetimePredicate(col, "<=", v, extract_field="MINUTE")
    @staticmethod
    def minute_gt(col, v): return DatetimePredicate(col, ">", v, extract_field="MINUTE")
    @staticmethod
    def minute_gte(col, v): return DatetimePredicate(col, ">=", v, extract_field="MINUTE")
    @staticmethod
    def minute_ne(col, v): return DatetimePredicate(col, "!=", v, extract_field="MINUTE")

    # ----------------------------
    # SECOND
    # ----------------------------
    @staticmethod
    def second_eq(col, v): return DatetimePredicate(col, "=", v, extract_field="SECOND")
    @staticmethod
    def second_lt(col, v): return DatetimePredicate(col, "<", v, extract_field="SECOND")
    @staticmethod
    def second_lte(col, v): return DatetimePredicate(col, "<=", v, extract_field="SECOND")
    @staticmethod
    def second_gt(col, v): return DatetimePredicate(col, ">", v, extract_field="SECOND")
    @staticmethod
    def second_gte(col, v): return DatetimePredicate(col, ">=", v, extract_field="SECOND")
    @staticmethod
    def second_ne(col, v): return DatetimePredicate(col, "!=", v, extract_field="SECOND")

    # ----------------------------
    # NULL checks
    # ----------------------------
    @staticmethod
    def is_null(col):
        return DatetimePredicate(col, "IS NULL")

    @staticmethod
    def not_null(col):
        return DatetimePredicate(col, "IS NOT NULL")

    # ----------------------------
    # Timezone-aware (FINAL versions kept)
    # ----------------------------
    @staticmethod
    def before_tz(col, ts, timezone):
        return DatetimePredicate(col, "<", ts, timezone=timezone)

    @staticmethod
    def after_tz(col, ts, timezone):
        return DatetimePredicate(col, ">", ts, timezone=timezone)

    @staticmethod
    def on_or_before_tz(col, ts, timezone):
        return DatetimePredicate(col, "<=", ts, timezone=timezone)

    @staticmethod
    def on_or_after_tz(col, ts, timezone):
        return DatetimePredicate(col, ">=", ts, timezone=timezone)

    @staticmethod
    def eq_tz(col, ts, timezone):
        return DatetimePredicate(col, "=", ts, timezone=timezone)

    @staticmethod
    def between_tz(col, start, end, timezone):
        pred = DatetimePredicate(col, "BETWEEN", start, end)
        pred.timezone = timezone
        return pred


    @staticmethod
    def is_business_hours(col, start_hour=9, end_hour=17):
        """EXTRACT(HOUR FROM timestamp) BETWEEN start_hour AND end_hour"""
        print(f"Time.is_business_hours('{col}', {start_hour}, {end_hour})")
        return Time.hour_between(col, start_hour, end_hour)

    @staticmethod
    def is_off_hours(col, start_hour=9, end_hour=17):
        """NOT business hours"""
        print(f"Time.is_off_hours('{col}', {start_hour}, {end_hour})")
        return ~Time.is_business_hours(col, start_hour, end_hour)
    # ----------------------------
    # Day of week helpers
    # ----------------------------
    @staticmethod
    def dow_eq(col, dow: int):
        """EXTRACT(DOW FROM timestamp) = dow (0=Sunday, 6=Saturday)"""
        print(f"Time.dow_eq('{col}', {dow})")
        return DatetimePredicate(col, "=", dow, extract_field="DOW")

    @staticmethod
    def is_weekday(col):
        """DOW NOT IN (0, 6) - Mon to Fri"""
        print(f"Time.is_weekday('{col}')")
        return (
            ~DatetimePredicate(col, "=", 0, extract_field="DOW")
            & ~DatetimePredicate(col, "=", 6, extract_field="DOW")
        )

    @staticmethod
    def is_weekend(col):
        """DOW IN (0, 6) - Sat or Sun"""
        print(f"Time.is_weekend('{col}')")
        return (
            DatetimePredicate(col, "=", 0, extract_field="DOW")
            | DatetimePredicate(col, "=", 6, extract_field="DOW")
        )

    @staticmethod
    def is_monday(col):
        """DOW = 1"""
        print(f"Time.is_monday('{col}')")
        return DatetimePredicate(col, "=", 1, extract_field="DOW")

    @staticmethod
    def is_friday(col):
        """DOW = 5"""
        print(f"Time.is_friday('{col}')")
        return DatetimePredicate(col, "=", 5, extract_field="DOW")

