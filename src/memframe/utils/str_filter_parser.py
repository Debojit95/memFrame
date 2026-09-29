# File: utils/str_filter_parser.py

from typing import List, Optional
import re

from memframe.core.analytix.filter_I import (
    CategoricalPredicate,
    Predicate,
    NumericPredicate,
    Num,
    DatetimePredicate,
)
from memframe.core.analytix.datetime import DT_FIELD_MAP


class ParseError(Exception):
    pass


# ============================================================================
# PREPROCESSING — handle glued 'and'/'or' keywords
# ============================================================================

def preprocess_filter_expr(expr: str) -> str:
    """
    Normalize a filter expression by ensuring 'and'/'or' keywords
    are properly separated from adjacent tokens.

    Handles:
      "col2or("       → "col2 or ("
      "col5and b"     → "col5 and b"
      "(a>5)or(b<3)"  → "(a>5) or (b<3)"
      "4.7or depth"   → "4.7 or depth"

    Does NOT split:
      "color"  → stays "color"  (no digit/)/quote before 'or')
      "band"   → stays "band"   (no digit/)/quote before 'and')
    """
    # Insert space BEFORE 'and'/'or' when preceded by: digit, ), ', "
    expr = re.sub(r'(?<=[\d)\'"])(and|or)\b', r' \1', expr)

    # Insert space AFTER 'and'/'or' when followed by: letter, (, _
    expr = re.sub(r'\b(and|or)(?=[a-zA-Z_(])', r'\1 ', expr)

    # Clean up multiple spaces
    expr = re.sub(r'\s+', ' ', expr).strip()
    return expr


# ============================================================================
# TOKENIZER
# ============================================================================

def tokenize(expr: str) -> List[str]:
    """
    Split the expression string into tokens: operators, parentheses,
    quoted strings, and bare words/numbers.  Spaces are ignored.
    """
    tokens = []
    i = 0
    n = len(expr)
    while i < n:
        ch = expr[i]
        if ch.isspace():
            i += 1
            continue

        if ch in ("'", '"'):
            quote = ch
            j = i + 1
            while j < n and expr[j] != quote:
                j += 1
            if j >= n:
                raise ParseError(f"Unterminated string literal: {expr[i:]}")
            j += 1
            tokens.append(expr[i:j])   # keep quotes for later stripping
            i = j
            continue

        if ch in "()":
            tokens.append(ch)
            i += 1
            continue

        if i + 1 < n and expr[i:i+2] in ("&&", "||", ">=", "<=", "!=", "=="):
            tokens.append(expr[i:i+2])
            i += 2
            continue

        if ch in "><=":
            tokens.append(ch)
            i += 1
            continue

        j = i
        while j < n and not expr[j].isspace() and expr[j] not in "()><='\"":
            if j+1 < n and expr[j:j+2] in ("&&", "||", ">=", "<=", "!=", "=="):
                break
            if expr[j] in "><=":
                break
            j += 1
        tokens.append(expr[i:j])
        i = j

    return tokens


# ============================================================================
# PARSER
# ============================================================================

def parse_filter_string(expr: str) -> Predicate:
    expr = preprocess_filter_expr(expr)          # ← CHANGED: preprocess first
    tokens = tokenize(expr)
    if not tokens:
        raise ParseError("Empty filter expression")
    parsed, _ = _parse_or(tokens, 0)
    return parsed


def _parse_or(tokens, pos):
    left, pos = _parse_and(tokens, pos)
    while pos < len(tokens) and tokens[pos] in ("||", "or"):        # ← CHANGED
        pos += 1
        right, pos = _parse_and(tokens, pos)
        left = left | right
    return left, pos


def _parse_and(tokens, pos):
    left, pos = _parse_atom(tokens, pos)
    while pos < len(tokens) and tokens[pos] in ("&&", "and"):       # ← CHANGED
        pos += 1
        right, pos = _parse_atom(tokens, pos)
        left = left & right
    return left, pos


def _parse_atom(tokens, pos):
    if pos >= len(tokens):
        raise ParseError("Unexpected end of expression")
    token = tokens[pos]
    if token == "(":
        pos += 1
        sub, pos = _parse_or(tokens, pos)
        if pos >= len(tokens) or tokens[pos] != ")":
            raise ParseError("Missing closing parenthesis")
        pos += 1
        return sub, pos
    return _parse_comparison(tokens, pos)


def _parse_comparison(tokens, pos):
    if pos + 2 > len(tokens):
        raise ParseError(f"Incomplete comparison at token {pos}")
    left_raw = tokens[pos]
    op_raw = tokens[pos + 1]
    right_raw = tokens[pos + 2]
    pos += 3

    op = "==" if op_raw == "=" else op_raw
    if op not in (">", "<", ">=", "<=", "==", "!="):
        raise ParseError(f"Expected comparison operator, got '{op_raw}'")

    left = _strip_quotes(left_raw)
    right = _strip_quotes(right_raw)
    left_quoted = _is_quoted(left_raw)
    right_quoted = _is_quoted(right_raw)

    # .dt. extractions
    left_dt = _parse_dt_extract(left)
    right_dt = _parse_dt_extract(right)
    if left_dt or right_dt:
        return _build_dt_extract_predicate(left, op, right, left_dt, right_dt), pos

    # Date/timestamp literals
    if _is_date_literal(left) or _is_date_literal(right):
        return _build_datetime_predicate(left, op, right), pos

    # Numeric: at least one unquoted side parses as a number
    left_numeric_literal = not left_quoted and _is_numeric_literal(left)
    right_numeric_literal = not right_quoted and _is_numeric_literal(right)
    if left_numeric_literal or right_numeric_literal:
        return _build_numeric_predicate(left, op, right), pos

    # String / categorical: at least one side is a non-numeric string
    if _is_string_operand(left) or _is_string_operand(right):
        return _build_categorical_or_string_predicate(
            left, op, right, left_quoted, right_quoted
        ), pos

    # Fallback: both look like identifiers (column vs column)
    return _build_numeric_predicate(left, op, right), pos

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _parse_dt_extract(token: str) -> Optional[tuple[str, str]]:
    """If token looks like 'col_name.dt.field', return (col_name, SQL extract field)."""
    parts = token.split('.')
    if len(parts) == 3 and parts[1] == 'dt' and parts[2] in DT_FIELD_MAP:
        return parts[0], DT_FIELD_MAP[parts[2]]
    return None


def _build_dt_extract_predicate(
    left: str, op: str, right: str,
    left_dt: Optional[tuple[str, str]],
    right_dt: Optional[tuple[str, str]]
) -> Predicate:
    if left_dt and right_dt:
        raise ParseError("Cannot compare two .dt extractions directly")
    if left_dt:
        col, extract_field = left_dt
        value = _parse_literal(right)
        return DatetimePredicate(col, _op_map(op), value,
                                 extract_field=extract_field)
    else:   # right_dt
        col, extract_field = right_dt
        value = _parse_literal(left)
        swapped_op = _swap_operator(op)
        return DatetimePredicate(col, swapped_op, value,
                                 extract_field=extract_field)


def _op_map(op: str) -> str:
    return {"==": "=", "!=": "!=", ">": ">", "<": "<",
            ">=": ">=", "<=": "<="}[op]


def _swap_operator(op: str) -> str:
    swap = {">": "<", "<": ">", ">=": "<=", "<=": ">=", "=": "=", "!=": "!="}
    return swap[op]


def _parse_literal(token: str) -> int | float | str:
    try:
        return int(token)
    except ValueError:
        try:
            return float(token)
        except ValueError:
            return token


def _strip_quotes(token: str) -> str:
    if (token.startswith("'") and token.endswith("'")) or \
       (token.startswith('"') and token.endswith('"')):
        return token[1:-1]
    return token


def _is_quoted(token: str) -> bool:
    return (token.startswith("'") and token.endswith("'")) or \
           (token.startswith('"') and token.endswith('"'))


def _is_numeric_literal(val: str) -> bool:
    try:
        float(val)
        return True
    except ValueError:
        return False


def _is_string_operand(val: str) -> bool:
    """Return True if val cannot be converted to float (i.e., is a string)."""
    try:
        float(val)
        return False
    except ValueError:
        return True


def _is_date_literal(val: str) -> bool:
    """Check if val looks like a date 'YYYY-MM-DD' or timestamp 'YYYY-MM-DD HH:MM:SS'."""
    return bool(re.fullmatch(r'\d{4}-\d{2}-\d{2}', val)) or \
           bool(re.fullmatch(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', val))


def _build_datetime_predicate(left, op, right):
    op_map = {"==": "=", "!=": "!=", ">": ">", "<": "<", ">=": ">=", "<=": "<="}
    return DatetimePredicate(left, op_map[op], right)


def _build_numeric_predicate(left, op, right):
    try:
        r_val = float(right)
    except ValueError:
        r_val = right
    try:
        l_val = float(left)
    except ValueError:
        l_val = left

    if isinstance(l_val, (int, float)) and isinstance(r_val, (int, float)):
        if op == ">":
            return Num.gt(left, r_val)
        if op == "<":
            return Num.lt(left, r_val)
        if op == ">=":
            return Num.gte(left, r_val)
        if op == "<=":
            return Num.lte(left, r_val)
        if op == "==":
            return Num.eq(left, r_val)
        if op == "!=":
            return Num.ne(left, r_val)

    other_col = right if isinstance(r_val, str) else None
    value = None if isinstance(r_val, str) else r_val

    if op == ">":
        return NumericPredicate(left, ">", value=value, other_column=other_col)
    if op == "<":
        return NumericPredicate(left, "<", value=value, other_column=other_col)
    if op == ">=":
        return NumericPredicate(left, ">=", value=value, other_column=other_col)
    if op == "<=":
        return NumericPredicate(left, "<=", value=value, other_column=other_col)
    if op == "==":
        return NumericPredicate(left, "=", value=value, other_column=other_col)
    if op == "!=":
        return NumericPredicate(left, "!=", value=value, other_column=other_col)

    raise ParseError(f"Unsupported operator: {op}")


def _build_categorical_or_string_predicate(
    left: str,
    op: str,
    right: str,
    left_quoted: bool = False,
    right_quoted: bool = False,
) -> Predicate:
    """
    Build a predicate when at least one operand is a non-numeric string.

    Decides the correct predicate type based on quoting:
      - Quoted side = literal string value  (e.g. 'NYC')
      - Unquoted side = column name         (e.g. city)

    Cases handled:
      1. column  op  'value'   →  CategoricalPredicate(column, op, value)
      2. 'value' op  column    →  CategoricalPredicate(column, swapped_op, value)
      3. column  op  column    →  NumericPredicate(col, op, other_column=col2)
      4. 'val1'  op  'val2'    →  NumericPredicate(val1, op, value=val2)  (edge)
    """
    op_map = {"==": "=", "!=": "!=", ">": ">", "<": "<", ">=": ">=", "<=": "<="}
    sql_op = op_map.get(op, "=")

    # ------------------------------------------------------------------
    # Case 1: Right is quoted string value, left is column
    #   city == 'NYC'  →  CategoricalPredicate("city", "=", "NYC")
    # ------------------------------------------------------------------
    if right_quoted and not left_quoted:
        if sql_op in ("=", "!="):
            return CategoricalPredicate(left, sql_op, right)
        # > < >= <= on strings are valid SQL lexicographic comparisons
        return CategoricalPredicate(left, sql_op, right)

    # ------------------------------------------------------------------
    # Case 2: Left is quoted string value, right is column
    #   'NYC' == city  →  CategoricalPredicate("city", "=", "NYC")
    #   'NYC' != city  →  CategoricalPredicate("city", "!=", "NYC")
    #   'NYC' <  city  →  CategoricalPredicate("city", ">", "NYC")  (swap)
    # ------------------------------------------------------------------
    if left_quoted and not right_quoted:
        # For = and !=, order doesn't matter
        if sql_op in ("=", "!="):
            return CategoricalPredicate(right, sql_op, left)
        # For ordering operators, swap to keep column on the left
        swapped_op = _swap_operator(op)
        sql_op_swapped = op_map.get(swapped_op, "=")
        return CategoricalPredicate(right, sql_op_swapped, left)

    # ------------------------------------------------------------------
    # Case 3: Both unquoted → column vs column comparison
    #   col1 > col2  →  NumericPredicate("col1", ">", other_column="col2")
    #   status == category  →  NumericPredicate("status", "=", other_column="category")
    # ------------------------------------------------------------------
    if not left_quoted and not right_quoted:
        return NumericPredicate(left, sql_op, other_column=right)

    # ------------------------------------------------------------------
    # Case 4: Both quoted → literal vs literal (rare edge case)
    #   'alpha' < 'beta'  →  NumericPredicate("alpha", "<", value="beta")
    # ------------------------------------------------------------------
    return NumericPredicate(left, sql_op, value=right)