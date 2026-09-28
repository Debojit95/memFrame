"""
Parser for compare expressions.
Expect a string like "col1 >= col2", with optional whitespace.
Returns (col1, operator, col2).
"""

from typing import Tuple


class CompareParseError(Exception):
    pass


# -----------------------------------------------------------------
#  Tokenizer – identical to the one in str_filter_parser.py
# -----------------------------------------------------------------
def _tokenize(expr: str):
    tokens = []
    i = 0
    n = len(expr)
    while i < n:
        ch = expr[i]
        if ch.isspace():
            i += 1
            continue
        # Quoted strings
        if ch in ("'", '"'):
            quote = ch
            j = i + 1
            while j < n and expr[j] != quote:
                j += 1
            if j >= n:
                raise CompareParseError(f"Unterminated string: {expr[i:]}")
            j += 1
            tokens.append(expr[i:j])
            i = j
            continue
        # Parentheses are not allowed in a simple compare
        if ch in "()":
            raise CompareParseError("Parentheses are not allowed in compare expression")
        # Two‑char operators (≥, ≤, ≠, ==)
        if i + 1 < n and expr[i:i+2] in (">=", "<=", "!=", "=="):
            tokens.append(expr[i:i+2])
            i += 2
            continue
        # Single‑char operators
        if ch in "><=":
            tokens.append(ch)
            i += 1
            continue
        # Word (column name)
        j = i
        while j < n and not expr[j].isspace() and expr[j] not in "()><='\"":
            if j+1 < n and expr[j:j+2] in (">=", "<=", "!=", "=="):
                break
            if expr[j] in "><=":
                break
            j += 1
        tokens.append(expr[i:j])
        i = j
    return tokens


# -----------------------------------------------------------------
#  Main parser
# -----------------------------------------------------------------
def parse_compare_expression(expr: str) -> Tuple[str, str, str]:
    tokens = _tokenize(expr)
    if len(tokens) != 3:
        raise CompareParseError(
            f"Expected exactly 3 tokens (left operator right), got {len(tokens)} tokens: {tokens}"
        )
    left, op, right = tokens

    # Strip quotes if the column name was quoted (rare, but supported)
    left = _strip_quotes(left)
    right = _strip_quotes(right)

    # Validate operator
    valid_ops = {">=", "<=", "!=", "==", ">", "<"}
    if op not in valid_ops:
        raise CompareParseError(f"Invalid operator '{op}'. Must be one of {valid_ops}")

    return left, op, right


def _strip_quotes(token: str) -> str:
    if (token.startswith("'") and token.endswith("'")) or \
       (token.startswith('"') and token.endswith('"')):
        return token[1:-1]
    return token