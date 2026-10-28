"""Small, quote/parenthesis-aware ClickHouse CREATE TABLE clause rewriter.

Used by ``ClickHouseIndexOps.set_order_by`` to rebuild a table with a new
ORDER BY while preserving engine, partitioning, TTL, settings, skip indexes,
projections, comments, and column definitions.
"""

from __future__ import annotations

import re
from typing import List, Optional, Tuple

_CLAUSES = (
    "ENGINE",
    "PARTITION BY",
    "PRIMARY KEY",
    "ORDER BY",
    "SAMPLE BY",
    "TTL",
    "SETTINGS",
    "COMMENT",
)


def _is_word_char(ch: str) -> bool:
    return ch.isalnum() or ch == "_"


def _clause_positions(sql: str) -> List[Tuple[int, str]]:
    """Return top-level clause keyword positions (outside quotes/parens)."""
    positions: List[Tuple[int, str]] = []
    i = 0
    depth = 0
    quote: Optional[str] = None
    n = len(sql)
    while i < n:
        ch = sql[i]
        if quote is not None:
            if ch == "\\":
                i += 2
                continue
            if ch == quote:
                if i + 1 < n and sql[i + 1] == quote:
                    i += 2
                    continue
                quote = None
            i += 1
            continue
        if ch in ("'", '"', "`"):
            quote = ch
            i += 1
            continue
        if ch == "(":
            depth += 1
            i += 1
            continue
        if ch == ")":
            depth = max(0, depth - 1)
            i += 1
            continue
        if depth == 0 and (i == 0 or not _is_word_char(sql[i - 1])):
            upper = sql[i:].upper()
            for clause in _CLAUSES:
                if upper.startswith(clause):
                    end = i + len(clause)
                    if end == n or not _is_word_char(sql[end]):
                        positions.append((i, clause))
                        break
        i += 1
    return positions


def _find(positions: List[Tuple[int, str]], clause: str) -> Optional[int]:
    for index, (pos, found) in enumerate(positions):
        if found == clause:
            return index
    return None


def _replace_clause(
    sql: str, positions: List[Tuple[int, str]], clause: str, replacement: str
) -> str:
    idx = _find(positions, clause)
    if idx is None:
        raise ValueError(f"{clause} clause not found")
    start = positions[idx][0]
    end = positions[idx + 1][0] if idx + 1 < len(positions) else len(sql)
    prefix = sql[:start]
    suffix = sql[end:]
    if prefix and not prefix.endswith((" ", "\n", "\t")):
        prefix += " "
    return prefix + replacement + (" " + suffix.lstrip() if suffix.strip() else "")


def _remove_clause(sql: str, positions: List[Tuple[int, str]], clause: str) -> str:
    idx = _find(positions, clause)
    if idx is None:
        return sql
    start = positions[idx][0]
    end = positions[idx + 1][0] if idx + 1 < len(positions) else len(sql)
    prefix = sql[:start].rstrip()
    suffix = sql[end:].lstrip()
    return prefix + (" " + suffix if suffix else "")


def rewrite_order_by(
    ddl: str,
    order_by_sql: str,
    primary_key_sql: Optional[str] = None,
) -> str:
    """Return ``ddl`` with ORDER BY replaced and PRIMARY KEY replaced/removed.

    ``order_by_sql`` / ``primary_key_sql`` are complete expressions such as
    ``("tenant_id", "event_date")``.
    """
    positions = _clause_positions(ddl)
    if _find(positions, "ORDER BY") is None:
        raise ValueError("ClickHouse CREATE TABLE has no ORDER BY clause")

    if primary_key_sql is not None:
        if _find(positions, "PRIMARY KEY") is not None:
            ddl = _replace_clause(ddl, positions, "PRIMARY KEY", f"PRIMARY KEY {primary_key_sql}")
        else:
            # Insert directly after ORDER BY; rebuild positions first.
            ddl = _replace_clause(
                ddl, positions, "ORDER BY",
                f"ORDER BY {order_by_sql} PRIMARY KEY {primary_key_sql}",
            )
            return ddl
    elif _find(positions, "PRIMARY KEY") is not None:
        ddl = _remove_clause(ddl, positions, "PRIMARY KEY")

    positions = _clause_positions(ddl)
    ddl = _replace_clause(ddl, positions, "ORDER BY", f"ORDER BY {order_by_sql}")
    return ddl


def ensure_setting(ddl: str, key: str, value: str) -> str:
    """Set a top-level ClickHouse table setting, preserving other settings."""
    positions = _clause_positions(ddl)
    idx = _find(positions, "SETTINGS")
    if idx is None:
        return ddl.rstrip() + f" SETTINGS {key} = {value}"
    start = positions[idx][0]
    end = positions[idx + 1][0] if idx + 1 < len(positions) else len(ddl)
    clause = ddl[start:end]
    pattern = re.compile(rf"\b{re.escape(key)}\s*=\s*[^,\s]+", re.IGNORECASE)
    if pattern.search(clause):
        clause = pattern.sub(f"{key} = {value}", clause, count=1)
    else:
        clause = clause.rstrip() + f", {key} = {value} "
    return ddl[:start] + clause + ddl[end:]


def replace_table_name(ddl: str, new_qualified: str) -> str:
    """Replace the table reference in a CREATE TABLE header."""
    pattern = re.compile(
        r"^(CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?)"
        r"(?:`[^`]+`|[^\s(]+)(?:\.(?:`[^`]+`|[^\s(]+))?",
        re.IGNORECASE,
    )
    if not pattern.match(ddl):
        raise ValueError("Could not parse CREATE TABLE header")
    return pattern.sub(lambda match: match.group(1) + new_qualified, ddl, count=1)
