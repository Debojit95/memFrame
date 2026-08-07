from memframe.db_manager.adapters.base import DatabaseAdapter
from memframe.utils.helper import SQLIdentifierSanitizer

_NUMERIC = ("int", "float", "double", "decimal", "numeric", "real", "bigint", "smallint")
_DATETIME = ("date", "timestamp", "datetime")
_UNIQUE_CAP = 50
_PREVIEW_ROWS = 5
_PREVIEW_COLS = 10
_CELL_CAP = 24


def _dtype_family(dtype: str) -> str:
    d = dtype.lower()
    if any(t in d for t in _NUMERIC):
        return "numeric"
    if any(t in d for t in _DATETIME):
        return "datetime"
    return "categorical"


async def _qualified(adapter: DatabaseAdapter, table: str, schema: str) -> str:
    return f"{adapter.quote_identifier(schema)}.{adapter.quote_identifier(table)}"


async def build_domain_context(session, lightweight: bool = False) -> str:
    """Build a per-table domain context block for the analytics agents.
    
    Args:
        session: The session object containing adapter, table, schema info
        lightweight: If True, returns minimal context (column names + types only).
                    If False, returns full context with profiles and sample data.
    """
    await session.ensure()
    adapter = session.adapter
    table = SQLIdentifierSanitizer.sanitize(session.table)
    schema = SQLIdentifierSanitizer.sanitize(session.schema)
    qual = await _qualified(adapter, table, schema)

    col_types = await adapter.get_column_types(table, schema)
    info = (await adapter.get_table_info(table, schema)) or {}
    rows = info.get("row_count", 0)
    cols = info.get("column_count", len(col_types))

    lines = [f"ACTIVE TABLE CONTEXT: {table} ({rows} rows x {cols} columns)"]
    lines.append("Columns: " + ", ".join(f"{col} [{_dtype_family(dtype)}]" for col, dtype in col_types.items()))
    
    if lightweight:
        return "\n".join(lines)

    lines.append("")
    lines.append("Column profiles:")

    for col, dtype in col_types.items():
        fam = _dtype_family(dtype)
        q = f'"{SQLIdentifierSanitizer.sanitize(col)}"'
        col_key = SQLIdentifierSanitizer.sanitize(col)

        if fam == "categorical":
            res = await adapter.fetch(
                f"SELECT DISTINCT {q} FROM {qual} LIMIT {_UNIQUE_CAP}"
            )
            vals = []
            for r in res:
                v = dict(r).get(col_key)
                if v is not None and str(v).strip() != "":
                    vals.append(str(v))
            shown = len(vals)
            note = f" ({shown} of N shown)" if shown >= _UNIQUE_CAP else ""
            lines.append(
                f"{col} [categorical]: UNIQUE VALUES {sorted(vals)}{note}; SQL type {dtype.upper()}"
            )
        else:
            row = await adapter.fetchrow(
                f"SELECT MIN({q}) AS mn, MAX({q}) AS mx, "
                f"COUNT(*) AS total, COUNT({q}) AS nn FROM {qual}"
            )
            mn = "n/a" if row["mn"] is None else str(row["mn"])
            mx = "n/a" if row["mx"] is None else str(row["mx"])
            nulls = int(row["total"]) - int(row["nn"])
            null_note = f"; ({nulls} nulls)" if nulls else ""
            if fam == "datetime":
                has_time = "time" in dtype.lower() or "timestamp" in dtype.lower()
                fmt = "%Y-%m-%d %H:%M:%S" if has_time else "%Y-%m-%d"
                lines.append(
                    f"{col} [datetime]: RANGE {mn} to {mx}; format {fmt}; "
                    f"SQL type {dtype.upper()}{null_note}"
                )
            else:
                lines.append(
                    f"{col} [numeric]: RANGE {mn} to {mx}; SQL type {dtype.upper()}{null_note}"
                )

    lines.append("")
    lines.extend(await _preview_lines(adapter, qual, table, schema))
    return "\n".join(lines)


def _cell(value) -> str:
    s = "None" if value is None else str(value)
    s = " ".join(s.split())
    return s if len(s) <= _CELL_CAP else s[:_CELL_CAP - 1] + "\u2026"


async def _preview_lines(adapter, qual: str, table: str, schema: str) -> list[str]:
    cols = await adapter.get_column_types(table, schema)
    names = list(cols.keys())[:_PREVIEW_COLS]
    rows = await adapter.fetch(f"SELECT * FROM {qual} LIMIT {_PREVIEW_ROWS}")

    def line(cells: list[str]) -> str:
        return "| " + " | ".join(f"{c:<{_CELL_CAP}}" for c in cells) + " |"

    header = line(names)
    rule = "|" + "|".join("-" * (_CELL_CAP + 2) for _ in names) + "|"
    body = []
    for r in rows:
        rec = dict(r)
        body.append(line([_cell(rec.get(n)) for n in names]))
    if not body:
        return []
    return [
        "DATA PREVIEW (first 5 rows):",
        header,
        rule,
        *body,
        "(" + ("first %d of " % len(cols) if len(cols) > _PREVIEW_COLS else "") + "%d rows shown)" % min(len(rows), _PREVIEW_ROWS),
    ]