import json

_SEP = "\n" + "\u2500" * 60 + "\n"


def records_to_md_table(records: list[dict]) -> str:
    """Render a list of record dicts as a markdown table."""
    if not records:
        return "(empty result)"
    columns = list(records[0].keys())
    widths = {
        c: max(len(str(c)), *(len(str(r[c]) if r[c] is not None else "None") for r in records))
        for c in columns
    }
    header = "| " + " | ".join(f"{c:<{widths[c]}}" for c in columns) + " |"
    rule = "|" + "|".join("-" * (widths[c] + 2) for c in columns) + "|"
    rows = []
    for rec in records:
        cells = " | ".join(f"{_cell(rec, c):<{widths[c]}}" for c in columns)
        rows.append("| " + cells + " |")
    return "\n".join([header, rule, *rows])


def _cell(rec: dict, c: str) -> str:
    v = rec[c]
    s = "None" if v is None else str(v)
    return s if len(s) <= 40 else s[:37] + "..."


def classify_block(label: str, result: dict) -> dict:
    """Classify a tool's normalized result dict into a typed response block."""
    if result.get("ok") is False:
        return {
            "query": label,
            "type": "error",
            "message": result.get("hint") or result.get("message") or "operation failed",
        }
    if "plot_id" in result or "spec" in result or "spec_preview" in result:
        return {
            "query": label,
            "type": "plot",
            "plot_id": result.get("plot_id"),
            "title": result.get("title"),
            "spec": result.get("spec") or result.get("spec_preview"),
            "error": None,
        }
    payload = result.get("result")
    if isinstance(payload, list):
        return {
            "query": label,
            "type": "df",
            "columns": list(payload[0].keys()) if payload else [],
            "rows": len(payload),
            "records": payload,
        }
    if isinstance(payload, dict):
        return {"query": label, "type": "dict", "value": payload}
    return {"query": label, "type": "dict", "value": payload}


def render_block(block: dict) -> str:
    kind = block["type"]
    if kind == "error":
        return f"[Error] {block['message']}"
    if kind == "plot":
        if block.get("error"):
            return f"[Plot error] {block['error']}"
        return f"[Plot] {block.get('title') or block.get('plot_id')} (id: {block.get('plot_id')})"
    if kind == "df":
        return records_to_md_table(block["records"])
    return json.dumps(block.get("value"), indent=2, default=str)


def render_blocks(blocks: list[dict]) -> str:
    """Render the per-query response blocks as a separated, indented view."""
    rendered = []
    for b in blocks:
        body = render_block(b)
        indented = "\n".join(f"    {line}" for line in body.splitlines())
        rendered.append(f"User query = {b['query']}\nResponse:\n{indented}")
    return _SEP.join(rendered)
