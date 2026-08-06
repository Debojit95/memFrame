import json

import pandas as pd


def _jsonable(value):
    if isinstance(value, (dict, list, str, int, float, bool)) or value is None:
        return value
    try:
        json.dumps(value)
        return value
    except TypeError:
        return str(value)


def df_to_records(df: pd.DataFrame, session) -> list[dict]:
    rows = session.settings.max_output_rows if session.settings else 20
    cols = session.settings.max_output_cols if session.settings else 20
    return df.head(rows).iloc[:, :cols].to_dict(orient="records")


async def normalize(op_result: dict, session, *, include_result: bool = True, advance: bool = True) -> dict:
    """Map a memFrame op dict to a sandbox-friendly {"ok": ...} dict.

    DataFrame results are truncated to records; a returned new_table
    advances the session's active table (unless advance=False, for
    read-only inspection tools that must not change the working dataset).
    """
    if op_result.get("is_error"):
        return {
            "ok": False,
            "message": "",
            "hint": op_result.get("error_message") or op_result.get("message") or "operation failed",
        }

    payload = {"ok": True, "message": op_result.get("message") or ""}
    for key, value in op_result.items():
        if key in ("is_error", "error_message", "current_state", "message"):
            continue
        if key == "result":
            if not include_result:
                continue
            if isinstance(value, pd.DataFrame):
                payload["result"] = df_to_records(value, session)
            elif value is not None:
                payload["result"] = _jsonable(value)
        else:
            payload[key] = _jsonable(value)

    new_table = op_result.get("new_table")
    if new_table and advance:
        await session.advance_table(new_table)
        payload["active_table"] = session.table
    return payload
