# GroupBy Fillna

> **Example data:** copy the setup cell from [Example Data](../example-data.md) into Colab once, then run the examples below. Examples use that `ctx`; local frames are shown inline.

Source: `src/memframe/wrappers/analytix/groupby_fillna.py`

`groupby_fillna` fills missing values using statistics or fill behavior
within groups.

```python
result = ctx.groupby("region").fillna("score", method="mean")
```

```python
result = await ctx.groupby("region").afillna("score", method="mean")
```

The filled `<column>_<method>_filled` column is written into the source
dataset. Repeated calls are cached as a source **ref**; no group/cache copy is
created.

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Column to fill. |
| `group_cols` | `list[str]` | Columns used for grouping. Required for direct API. |
| `value` | any | Replacement value for `method="constant"` where supported. |
| `method` | `str` | Group-wise fill strategy. Defaults to `"mean"`. |
| `dtype` | `str` or `None` | Optional dtype override: `"numeric"`, `"categorical"`, or `"datetime"`. |
