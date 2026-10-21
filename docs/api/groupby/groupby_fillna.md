# GroupBy Fillna

Source: `src/memframe/wrappers/analytix/groupby_fillna.py`

`groupby_fillna` fills missing values using statistics or fill behavior
within groups.

```python
result = dataset.groupby("department").fillna("salary", method="mean")
```

```python
result = await dataset.groupby("department").afillna("salary", method="mean")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Column to fill. |
| `group_cols` | `list[str]` | Columns used for grouping. Required for direct API. |
| `value` | any | Replacement value for `method="constant"` where supported. |
| `method` | `str` | Group-wise fill strategy. Defaults to `"mean"`. |
| `dtype` | `str` or `None` | Optional dtype override: `"numeric"`, `"categorical"`, or `"datetime"`. |
