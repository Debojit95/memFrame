# Plan: Pure in-DB single-scan correlation / covariance (Tier-1 + Tier-2 ClickHouse)

Status: APPLICATION BLOCKED — an active permission rule denies `edit` on source files
(`edit * -> deny`), consistent with read-only plan mode. The code below is ready to
apply once edit permission / plan mode is lifted.

## Goal
Replace the `UNION ALL` of per-pair `CORR()`/`COVAR_SAMP()` (one full table scan per
pair ≈ n²/2 scans) with batches of B aggregate calls inside ONE `SELECT` (one scan per
batch). The DB engine evaluates all aggregates in a single pass over the data. Keep the
DB-native `CORR`/`COVAR_SAMP` so results still match `pandas.corr()/.cov()` within
`rtol=1e-5` (existing tests pass). Drop the intermediate output table.

## Tier-2 (ClickHouse, adaptive single-scan, no UDF)
Batch size `B = min(total_pairs, 250)` for ClickHouse; when the whole pair set fits the
expression budget the loop issues a *single* `SELECT` (one scan). Wider tables fall back
to batching (still one scan per batch). DuckDB/Postgres use `B = min(total_pairs, 500)`.

## File: src/memframe/core/analytix/stats.py

### 1) Replace `_exec_union_batched` (lines 809-819) with this helper

```python
    async def _multi_column_assoc_matrix(
        self,
        table: str,
        schema: str,
        columns: List[str],
        agg_fn: str,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Compute an n x n correlation/covariance matrix purely in the database.

        All CORR/COVAR_SAMP calls are batched into single SELECT statements so the
        engine scans the table once per batch, not once per pair. ClickHouse uses a
        tighter batch so a single scan covers the table whenever the pair count fits
        its expression budget (Tier-2); wider tables fall back to batching.
        """
        try:
            if not isinstance(self.db, (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)):
                raise self._unsupported_backend_error()

            q = self._qualified_table(table, schema)
            cols = [SQLIdentifierSanitizer.sanitize(c) for c in columns]
            n = len(cols)
            if n < 1:
                return self._error_response("No columns provided", columns)

            def _cast(c: str) -> str:
                if isinstance(self.db, PostgresAdapter):
                    return f'CAST(NULLIF("{c}"::text, \'\') AS DOUBLE PRECISION)'
                if isinstance(self.db, DuckDBAdapter):
                    return f'TRY_CAST("{c}" AS DOUBLE)'
                return f'toFloat64OrNull(toString("{c}"))'

            alias = {c: f"a{idx}" for idx, c in enumerate(cols)}
            pairs = [(i, j) for i in range(n) for j in range(i, n)]

            # ponytail: batch size chosen under each backend's expression-count limit
            if isinstance(self.db, ClickHouseAdapter):
                B = min(len(pairs), 250)
            else:
                B = min(len(pairs), 500)

            results: Dict[tuple, Any] = {}
            for start in range(0, len(pairs), B):
                batch = pairs[start:start + B]
                used = sorted(
                    {cols[i] for (i, j) in batch} | {cols[j] for (i, j) in batch},
                    key=lambda c: cols.index(c),
                )
                cte = ", ".join(f"{_cast(c)} AS {alias[c]}" for c in used)
                sel = []
                for k, (i, j) in enumerate(batch):
                    ai, aj = alias[cols[i]], alias[cols[j]]
                    cond = f"{ai} IS NOT NULL AND {aj} IS NOT NULL"
                    sel.append(
                        f"{agg_fn}(CASE WHEN {cond} THEN {ai} END, "
                        f"CASE WHEN {cond} THEN {aj} END) AS v_{k}"
                    )
                sql = f"WITH q AS (SELECT {cte} FROM {q}) SELECT " + ", ".join(sel) + " FROM q"
                rows = await self._fetch(sql)
                row = rows[0] if rows else {}
                for k, (i, j) in enumerate(batch):
                    val = row.get(f"v_{k}") if row else None
                    results[(i, j)] = val if val is not None else float("nan")

            mat = [[results.get((min(i, j), max(i, j))) for j in range(n)] for i in range(n)]
            df = pd.DataFrame(mat, index=columns, columns=columns)
            msg = f"Computed {agg_fn} matrix for {n} columns"
            return self._success_response(msg, columns, result=df)

        except Exception as e:
            return self._error_response(
                f"multi_column_assoc_matrix error: {str(e)}\n{traceback.format_exc()}",
                columns,
            )
```

### 2) Replace `numeric_multi_column_correlation` (lines 821-889) with

```python
    async def numeric_multi_column_correlation(
        self,
        table: str,
        schema: str,
        columns: List[str],
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        agg = "CORR" if isinstance(self.db, (PostgresAdapter, DuckDBAdapter)) else "corr"
        return await self._multi_column_assoc_matrix(
            table, schema, columns, agg, backend=backend, data_id=data_id, new_table=new_table
        )
```

### 3) Replace `numeric_multi_column_covariance` (lines 891-952) with

```python
    async def numeric_multi_column_covariance(
        self,
        table: str,
        schema: str,
        columns: List[str],
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        agg = "COVAR_SAMP" if isinstance(self.db, (PostgresAdapter, DuckDBAdapter)) else "covarSamp"
        return await self._multi_column_assoc_matrix(
            table, schema, columns, agg, backend=backend, data_id=data_id, new_table=new_table
        )
```

## Why it is correct
- One `SELECT` with B `CORR`/`COVAR_SAMP` = one table scan (SQL engines aggregate in a
  single pass). Scans drop from n(n+1)/2 to ceil(n(n+1)/2 / B).
- Per-pair `CASE WHEN ai IS NOT NULL AND aj IS NOT NULL THEN ai END` keeps pairwise
  deletion; cast yields NULL on empty-string/non-numeric, so old behaviour is preserved.
- Uses the DB's native `CORR`/`COVAR_SAMP` (sample, ddof=1) → matches pandas within rtol.
- `new_table`/output table removed; cache auto-persists the result DataFrame.

## Removed
- `_exec_union_batched` (was only used by these two functions).

## Validation
1. `tests/integration/ops/test_inspect.py::test_corr`
   `tests/integration/ops/test_stats.py::test_corr` / `test_cov` / `test_autocorr`
   on DuckDB + Postgres + ClickHouse → green.
2. `/tmp/opencode/covid_sample_test.py` (56 cols × 3 backends) vs `df.corr()/.cov()` (rtol=1e-5).
3. Timing: 61×350k DuckDB before (~65s) vs after (seconds).
