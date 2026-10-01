import numpy as np

from memframe.core.ingestion.datatype_detector import DatatypeDetector

from memframe.core.analytix.comparison import ComparisonOps, make_comparison_ops
from memframe.cache import record_call


class ComparisonOrchestrator:
    """
    Access: ops.compare.compare(col1, col2, operator)
    """

    def __init__(self, memframe_ops_instance):
        self._ops_parent = memframe_ops_instance
        self._compare_ops = None
        self._memframe = memframe_ops_instance.memframe   # MemFrame
        self._data_id = memframe_ops_instance._data_id
        self._dtype_detector = DatatypeDetector()

    async def _ensure_ops(self) -> ComparisonOps:
        if self._compare_ops is None:
            await self._ops_parent._ensure_adapter()
            self._compare_ops = make_comparison_ops(self._ops_parent._adapter)
        return self._compare_ops

    async def _get_context(self):
        return await self._ops_parent._get_active_context()

    def _family_from_sql_type(self, sql_type: str) -> str:
        if not sql_type:
            return "categorical"

        t = str(sql_type).upper().strip()
        base = t.split("(", 1)[0].strip()

        if "TIMESTAMP" in base or base in {"DATE", "DATETIME", "TIMESTAMPTZ"}:
            return "datetime"

        numeric_types = {
            "INTEGER", "INT", "BIGINT", "SMALLINT",
            "FLOAT", "DOUBLE", "DOUBLE PRECISION", "REAL",
            "NUMERIC", "DECIMAL",
        }
        if base in numeric_types:
            return "numeric"

        return "categorical"

    async def _detect_compare_dtype(self, ops: ComparisonOps,table: str, schema: str, column: str,) -> str:
        """
        Detect column family for compare routing.
        Returns one of: 'numeric', 'categorical', 'datetime'.
        """
        sample_df = await ops._fetch_data(table, schema, columns=[column])

        if sample_df.empty:
            return "categorical"

        # Prefer exact name; fallback to first selected column.
        series = sample_df[column].replace('', np.nan) if column in sample_df.columns else sample_df.iloc[:, 0]
        import pyarrow as pa
        chunked = pa.chunked_array([pa.array(series)])
        inferred = self._dtype_detector._infer_column(chunked)
        detected = str(inferred.get("type", "text")).lower()
        postgres_type = str(inferred.get("postgres_type", "")).upper()

        if detected in ("integer", "float"):
            return "numeric"
        if detected == "datetime" or self._family_from_sql_type(postgres_type) == "datetime":
            return "datetime"
        return "categorical"

    
    @record_call
    async def compare(self, *args, **kwargs):
        """
        Compare two columns element-wise.

        Can be called as:
            compare("col1 >= col2")          ← string expression
            compare("col1", "col2", ">=")    ← original three-argument form
        """
        # ------------------------------------------------------------------
        #  Argument parsing
        # ------------------------------------------------------------------
        if len(args) == 1 and not kwargs:
            expr = args[0]
            try:
                from memframe.utils.str_compare_parser import parse_compare_expression
                col1, operator, col2 = parse_compare_expression(expr)
            except Exception as e:
                return {
                    "is_error": True,
                    "message": "",
                    "error_error": f"Parse error: {str(e)}",
                }
        elif len(args) == 3:
            col1, col2, operator = args
        else:
            return {
                "is_error": True,
                "message": "",
                "error_message": (
                    "compare() expects either a single expression string "
                    "like 'A >= B' or three arguments: col1, col2, operator."
                ),
            }

        # Validate operator
        valid_ops = {"==", "!=", ">", "<", ">=", "<="}
        op = operator.strip()
        if op not in valid_ops:
            return {
                "is_error": True,
                "message": "",
                "error_message": f"Invalid operator '{operator}'. Must be one of {valid_ops}.",
            }

        # ------------------------------------------------------------------
        #  Execute
        # ------------------------------------------------------------------
        ops = await self._ensure_ops()
        table, schema = await self._get_context()

        backend = self._memframe._backend                          # ← ADD
        data_id = self._data_id or self._memframe._active_id       # ← ADD

        try:
            col_types = await ops.db.get_column_types(table, schema)
        except Exception:
            col_types = {}

        detected_dtype1 = await self._detect_compare_dtype(ops, table, schema, col1)
        detected_dtype2 = await self._detect_compare_dtype(ops, table, schema, col2)
        sql_dtype1 = self._family_from_sql_type(col_types.get(col1, ""))
        sql_dtype2 = self._family_from_sql_type(col_types.get(col2, ""))

        dtype1 = detected_dtype1 if detected_dtype1 != "categorical" else sql_dtype1
        dtype2 = detected_dtype2 if detected_dtype2 != "categorical" else sql_dtype2

        if dtype1 != dtype2:
            return {
                "is_error": True,
                "message": "",
                "error_message": (
                    f"Datatype mismatch: '{col1}' is detected as '{dtype1}', "
                    f"but '{col2}' is detected as '{dtype2}'. "
                    "Both columns must have the same datatype for compare."
                ),
            }

        if dtype1 == "datetime":
            result = await ops.compare_datetime(
                table, schema, col1, col2, op,
                backend=backend, data_id=data_id                    # ← ADD
            )
        elif dtype1 == "numeric":
            result = await ops.compare_numeric(
                table, schema, col1, col2, op,
                backend=backend, data_id=data_id                    # ← ADD
            )
        else:
            result = await ops.compare_categorical(
                table, schema, col1, col2, op,
                backend=backend, data_id=data_id                    # ← ADD
            )

        return result
    
    
    
        """
        Compare two columns element‑wise.
        
        Can be called as:
            compare("col1 >= col2")          ← string expression
            compare("col1", "col2", ">=")    ← original three‑argument form
        """
        # ------------------------------------------------------------------
        #  Argument parsing
        # ------------------------------------------------------------------
        if len(args) == 1 and not kwargs:
            # String expression
            expr = args[0]
            try:
                from memframe.utils.str_compare_parser import parse_compare_expression
                col1, operator, col2 = parse_compare_expression(expr)
            except Exception as e:
                return {
                    "is_error": True,
                    "message": "",
                    "error_message": f"Parse error: {str(e)}",
                }
        elif len(args) == 3:
            col1, col2, operator = args
        else:
            return {
                "is_error": True,
                "message": "",
                "error_message": (
                    "compare() expects either a single expression string "
                    "like 'A >= B' or three arguments: col1, col2, operator."
                ),
            }

        # Validate operator
        valid_ops = {"==", "!=", ">", "<", ">=", "<="}
        op = operator.strip()
        if op not in valid_ops:
            return {
                "is_error": True,
                "message": "",
                "error_message": f"Invalid operator '{operator}'. Must be one of {valid_ops}.",
            }

        # ------------------------------------------------------------------
        #  Rest of the method (unchanged)
        # ------------------------------------------------------------------
        ops = await self._ensure_ops()
        table, schema = await self._get_context()

        try:
            col_types = await ops.db.get_column_types(table, schema)
        except Exception:
            col_types = {}

        detected_dtype1 = await self._detect_compare_dtype(ops, table, schema, col1)
        detected_dtype2 = await self._detect_compare_dtype(ops, table, schema, col2)
        sql_dtype1 = self._family_from_sql_type(col_types.get(col1, ""))
        sql_dtype2 = self._family_from_sql_type(col_types.get(col2, ""))

        dtype1 = detected_dtype1 if detected_dtype1 != "categorical" else sql_dtype1
        dtype2 = detected_dtype2 if detected_dtype2 != "categorical" else sql_dtype2

        if dtype1 != dtype2:
            return {
                "is_error": True,
                "message": "",
                "error_message": (
                    f"Datatype mismatch: '{col1}' is detected as '{dtype1}', "
                    f"but '{col2}' is detected as '{dtype2}'. "
                    "Both columns must have the same datatype for compare."
                ),
            }

        if dtype1 == "datetime":
            result = await ops.compare_datetime(table, schema, col1, col2, op)
        elif dtype1 == "numeric":
            result = await ops.compare_numeric(table, schema, col1, col2, op)
        else:
            result = await ops.compare_categorical(table, schema, col1, col2, op)

        return result