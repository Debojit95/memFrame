"""ClickHouse type inference for values loaded from pandas."""

import pandas as pd

from memframe.core.analytix.inspection.clickhouse import ClickHouseTableOps


def test_object_bool_series_becomes_nullable_uint8():
    ops = ClickHouseTableOps(None)
    series = pd.Series([True, None, False], dtype=object)

    assert ops._sql_type_for_series(series) == "Nullable(UInt8)"


def test_bool_cells_normalize_to_int():
    ops = ClickHouseTableOps(None)

    assert ops._normalize_cell_value(True) == 1
    assert ops._normalize_cell_value(False) == 0
    assert ops._normalize_cell_value(None) is None
