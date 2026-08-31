"""Phase-1 security-hardening regression tests (no DB needed)."""

import pytest

from memframe.core.analytix.cleaning.base import _sql_literal
from memframe.core.analytix.inspection.base import _apply_map_placeholder
from memframe.core.orchestrator.analytix.cleaning import _coerce_int, _coerce_num
from memframe.dashboard.render import render_guardrail_blocked
from memframe.utils.helper import SQLIdentifierSanitizer


# ── _sql_literal: fillna CONSTANT fill values must not inject ────────────


def test_sql_literal_plain_string():
    assert _sql_literal("abc") == "'abc'"


def test_sql_literal_escapes_single_quotes():
    assert _sql_literal("o'brien") == "'o''brien'"


def test_sql_literal_injection_attempt_is_escaped():
    evil = "x'); DROP TABLE t; --"
    literal = _sql_literal(evil)
    assert "''" in literal
    assert literal.count("'") == 4  # opening + closing + the doubled pair


def test_sql_literal_non_string_unchanged():
    assert _sql_literal(3) == "3"
    assert _sql_literal(2.5) == "2.5"
    assert _sql_literal(None) == "None"


# ── identifier validation: trailing newline must be rejected ─────────────


def test_identifier_pattern_rejects_trailing_newline():
    # validate() strips input, but the pattern itself must not match "users\n"
    assert SQLIdentifierSanitizer._VALID_QUALIFIED.match("users\n") is None
    assert SQLIdentifierSanitizer._VALID_SEGMENT.match("users\n") is None


def test_validate_still_accepts_plain_identifiers():
    assert SQLIdentifierSanitizer.validate("users") == "users"
    assert SQLIdentifierSanitizer.validate("main.users") == "main.users"


# ── map placeholder: only standalone "x" is substituted ──────────────────


def test_map_placeholder_preserves_function_names():
    assert _apply_map_placeholder("MAX(x)", '"score"') == 'MAX("score")'


def test_map_placeholder_lowercase_function_names():
    assert _apply_map_placeholder("exp(x)", '"v"') == 'exp("v")'


def test_map_placeholder_bare_x_and_arithmetic():
    assert _apply_map_placeholder("x * 2 + 1", '"v"') == '"v" * 2 + 1'


# ── orchestrator numeric coercion helpers ────────────────────────────────


def test_coerce_num_accepts_numbers_and_numeric_strings():
    assert _coerce_num(3) == 3
    assert _coerce_num("3.0") == 3.0
    assert _coerce_num(None) is None


def test_coerce_num_rejects_non_numeric():
    with pytest.raises(ValueError):
        _coerce_num("abc")


def test_coerce_int_accepts_ints_and_numeric_strings():
    assert _coerce_int(5) == 5
    assert _coerce_int("7") == 7


def test_coerce_int_rejects_floats_and_garbage():
    with pytest.raises(ValueError):
        _coerce_int(3.7)
    with pytest.raises(ValueError):
        _coerce_int("abc")


# ── guardrail HTML escaping ──────────────────────────────────────────────


def test_guardrail_reason_is_html_escaped():
    out = render_guardrail_blocked("<script>alert(1)</script>")
    assert "<script>" not in out
    assert "&lt;script&gt;" in out


def test_guardrail_braces_do_not_break_format():
    out = render_guardrail_blocked("no {columns} matched")
    assert "no {columns} matched" in out


# ── SecretStr for agent credentials (only when memframe_ai is importable) ─


def test_ai_settings_key_is_masked():
    AISettings = pytest.importorskip("memframe_ai.config").AISettings
    s = AISettings(api_key="sk-secret")
    assert "sk-secret" not in repr(s)
    assert "sk-secret" not in str(s.model_dump())
    assert s.api_key.get_secret_value() == "sk-secret"


def test_ai_settings_accepts_plain_string_key():
    AISettings = pytest.importorskip("memframe_ai.config").AISettings
    s = AISettings(api_key="sk-secret")
    assert isinstance(s.api_key.get_secret_value(), str)
