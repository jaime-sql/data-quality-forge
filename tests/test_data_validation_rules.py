"""Unit tests for Week 9 Lab 4 validation helpers.

No Spark session is created. The notebook under ``notebooks/`` is checked
only as source text so CI can run on a plain GitHub-hosted runner.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from data_quality.validation import (
    MISSING_TOKENS,
    completeness_ratio,
    country_currency_consistent,
    duplicate_values,
    is_missing,
    is_valid_number_in_range,
    try_parse_number,
    uniqueness_ratio,
    validity_ratio,
    within_absolute_tolerance,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK = REPO_ROOT / "notebooks" / "Week09_Lab04_DataCleansing_and_Profiling.py"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "deploy_databricks.yml"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, True),
        ("", True),
        ("   ", True),
        ("N/A", True),
        ("n/a", True),
        ("NaN", True),
        ("null", True),
        ("NONE", True),
        ("-", True),
        ("nil", True),
        ("ok", False),
        ("0", False),
        (0, False),
        (0.0, False),
        (False, False),
        (float("nan"), True),
    ],
)
def test_is_missing_null_and_sentinels(value, expected):
    assert is_missing(value) is expected


def test_completeness_ratio_counts_only_present_values():
    values = [None, "", "  ", "N/A", "null", "None", "ok", 0]
    assert completeness_ratio(values) == pytest.approx(2 / 8)


def test_completeness_ratio_empty_input_is_vacuous():
    assert completeness_ratio([]) == 1.0


def test_completeness_ratio_all_missing_is_zero():
    assert completeness_ratio([None, "n/a", "  "]) == 0.0


def test_missing_token_set_is_stable():
    assert MISSING_TOKENS == frozenset(
        {"", "null", "none", "n/a", "na", "nan", "nil", "-"}
    )


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("10", 10.0),
        ("10.9", 10.9),
        ("  -3.5 ", -3.5),
        (0, 0.0),
        (4, 4.0),
        (True, None),
        (False, None),
        (None, None),
        ("", None),
        ("abc", None),
        ("1,200.50", None),
        ("1.200,50", None),
        ("N/A", None),
        (float("inf"), None),
        (float("-inf"), None),
        (float("nan"), None),
    ],
)
def test_try_parse_number_rejects_bad_and_lossy_input(value, expected):
    assert try_parse_number(value) == expected


def test_try_parse_does_not_truncate_fractional_strings():
    # A silent int(...) cast would turn this into 10 and hide the fraction.
    assert try_parse_number("10.9") == 10.9


@pytest.mark.parametrize(
    ("value", "inclusive", "expected"),
    [
        (0, True, True),
        (10, True, True),
        (10, False, False),
        (0, False, False),
        (-0.01, True, False),
        (10.01, True, False),
        ("10", True, True),
        (" 2.5 ", True, True),
        (None, True, False),
        ("abc", True, False),
        ("1.200,50", True, False),
        (True, True, False),
        (False, True, False),
    ],
)
def test_validity_range_bounds(value, inclusive, expected):
    assert is_valid_number_in_range(value, 0, 10, inclusive=inclusive) is expected


def test_validity_range_rejects_inverted_bounds():
    with pytest.raises(ValueError, match="low must be <= high"):
        is_valid_number_in_range(1, 10, 0)


def test_validity_ratio_uses_predicate_on_every_value():
    values = [0, 5, 11, "x", None]
    ratio = validity_ratio(
        values, lambda value: is_valid_number_in_range(value, 0, 10)
    )
    assert ratio == pytest.approx(2 / 5)


def test_validity_ratio_empty_input_is_vacuous():
    assert validity_ratio([], lambda value: False) == 1.0


def test_uniqueness_ratio_ignores_missing_by_default():
    assert uniqueness_ratio(["a", "a", "b"]) == pytest.approx(2 / 3)
    assert uniqueness_ratio([None, None, "a"]) == 1.0
    assert uniqueness_ratio([None, "n/a", "  "]) == 1.0
    assert uniqueness_ratio([]) == 1.0


def test_uniqueness_ratio_can_count_missing_as_values():
    assert uniqueness_ratio([None, None, "a"], ignore_missing=False) == pytest.approx(
        2 / 3
    )


def test_duplicate_values_keep_first_seen_order():
    assert duplicate_values(["a", "b", "a", "c", "b", "b"]) == ["a", "b"]


def test_duplicate_values_skip_missing_unless_asked():
    assert duplicate_values([None, None, "a", "a"]) == ["a"]
    assert duplicate_values([None, "a", None], ignore_missing=False) == [None]


@pytest.mark.parametrize(
    ("country", "currency", "expected"),
    [
        ("ES", "EUR", True),
        ("es", "eur", True),
        (" FR ", "EUR", True),
        ("MX", "MXN", True),
        ("US", "USD", True),
        ("ES", "USD", False),
        ("USA", "USD", False),
        ("DE", "EUR", False),
        (None, "EUR", False),
        ("ES", None, False),
        ("", "EUR", False),
        ("ES", "n/a", False),
    ],
)
def test_country_currency_consistency(country, currency, expected):
    assert country_currency_consistent(country, currency) is expected


@pytest.mark.parametrize(
    ("actual", "expected_value", "tolerance", "expected"),
    [
        (10, 10, 0, True),
        (10.005, 10, 0.01, True),
        ("10.00", 10, 0.01, True),
        (10.02, 10, 0.01, False),
        ("10.9", 10, 0.01, False),
        ("N/A", 10, 0.01, False),
        (None, 0, 0, False),
        (True, 1, 0, False),
        ("1.200,50", 1200.50, 0.01, False),
    ],
)
def test_precision_within_absolute_tolerance(actual, expected_value, tolerance, expected):
    assert within_absolute_tolerance(actual, expected_value, tolerance) is expected


def test_precision_rejects_negative_tolerance():
    with pytest.raises(ValueError, match="tolerance must be >= 0"):
        within_absolute_tolerance(1, 1, -0.01)


def test_precision_rejects_non_finite_values():
    assert within_absolute_tolerance(float("nan"), 0, 1) is False
    assert within_absolute_tolerance(0, float("nan"), 1) is False


def test_notebook_is_a_teaching_scaffold_not_an_answer_key():
    source = NOTEBOOK.read_text(encoding="utf-8")
    compile(source, str(NOTEBOOK), "exec")
    assert source.startswith("# Databricks notebook source\n")
    assert "# COMMAND ----------" in source
    for dimension in (
        "Completitud",
        "Validez",
        "Unicidad",
        "Consistencia",
        "Precisión",
    ):
        assert dimension in source
    assert source.count("# TODO(estudiante):") >= 5
    assert source.count("raise NotImplementedError") >= 5
    assert "try_cast" in source
    assert "row_number" in source
    assert "createDataFrame" in source
    assert "cuarentena" in source.lower()


def test_workflow_syncs_notebooks_with_workspace_secrets():
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "workflow_dispatch" in text
    assert "branches:" in text
    assert "main" in text
    assert "pytest -q" in text
    assert "requirements-dev.txt" in text
    assert "needs: test" in text
    assert "secrets.DATABRICKS_HOST" in text
    assert "secrets.DATABRICKS_TOKEN" in text
    assert "/Shared/Week09_Lab04_DataCleansing" in text
    assert "/api/2.0/workspace/mkdirs" in text
    assert "/api/2.0/workspace/import" in text
    assert '"SOURCE"' in text or "'SOURCE'" in text
    assert '"PYTHON"' in text or "'PYTHON'" in text
    assert "overwrite" in text


def test_repository_does_not_contain_live_credentials():
    # Prefixes are concatenated so this file is not itself a false positive.
    banned_patterns = (
        "da" + "pi",
        "gh" + "p_",
        "github_" + "pat_",
        "BEGIN " + "PRIVATE KEY",
        "AK" + "IA",
    )
    tracked_roots = [
        REPO_ROOT / "README.md",
        REPO_ROOT / "requirements-dev.txt",
        REPO_ROOT / ".gitignore",
        NOTEBOOK,
        WORKFLOW,
        REPO_ROOT / "data_quality",
        REPO_ROOT / "tests",
    ]
    text_suffixes = {".py", ".md", ".yml", ".yaml", ".txt", ".gitignore"}
    files: list[Path] = []
    for root in tracked_roots:
        if root.is_dir():
            files.extend(
                path
                for path in root.rglob("*")
                if path.is_file() and path.suffix in text_suffixes
            )
        elif root.suffix in text_suffixes or root.name == ".gitignore":
            files.append(root)
    for path in files:
        text = path.read_text(encoding="utf-8", errors="replace")
        for pattern in banned_patterns:
            assert pattern not in text, f"{path} contains {pattern}"
