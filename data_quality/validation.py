"""Pure-Python checks for the five quality dimensions in Week 9 Lab 4.

Dimensions
----------
- Completitud: is the value present?
- Validez: does a present value parse and fall inside an allowed range?
- Unicidad: how many distinct values are there among the ones considered?
- Consistencia: do related fields agree (country vs currency)?
- Precisión: is a number close enough to a reference value?

These helpers never start Spark. Invalid input is rejected by returning
``False`` or ``None``. They do not coerce with a lossy cast (for example
``int(float("10.9"))``).
"""

from __future__ import annotations

import math
import re
from collections import Counter
from collections.abc import Callable, Iterable
from typing import Any

# Tokens that flat files use instead of a real value. Matching is
# case-insensitive and ignores surrounding whitespace.
MISSING_TOKENS = frozenset({"", "null", "none", "n/a", "na", "nan", "nil", "-"})

# ISO-2 country to the currency this lab treats as consistent.
# Normalization such as USA -> US belongs to cleansing, not to this check.
COUNTRY_CURRENCY = {
    "ES": "EUR",
    "FR": "EUR",
    "MX": "MXN",
    "US": "USD",
}

_PLAIN_NUMBER = re.compile(r"^[+-]?(?:\d+(?:\.\d+)?|\.\d+)$")


def is_missing(value: Any) -> bool:
    """Return whether ``value`` should count as absent for Completitud.

    ``0`` and ``False`` are present. ``None``, NaN, blank strings, and
    :data:`MISSING_TOKENS` are absent.
    """
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, str):
        return value.strip().lower() in MISSING_TOKENS
    return False


def completeness_ratio(values: Iterable[Any]) -> float:
    """Share of values that are present, in ``[0, 1]``.

    An empty collection is ``1.0``: there is nothing incomplete to report.
    """
    rows = list(values)
    if not rows:
        return 1.0
    present = sum(1 for value in rows if not is_missing(value))
    return present / len(rows)


def try_parse_number(value: Any) -> float | None:
    """Parse a number without raising and without a lossy cast.

    Returns ``None`` for missing values, booleans, infinities, NaN, and
    strings that are not a plain decimal (so ``"1,200.50"`` and
    ``"1.200,50"`` are rejected instead of being guessed).
    """
    if is_missing(value) or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
    elif isinstance(value, str):
        text = value.strip()
        if _PLAIN_NUMBER.fullmatch(text) is None:
            return None
        number = float(text)
    else:
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def is_valid_number_in_range(
    value: Any,
    low: float,
    high: float,
    *,
    inclusive: bool = True,
) -> bool:
    """Validez: ``value`` parses and lies inside ``[low, high]``.

    Missing and non-numeric values are invalid. Raises ``ValueError`` when
    ``low > high``.
    """
    if low > high:
        raise ValueError("low must be <= high")
    number = try_parse_number(value)
    if number is None:
        return False
    if inclusive:
        return low <= number <= high
    return low < number < high


def validity_ratio(values: Iterable[Any], predicate: Callable[[Any], bool]) -> float:
    """Share of values for which ``predicate`` is true.

    An empty collection is ``1.0``. The predicate is applied to every value,
    including missing ones, so the caller decides whether nulls are invalid.
    """
    rows = list(values)
    if not rows:
        return 1.0
    valid = sum(1 for value in rows if predicate(value))
    return valid / len(rows)


def uniqueness_ratio(values: Iterable[Any], *, ignore_missing: bool = True) -> float:
    """Unicidad: ``distinct / considered``.

    Missing values are dropped by default so a pile of nulls is not treated
    as one repeated business key. An empty collection (after that filter) is
    ``1.0``.
    """
    rows = list(values)
    if ignore_missing:
        rows = [value for value in rows if not is_missing(value)]
    if not rows:
        return 1.0
    distinct = len({_hashable(value) for value in rows})
    return distinct / len(rows)


def duplicate_values(values: Iterable[Any], *, ignore_missing: bool = True) -> list[Any]:
    """Values that occur more than once, in first-seen order."""
    rows = list(values)
    if ignore_missing:
        rows = [value for value in rows if not is_missing(value)]
    counts = Counter(_hashable(value) for value in rows)
    seen: set[Any] = set()
    duplicates: list[Any] = []
    for value in rows:
        key = _hashable(value)
        if counts[key] > 1 and key not in seen:
            seen.add(key)
            duplicates.append(value)
    return duplicates


def country_currency_consistent(country: Any, currency: Any) -> bool:
    """Consistencia: currency matches the ISO-2 country in :data:`COUNTRY_CURRENCY`.

    Comparison ignores case and surrounding space. Unknown country codes,
    including un-normalized aliases such as ``USA``, are inconsistent.
    """
    if is_missing(country) or is_missing(currency):
        return False
    expected = COUNTRY_CURRENCY.get(str(country).strip().upper())
    if expected is None:
        return False
    return expected == str(currency).strip().upper()


def within_absolute_tolerance(actual: Any, expected: float, tolerance: float) -> bool:
    """Precisión: ``actual`` parses and ``abs(actual - expected) <= tolerance``.

    Missing or non-numeric actuals are not precise. Raises ``ValueError``
    when ``tolerance`` is negative.
    """
    if tolerance < 0:
        raise ValueError("tolerance must be >= 0")
    number = try_parse_number(actual)
    if number is None:
        return False
    return abs(number - float(expected)) <= tolerance


def _hashable(value: Any) -> Any:
    if isinstance(value, list):
        return tuple(_hashable(item) for item in value)
    if isinstance(value, dict):
        return tuple(sorted((key, _hashable(item)) for key, item in value.items()))
    return value
