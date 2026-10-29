from datetime import datetime, timezone

import pytest

from aw_datastore.storages import MemoryStorage
from aw_query import query
from aw_query.exceptions import QueryFunctionException, QueryInterpretException


def evaluate(expression):
    return query(
        "arity",
        f"RETURN = {expression};",
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        datetime(2026, 1, 2, tzinfo=timezone.utc),
        MemoryStorage(testing=True),
    )


@pytest.mark.parametrize(
    "expression",
    [
        'filter_keyvals([], "title")',
        "filter_keyvals()",
        "query_bucket()",
        "sort_by_timestamp()",
    ],
)
def test_missing_required_arguments_are_query_errors(expression):
    with pytest.raises(QueryInterpretException, match="invalid amount of arguments"):
        evaluate(expression)


@pytest.mark.parametrize(
    "expression, expected",
    [
        ('filter_keyvals([], "title", ["x"])', []),
        ("flood([])", []),
        ("flood([], 5)", []),
        ("nop()", 1),
    ],
)
def test_valid_and_default_arguments_remain_supported(expression, expected):
    assert evaluate(expression) == expected


def test_extra_argument_remains_a_query_error():
    with pytest.raises(QueryInterpretException, match="invalid amount of arguments"):
        evaluate("nop(1)")


def test_wrong_argument_type_remains_a_function_error():
    with pytest.raises(QueryFunctionException, match="invalid type"):
        evaluate('filter_keyvals([], "title", "x")')
