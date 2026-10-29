import json
import re
from datetime import datetime, timedelta, timezone

import pytest

from aw_core.models import Event
from aw_datastore import Datastore
from aw_datastore.storages import MemoryStorage
from aw_query import query
from aw_query.exceptions import QueryFunctionException
from aw_query.functions import q2_filter_keyvals_regex
from aw_transform import filter_keyvals_regex


@pytest.fixture
def regex_datastore():
    datastore = Datastore(storage_strategy=MemoryStorage, testing=True)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    bucket = datastore.create_bucket(
        bucket_id="regex-test", type="test", client="test", hostname="test"
    )
    for index, data in enumerate(
        [{"title": "Editor"}, {"title": "Browser"}, {"other": "Editor"}]
    ):
        bucket.insert(
            Event(
                timestamp=start + timedelta(seconds=index),
                duration=timedelta(seconds=1),
                data=data,
            )
        )
    return datastore, start, start + timedelta(hours=1)


def run_filter(fixture, events, pattern):
    datastore, start, end = fixture
    return query(
        "regex-test",
        f'RETURN = filter_keyvals_regex({events}, "title", {json.dumps(pattern)});',
        start,
        end,
        datastore,
    )


@pytest.mark.parametrize("events", ["[]", 'query_bucket("regex-test")'])
@pytest.mark.parametrize("pattern", ["[", "(", "*"])
def test_invalid_regex_is_query_error(regex_datastore, events, pattern):
    with pytest.raises(QueryFunctionException, match="Invalid regex") as exc:
        run_filter(regex_datastore, events, pattern)
    assert "position 0" in str(exc.value)


@pytest.mark.parametrize(
    "events, pattern, titles",
    [
        ("[]", "Editor", []),
        ('query_bucket("regex-test")', "Edit", ["Editor"]),
        ('query_bucket("regex-test")', "^Missing$", []),
        ('query_bucket("regex-test")', "", ["Browser", "Editor"]),
    ],
)
def test_valid_regex_matching(regex_datastore, events, pattern, titles):
    result = run_filter(regex_datastore, events, pattern)
    assert [event["data"]["title"] for event in result] == titles


def test_transform_still_raises_regex_error():
    with pytest.raises(re.error):
        filter_keyvals_regex([], "title", "[")


def test_query_wrapper_does_not_reclassify_event_data_type_errors():
    with pytest.raises(TypeError):
        q2_filter_keyvals_regex(None, {}, [Event(data={"title": 42})], "title", ".*")
