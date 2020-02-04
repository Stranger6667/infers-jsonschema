import json
from contextlib import suppress
from string import printable

import jsonschema
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from infers_jsonschema import from_py, from_string

SCHEMA_WITH_FORMAT = {
    "properties": {"key": {"format": "integer", "type": "string"}},
    "required": ["key"],
    "type": "object",
    "$schema": "http://json-schema.org/draft-07/schema#",
}

json_strat = st.recursive(
    st.none() | st.booleans() | st.text() | st.integers() | st.floats(),
    lambda children: st.lists(children, min_size=1)
    | st.dictionaries(st.text(), children, min_size=1),
)


@given(data=json_strat)
@settings(max_examples=1000)
@pytest.mark.parametrize(
    "constructor", (lambda x: from_py(x), lambda x: from_string(json.dumps(x)),),
)
def test_something(data, constructor):
    with suppress(ValueError):
        schema = constructor(data).to_py()
        jsonschema.validate(data, schema)


@pytest.mark.parametrize(
    "instance, error",
    (
        (float("inf"), "Can't represent inf as JSON"),
        (object(), "Can't convert to JSON:"),
    ),
)
def test_not_serializable(instance, error):
    with pytest.raises(ValueError, match=error):
        from_py(instance)


@pytest.mark.parametrize(
    "kwargs, expected",
    (
        ({"detect_format": True}, SCHEMA_WITH_FORMAT),
        ({}, SCHEMA_WITH_FORMAT),
        (
            {"detect_format": False},
            {
                "properties": {"key": {"type": "string"}},
                "required": ["key"],
                "type": "object",
                "$schema": "http://json-schema.org/draft-07/schema#",
            },
        ),
    ),
)
def test_detect_format(kwargs, expected):
    assert from_py({"key": "1"}, **kwargs).to_py() == expected
