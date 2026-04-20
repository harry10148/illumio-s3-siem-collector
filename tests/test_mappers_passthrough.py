import json
from mappers.passthrough import PassthroughMapper


def test_passthrough_outputs_compact_json_bytes():
    m = PassthroughMapper(flatten_enabled=False)
    out = m.format({"a": 1, "b": "x"})
    assert isinstance(out, bytes)
    assert json.loads(out.decode("utf-8")) == {"a": 1, "b": "x"}


def test_passthrough_with_flatten():
    m = PassthroughMapper(flatten_enabled=True)
    out = m.format({"a": {"b": 1}})
    assert json.loads(out.decode("utf-8")) == {"a_b": 1}
