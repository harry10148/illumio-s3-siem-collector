from mappers._flatten import flatten


def test_flat_dict_unchanged():
    assert flatten({"a": 1, "b": "x"}) == {"a": 1, "b": "x"}


def test_one_level_nested():
    assert flatten({"a": {"b": 1, "c": 2}}) == {"a_b": 1, "a_c": 2}


def test_three_level_nested():
    result = flatten({"a": {"b": {"c": {"d": 42}}}})
    assert result == {"a_b_c_d": 42}


def test_custom_separator():
    assert flatten({"a": {"b": 1}}, separator=".") == {"a.b": 1}


def test_array_stringify_default():
    result = flatten({"a": [1, 2, 3]})
    assert result == {"a": "[1, 2, 3]"} or result == {"a": "[1,2,3]"}


def test_array_strategy_first():
    result = flatten({"a": [{"x": 1}, {"x": 2}]}, array_strategy="first")
    assert result == {"a_x": 1}


def test_array_strategy_skip():
    assert flatten({"a": [1, 2], "b": 3}, array_strategy="skip") == {"b": 3}


def test_none_values_preserved_not_stringified():
    assert flatten({"a": None, "b": 1}) == {"a": None, "b": 1}


def test_empty_dict():
    assert flatten({}) == {}


def test_empty_array_stringify():
    result = flatten({"a": []})
    assert result["a"] in ("[]",)


def test_max_depth_reached_stringifies():
    deep = {"a": {"b": {"c": {"d": {"e": 1}}}}}
    result = flatten(deep, max_depth=2)
    assert "a_b_c" in result
    assert isinstance(result["a_b_c"], str)
    assert "e" in result["a_b_c"]


def test_nested_with_array_of_dicts_stringify():
    ev = {"x": {"y": [{"z": 1}]}}
    assert flatten(ev) == {"x_y": '[{"z": 1}]'}
