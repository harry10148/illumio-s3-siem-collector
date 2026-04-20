from core.expression_filter import compile_expression


def test_simple_equality():
    match = compile_expression("ev.pd == 2")
    assert match({"pd": 2}) is True
    assert match({"pd": 1}) is False


def test_nested_path():
    match = compile_expression("ev.created_by.agent.hostname == 'host1'")
    assert match({"created_by": {"agent": {"hostname": "host1"}}}) is True
    assert match({"created_by": {"agent": {"hostname": "host2"}}}) is False


def test_missing_field_does_not_raise():
    match = compile_expression("ev.missing_field == 'x'")
    assert match({}) is False


def test_in_operator_with_tuple():
    match = compile_expression("ev.dst_port in (445, 3389)")
    assert match({"dst_port": 445}) is True
    assert match({"dst_port": 80}) is False


def test_and_or():
    match = compile_expression("ev.pd == 2 and ev.dst_port in (22, 445)")
    assert match({"pd": 2, "dst_port": 22}) is True
    assert match({"pd": 1, "dst_port": 22}) is False
    assert match({"pd": 2, "dst_port": 80}) is False


def test_str_function_available():
    match = compile_expression("'login' in str(ev.notifications)")
    assert match({"notifications": [{"type": "login"}]}) is True
    assert match({"notifications": [{"type": "logout"}]}) is False


def test_invalid_expression_always_false():
    match = compile_expression("this is not valid python")
    assert match({"pd": 2}) is False


def test_null_event_fields():
    match = compile_expression("ev.x == None")
    assert match({"x": None}) is True
    assert match({}) is True


def test_dangerous_builtins_blocked():
    match = compile_expression("__import__('os')")
    assert match({}) is False
