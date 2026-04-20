import json
import re

from mappers.syslog_json import SyslogJsonMapper


def _parse_header(line: str):
    m = re.match(
        r"^<(\d+)>(\d+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (.*)$",
        line, re.DOTALL,
    )
    assert m, f"unparseable: {line[:120]}"
    return {
        "pri": int(m.group(1)),
        "version": m.group(2),
        "timestamp": m.group(3),
        "hostname": m.group(4),
        "appname": m.group(5),
        "procid": m.group(6),
        "msgid": m.group(7),
        "structured": m.group(8),
        "msg": m.group(9),
    }


def test_auditable_event_header():
    m = SyslogJsonMapper(log_type="auditable")
    ev = {
        "timestamp": "2026-04-20T07:00:17.395Z",
        "pce_fqdn": "ap-scp45.illum.io",
        "href": "/orgs/1/events/x",
        "created_by": {"agent": {"hostname": "host1"}},
    }
    line = m.format(ev).decode("utf-8")
    h = _parse_header(line)
    assert h["pri"] == 134
    assert h["version"] == "1"
    assert h["timestamp"] == "2026-04-20T07:00:17.395Z"
    assert h["hostname"] == "ap-scp45.illum.io"
    assert h["appname"] == "illumio-pce"
    assert h["procid"] == "audit"
    assert h["msgid"] == "auditable"
    assert h["structured"] == "-"

    body = json.loads(h["msg"])
    assert body["href"] == "/orgs/1/events/x"
    assert body["created_by_agent_hostname"] == "host1"


def test_summaries_procid_is_summary():
    m = SyslogJsonMapper(log_type="pd2")
    ev = {"pd": 2, "timestamp": "2026-04-20T01:02:03Z",
          "pce_fqdn": "x", "src_ip": "10.0.0.1"}
    line = m.format(ev).decode("utf-8")
    h = _parse_header(line)
    assert h["procid"] == "summary"
    assert h["msgid"] == "pd2"


def test_missing_timestamp_uses_fallback():
    m = SyslogJsonMapper(log_type="auditable")
    ev = {"pce_fqdn": "x", "href": "y"}
    line = m.format(ev).decode("utf-8")
    h = _parse_header(line)
    assert re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", h["timestamp"])


def test_missing_hostname_uses_dash():
    m = SyslogJsonMapper(log_type="auditable")
    line = m.format({"timestamp": "2026-04-20T00:00:00Z"}).decode("utf-8")
    h = _parse_header(line)
    assert h["hostname"] == "-"


def test_flatten_disabled():
    m = SyslogJsonMapper(log_type="auditable", flatten_enabled=False)
    ev = {"pce_fqdn": "x", "timestamp": "2026-04-20T00:00:00Z",
          "a": {"b": 1}}
    line = m.format(ev).decode("utf-8")
    h = _parse_header(line)
    body = json.loads(h["msg"])
    assert body["a"] == {"b": 1}
