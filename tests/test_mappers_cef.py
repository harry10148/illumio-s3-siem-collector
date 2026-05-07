import re
from pathlib import Path

import yaml

from mappers.cef import CefMapper

REPO = Path(__file__).resolve().parent.parent


def _summaries_mapping():
    return REPO / "mappings" / "summaries.yaml"


def _auditable_mapping():
    return REPO / "mappings" / "auditable.yaml"


def test_cef_summaries_basic():
    m = CefMapper(log_type="pd2", mapping_path=_summaries_mapping())
    ev = {
        "timestamp": "2026-04-20T01:00:00Z",
        "pce_fqdn": "pce1",
        "pd": 2,
        "pd_qualifier": 0,
        "src_ip": "10.0.0.1", "dst_ip": "10.0.0.2",
        "dst_port": 22, "proto": 6, "dir": "O",
        "un": "root",
    }
    line = m.format(ev).decode("utf-8")
    m_re = re.search(r"CEF:0\|Illumio\|PCE\|1\.0\|(\S+)\|([^|]+)\|(\d+)\|(.*)$", line)
    assert m_re, line
    signature, name, severity, ext = m_re.groups()
    assert signature == "2"
    assert "pd=2" in name
    assert int(severity) == 9
    assert "src=10.0.0.1" in ext
    assert "dst=10.0.0.2" in ext
    assert "dpt=22" in ext
    assert "cs1=2" in ext
    assert "cs1Label=PolicyDecision" in ext
    assert "suser=root" in ext


def test_cef_escapes_equals_and_backslash_in_extension():
    m = CefMapper(log_type="pd0", mapping_path=_summaries_mapping())
    ev = {"timestamp": "2026-04-20T00:00:00Z", "pce_fqdn": "p",
          "pd": 0, "src_ip": "a=b\\c", "dst_ip": "d"}
    line = m.format(ev).decode("utf-8")
    assert "src=a\\=b\\\\c" in line


def test_cef_auditable_dotted_path_resolves():
    m = CefMapper(log_type="auditable", mapping_path=_auditable_mapping())
    ev = {
        "timestamp": "2026-04-20T01:00:00Z",
        "pce_fqdn": "pce1",
        "href": "/orgs/1/events/xyz",
        "created_by": {"agent": {"hostname": "host1"},
                       "ven": {"href": "/orgs/1/vens/v1"}},
    }
    line = m.format(ev).decode("utf-8")
    assert "cs2=host1" in line
    assert "cs3=/orgs/1/vens/v1" in line
    assert "cs4=/orgs/1/events/xyz" in line


def test_cef_missing_severity_key_uses_default():
    m = CefMapper(log_type="auditable", mapping_path=_auditable_mapping())
    line = m.format({"timestamp": "2026-04-20T00:00:00Z",
                     "pce_fqdn": "p", "href": "h"}).decode("utf-8")
    assert re.search(r"CEF:0\|Illumio\|PCE\|1\.0\|\S+\|[^|]+\|5\|", line)


def test_header_pipe_is_escaped(tmp_path):
    mapping = {
        "cef_header": {
            "vendor": "Illumio",
            "product": "PCE",
            "version": "1.0",
            "signature_id_field": "pd",
            "name_template": "Illumio | Audit",
            "severity_map_default": 5,
        },
        "extensions": {},
    }
    p = tmp_path / "m.yaml"
    p.write_text(yaml.safe_dump(mapping), encoding="utf-8")
    m = CefMapper(log_type="pd0", mapping_path=p)
    line = m.format({"timestamp": "2026-04-20T00:00:00Z",
                     "pce_fqdn": "p", "pd": 0}).decode("utf-8")
    # Name's literal '|' must be escaped as '\|'.
    assert "Illumio \\| Audit" in line
    # The CEF body has exactly 7 unescaped '|' separators.
    cef_body = line.split("CEF:0|", 1)[1]
    cef_body = "CEF:0|" + cef_body
    unescaped_pipes = re.findall(r"(?<!\\)\|", cef_body)
    assert len(unescaped_pipes) == 7, line
