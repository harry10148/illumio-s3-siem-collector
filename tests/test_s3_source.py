import gzip
from datetime import datetime, timezone

import boto3
import pytest
from moto import mock_aws

from core.checkpoint import Checkpoint
from sources.s3_source import S3Source


BUCKET = "test-bucket"
FQDN = "pce.example.com"
ORG = "42"


def _put(s3, key, payload: bytes):
    s3.put_object(Bucket=BUCKET, Key=key, Body=payload)


def _gz(obj: str) -> bytes:
    return gzip.compress(obj.encode("utf-8"))


@pytest.fixture
def s3_env():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        yield s3


def test_iter_new_files_returns_only_new(s3_env):
    base = f"{FQDN}/org_id={ORG}/auditable/"
    _put(s3_env, base + "20260420_old.jsonl.gz", _gz('{"x":1}'))
    _put(s3_env, base + "20260420_new.jsonl.gz", _gz('{"y":2}'))

    cp = Checkpoint(pipeline="p", last_modified=datetime(2020, 1, 1, tzinfo=timezone.utc))

    src = S3Source(bucket=BUCKET, fqdn=FQDN, org_id=ORG, s3_client=s3_env,
                   today=datetime(2026, 4, 20, 23, 59, 59, tzinfo=timezone.utc))
    results = list(src.iter_new_files("auditable", cp, max_files_per_tick=1000))
    assert len(results) == 2


def test_iter_new_files_skips_already_processed(s3_env):
    base = f"{FQDN}/org_id={ORG}/auditable/"
    _put(s3_env, base + "20260420_a.jsonl.gz", _gz('{"x":1}'))

    listed = s3_env.list_objects_v2(Bucket=BUCKET, Prefix=base)["Contents"][0]
    cp = Checkpoint(
        pipeline="p",
        last_modified=listed["LastModified"],
        last_key=listed["Key"],
    )

    src = S3Source(bucket=BUCKET, fqdn=FQDN, org_id=ORG, s3_client=s3_env,
                   today=datetime(2026, 4, 20, 23, 59, 59, tzinfo=timezone.utc))
    results = list(src.iter_new_files("auditable", cp))
    assert results == []


def test_summaries_pd2_path(s3_env):
    base = f"{FQDN}/org_id={ORG}/summaries/pd=2/"
    _put(s3_env, base + "20260420_x.jsonl.gz", _gz('{"pd":2}'))

    cp = Checkpoint(pipeline="p",
                    last_modified=datetime(2020, 1, 1, tzinfo=timezone.utc))
    src = S3Source(bucket=BUCKET, fqdn=FQDN, org_id=ORG, s3_client=s3_env,
                   today=datetime(2026, 4, 20, 23, 59, 59, tzinfo=timezone.utc))
    results = list(src.iter_new_files("pd2", cp))
    assert len(results) == 1


def test_max_files_per_tick_truncates(s3_env):
    base = f"{FQDN}/org_id={ORG}/auditable/"
    for i in range(20):
        _put(s3_env, base + f"20260420_{i:02d}.jsonl.gz", _gz('{"i":%d}' % i))

    cp = Checkpoint(pipeline="p",
                    last_modified=datetime(2020, 1, 1, tzinfo=timezone.utc))
    src = S3Source(bucket=BUCKET, fqdn=FQDN, org_id=ORG, s3_client=s3_env,
                   today=datetime(2026, 4, 20, 23, 59, 59, tzinfo=timezone.utc))
    results = list(src.iter_new_files("auditable", cp, max_files_per_tick=5))
    assert len(results) == 5


def test_tie_break_on_same_lastmodified(s3_env):
    base = f"{FQDN}/org_id={ORG}/auditable/"
    _put(s3_env, base + "20260420_aaa.jsonl.gz", _gz('{"x":1}'))
    _put(s3_env, base + "20260420_bbb.jsonl.gz", _gz('{"x":2}'))

    objs = s3_env.list_objects_v2(Bucket=BUCKET, Prefix=base)["Contents"]
    assert len(objs) == 2
    objs.sort(key=lambda o: (o["LastModified"], o["Key"]))
    cp = Checkpoint(
        pipeline="p",
        last_modified=objs[0]["LastModified"],
        last_key=objs[0]["Key"],
    )
    src = S3Source(bucket=BUCKET, fqdn=FQDN, org_id=ORG, s3_client=s3_env,
                   today=datetime(2026, 4, 20, 23, 59, 59, tzinfo=timezone.utc))
    results = list(src.iter_new_files("auditable", cp))
    assert len(results) == 1
    assert results[0][0] == objs[1]["Key"]


def test_path_to_log_type_resolves_known_paths():
    from sources.s3_source import path_to_log_type
    fqdn = "pce.example.com"
    org_id = "123"
    cases = {
        "pce.example.com/org_id=123/auditable/2026/05/07/x.json.gz": "auditable",
        "pce.example.com/org_id=123/summaries/pd=0/2026/05/07/x.json.gz": "pd0",
        "pce.example.com/org_id=123/summaries/pd=1/2026/05/07/x.json.gz": "pd1",
        "pce.example.com/org_id=123/summaries/pd=2/2026/05/07/x.json.gz": "pd2",
        "pce.example.com/org_id=123/summaries/pd=3/2026/05/07/x.json.gz": "pd3",
    }
    for key, expected in cases.items():
        assert path_to_log_type(key, fqdn, org_id) == expected


def test_path_to_log_type_unknown_returns_none():
    from sources.s3_source import path_to_log_type
    assert path_to_log_type("pce.example.com/org_id=123/foo/x", "pce.example.com", "123") is None
    assert path_to_log_type("other/org_id=123/auditable/x", "pce.example.com", "123") is None
    assert path_to_log_type("pce.example.com/org_id=999/auditable/x", "pce.example.com", "123") is None
