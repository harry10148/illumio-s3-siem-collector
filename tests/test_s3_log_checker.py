import boto3
from moto import mock_aws

import s3_log_checker as checker


BUCKET = "test-bucket"
FQDN = "pce.example.com"
ORG = "42"


def _put(s3, key, body: bytes):
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)


def test_list_s3_files_respects_max_keys_and_skips_directory_markers(capsys):
    base = f"{FQDN}/org_id={ORG}/auditable/"

    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        _put(s3, base, b"")
        _put(s3, base + "20260421_a.jsonl.gz", b"a")
        _put(s3, base + "20260421_b.jsonl.gz", b"b")
        _put(s3, base + "20260421_c.jsonl.gz", b"c")

        session = boto3.Session(region_name="us-east-1")
        checker.list_s3_files(session, BUCKET, base, max_keys=2)

    out = capsys.readouterr().out
    assert "20260421_a.jsonl.gz" in out
    assert "20260421_b.jsonl.gz" in out
    assert "20260421_c.jsonl.gz" not in out
    assert f"  {base}\n" not in out
    assert "共顯示 2 個檔案" in out


def test_download_s3_files_preserves_relative_paths_for_prefix(tmp_path):
    base = f"{FQDN}/org_id={ORG}/auditable/"
    key_a = base + "2026/04/21/file.jsonl.gz"
    key_b = base + "2026/04/22/file.jsonl.gz"

    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        _put(s3, key_a, b"day-1")
        _put(s3, key_b, b"day-2")

        session = boto3.Session(region_name="us-east-1")
        checker.download_s3_files(session, BUCKET, prefix=base, out_dir=str(tmp_path))

    path_a = tmp_path / "2026" / "04" / "21" / "file.jsonl.gz"
    path_b = tmp_path / "2026" / "04" / "22" / "file.jsonl.gz"
    assert path_a.read_bytes() == b"day-1"
    assert path_b.read_bytes() == b"day-2"


def test_resolve_source_prefers_cli_over_config(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
aws:
  profile: test-profile
  region: us-east-1
source:
  bucket: config-bucket
  fqdn: config.example.com
  org_id: "99"
""".strip(),
        encoding="utf-8",
    )

    args = checker.build_parser().parse_args(
        ["--config", str(config_path), "--bucket", "cli-bucket", "--org-id", "123"]
    )
    config = checker._load_yaml_config(config_path)
    bucket, fqdn, org_id = checker._resolve_s3_source(args, config)

    assert bucket == "cli-bucket"
    assert fqdn == "config.example.com"
    assert org_id == "123"


def test_main_can_list_using_config_yaml(tmp_path, capsys):
    base = f"{FQDN}/org_id={ORG}/auditable/"
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        f"""
aws:
  region: us-east-1
source:
  bucket: {BUCKET}
  fqdn: {FQDN}
  org_id: "{ORG}"
""".strip(),
        encoding="utf-8",
    )

    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        _put(s3, base + "20260421_a.jsonl.gz", b"a")

        rc = checker.main(["--config", str(config_path), "--list", "--log-type", "auditable"])

    out = capsys.readouterr().out
    assert rc == 0
    assert f"s3://{BUCKET}/{base}" in out
    assert "20260421_a.jsonl.gz" in out
