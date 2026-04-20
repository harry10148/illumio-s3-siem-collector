# Task

## Active

*(none — all 29 implementation tasks complete)*

---

## Done

- [x] Explore existing `s3_log_checker.py`
- [x] Read Illumio docs (S3 collection mechanisms, traffic log format)
- [x] Probe real S3 bucket — verify layout assumptions
- [x] Research SIEM syslog & HTTP receiver constraints
- [x] Brainstorm design with user (multi-round)
- [x] Write design spec
- [x] User approves design spec
- [x] Write implementation plan (29 tasks)
- [x] Execute implementation plan (Tasks 1–29) — branch `feature/implement`, tag `v1.0`
- [x] SIEM parser XML templates (`siem_parser/IllumioPCE_Auditable.xml`, `IllumioPCE_Summaries.xml`)
- [x] Production smoke test — real S3 bucket, RFC5424 syslog events confirmed at UDP 5514

---

## Next steps (post-v1.0)

- Merge `feature/implement` → `master`
- Deploy to customer (offline bundle for Linux or Windows)
- Import SIEM parsers (`siem_parser/`) at customer site
- Edit `/etc/illumio-collector/config.yaml` with real credentials + SIEM IP/port
- Start service: `sudo systemctl start illumio-collector`

---

## Notes

- Real bucket: `illumio-flow-XXXXXXXX-your-bucket` (git-ignored；真實值在本地 Access Key 文件)
- AWS region: `ap-northeast-1` (Tokyo) — confirmed by smoke test
- AWS credentials: in `MSGI-S3-Access-Key.txt` at repo root — **do not commit**
- `config.sandbox.yaml` in worktree is git-ignored; contains real credentials for local testing
