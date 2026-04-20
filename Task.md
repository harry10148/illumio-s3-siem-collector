# Task

## Active

### [ ] User review of design spec

**Path**: [docs/superpowers/specs/2026-04-20-illumio-s3-siem-collector-design.md](docs/superpowers/specs/2026-04-20-illumio-s3-siem-collector-design.md)

**Blockers**: Waiting for user to read and approve (or request changes).

**When done**: Unblock writing-plans phase.

---

## Upcoming (after spec approval)

### [ ] Produce implementation plan

Use `writing-plans` skill. Plan should cover:

- Phase 1: Project skeleton + config loader + checkpoint + unit tests
- Phase 2: S3 source + flatten + syslog_json mapper + smoke run
- Phase 3: TLS sink + end-to-end pipeline wired
- Phase 4: Remaining sinks (UDP / TCP / HTTPS), CEF mapper (optional v1)
- Phase 5: Scheduler + multi-pipeline + FortiSIEM parser XML
- Phase 6: systemd / NSSM deployment docs, README
- Phase 7: Offline bundle scripts (`scripts/build_offline_bundle.sh`,
  `scripts/build_offline_bundle.ps1`, `scripts/install.sh`, `scripts/install.ps1`)
  + test both bundles on clean Linux + Windows VM without network

### [ ] Implement per plan

Each phase ends with verification checkpoint per design §15.

### [ ] FortiSIEM parser XML templates

Two parsers: `IllumioPCE_Auditable.xml`, `IllumioPCE_Summaries.xml`. Test by
importing into FortiSIEM and verifying events show up with parsed fields.

---

## Done

- [x] Explore existing `s3_log_checker.py`
- [x] Read Illumio docs (S3 collection mechanisms, traffic log format)
- [x] Probe real S3 bucket — verify layout assumptions
- [x] Research FortiSIEM syslog & HTTP receiver constraints
- [x] Brainstorm design with user (multi-round)
- [x] Write design spec
- [x] Create Status.md and Task.md

---

## Notes

- Real bucket: `illumio-flow-ap-scp45-msig-mingtai-com-tw` (customer: Mingtai Fire & Marine)
- AWS credentials: in `Mingtai-Fire-&-Marine-Insurance-S3-Access-Key.txt` at repo root —
  **do not commit**; treat as sensitive
- One-off probe script `_probe.py` had credentials inlined for testing — removed
  after use
