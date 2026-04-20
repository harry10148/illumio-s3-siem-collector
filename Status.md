# Status

## Current phase

**v1.0 shipped. Ready to deploy to customer.**

## Summary

Refactoring the one-shot `s3_log_checker.py` into a production S3 → SIEM collector
(`illumio_s3_collector`). Target SIEM: **SIEM** at customer site.

## Completed

- Brainstorming session with user (multi-round Q&A)
- Real S3 bucket probe — confirmed file layout assumptions
  - 85,000+ files across auditable / pd=0,1,3 prefixes (pd=2 empty)
  - Filename pattern `{YYYYMMDD}_{uuid}.jsonl.gz` 100% consistent
  - Critical finding: same-day UUIDs are random → checkpoint must use
    `LastModified`, not key ordering alone
- SIEM constraints researched
  - UDP ≤ 1024 bytes, TCP/TLS ≤ 8192 bytes
  - Basic JSON parser flat-only → need upstream flatten
  - HTTPS rawupload endpoint supports batch
- Design spec written: [docs/superpowers/specs/2026-04-20-illumio-s3-siem-collector-design.md](docs/superpowers/specs/2026-04-20-illumio-s3-siem-collector-design.md)

## Key decisions

| Area | Decision |
|---|---|
| Pipeline topology | Multi-pipeline, each independent |
| Default format | `syslog_json` (RFC5424 header + flattened JSON body) |
| Optional formats | `cef` (with YAML mapping), `json` (HTTPS) |
| Transports | UDP / TCP / TLS / HTTPS all supported; TLS/6514 recommended |
| Source pull | S3 only; SQS reserved for future (abstraction preserved) |
| Scheduling | APScheduler BlockingScheduler, per-pipeline interval |
| Checkpoint | JSON file, `last_modified` + `last_key` tuple, atomic write |
| Startup | Configurable `initial_lookback_hours` (default 0) |
| Failure | Retry with backoff; on sink failure, checkpoint does not advance |
| Filter | Secondary filter with `simpleeval` expressions |
| Flatten | Nested JSON → `_`-separated keys, arrays stringified by default |
| Platforms | Linux (systemd) + Windows (NSSM), no Docker |
| Offline deploy | Bundle ships portable Python (python-build-standalone); target needs **no Python, no pip, no internet** |
| SIEM side | Custom Parser XML templates shipped with tool |

## Next steps

1. ~~User reviews [design spec](docs/superpowers/specs/2026-04-20-illumio-s3-siem-collector-design.md)~~ — approved
2. ~~Invoke `writing-plans` skill~~ — done, see [plan](docs/superpowers/plans/2026-04-20-illumio-s3-siem-collector.md)
3. Execute the plan: 29 tasks organized into foundation → transforms → I/O → orchestration → assets → offline deployment → tests
4. Verification at Task 28 (full suite) and Task 29 (real-bucket smoke)

## Resuming in a new session

Context persisted to mem0 under `project="illumio_s3_collector"`, 8 topics
(project-overview, architecture-decisions, s3-pull-algorithm,
siem-constraints, offline-deployment, resolved-defaults,
execution-state, real-bucket-data-facts).

On a new device, after cloning the repo, run:

```python
from mem0 import MemoryClient
client = MemoryClient()
filters = {"AND": [{"user_id": "harry10148"},
                   {"metadata": {"project": "illumio_s3_collector"}}]}
# search any topic or list all
client.search("illumio collector architecture", filters=filters, version="v2", limit=10)
```

Then read `docs/superpowers/specs/`, `docs/superpowers/plans/`, `Status.md`,
`Task.md`, and start at plan Task 1.

## Open questions (deferred to plan phase)

- `max_files_per_tick` default (proposal: 1000)
- Ship CEF mappings in v1 or defer to v1.1? (proposal: defer, v1 = syslog_json only)
- Initial lookback default (proposal: 0 hours = start from now)
