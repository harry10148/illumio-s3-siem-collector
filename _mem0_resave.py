"""Re-save the 7 missing topics for illumio_s3_collector, one at a time
with per-save verification."""
import sys
import time

sys.stdout.reconfigure(encoding="utf-8")

from mem0 import MemoryClient

client = MemoryClient()
USER = "harry10148"
PROJECT = "illumio_s3_collector"

TOPICS = [
    (
        "architecture-decisions",
        [
            {"role": "user", "content": "What's the locked architecture for illumio_s3_collector?"},
            {"role": "assistant", "content": (
                "Locked design decisions (approved during brainstorm, spec §4-7):\n\n"
                "1. MULTI-PIPELINE: one process, multiple independent pipelines. Each pipeline "
                "has its own log_type (auditable/pd0/pd1/pd2/pd3), poll_interval, mapper, sink, "
                "filter. User can enable/disable each independently.\n\n"
                "2. THREE-LAYER ABSTRACTION: Source -> Mapper -> Sink with abstract base classes. "
                "Modules: sources/base.py, mappers/base.py, sinks/base.py. "
                "Allows future SQS source, Kafka sink, etc. without rewriting pipeline core.\n\n"
                "3. DEFAULT FORMAT: syslog_json (RFC5424 header + flattened JSON body), NOT CEF. "
                "CEF is shipped as secondary option with YAML field mapping at mappings/*.yaml. "
                "Rationale: user researched FortiSIEM supports JSON-in-syslog via custom parser; "
                "tradeoff favored JSON for field preservation over CEF's column limits.\n\n"
                "4. FLATTEN: nested JSON flattened with _ separator before send. Example: "
                "created_by.agent.hostname becomes created_by_agent_hostname. Arrays stringified "
                "by default (array_strategy=stringify, also 'first' and 'skip' available). "
                "Enables FortiSIEM basic JSON parser to extract fields without custom nested parser.\n\n"
                "5. TRANSPORTS (sinks/): UDP (1024 byte cap), TCP, TLS (port 6514), HTTPS with "
                "batch NDJSON POST. TLS/6514 is recommended default. UDP for test only.\n\n"
                "6. SCHEDULER: APScheduler BlockingScheduler, one job per pipeline. "
                "coalesce=True, max_instances=1, ThreadPoolExecutor for parallel pipelines. "
                "No external cron or Task Scheduler needed.\n\n"
                "7. FILTER: simpleeval (safe expression evaluator, not Python builtin) with "
                "DotDict proxy for nested path access. Example: "
                "'ev.pd == 2 and ev.dst_port in (22, 445, 3389)'. Runs BEFORE flatten so user "
                "sees original structure. Failed expressions log once then go silent."
            )},
        ],
    ),
    (
        "s3-pull-algorithm",
        [
            {"role": "user", "content": "How does the S3 pull and checkpoint strategy work?"},
            {"role": "assistant", "content": (
                "CRITICAL INSIGHT discovered during brainstorm: Illumio S3 file naming is "
                "{YYYYMMDD}_{uuid}.jsonl.gz. My first design used key ordering "
                "(StartAfter=last_key) for checkpoint, which is WRONG - same-day UUIDs are "
                "RANDOM, so lexical order != chronological within a day. A new file with a "
                "lexically-smaller UUID would be missed.\n\n"
                "CORRECT ALGORITHM (spec §7.2, plan Task 10, sources/s3_source.py):\n\n"
                "1. Scope list by date prefix: steady-state scans today + yesterday (UTC). "
                "Cold start or >48h gap fans out to all dates in lookback window. "
                "Avoids scanning 43k+ files every tick.\n\n"
                "2. Store checkpoint as (last_modified, last_key) tuple at state/<pipeline>.json "
                "with atomic write (tempfile + os.replace).\n\n"
                "3. For each S3 object returned: is_new if LastModified > checkpoint.last_modified, "
                "OR (LastModified == last_modified AND key > last_key). "
                "The key is tiebreak for same-second writes.\n\n"
                "4. Sort candidates by (LastModified, Key) ascending.\n\n"
                "5. Cap at max_files_per_tick=1000.\n\n"
                "6. Per-file checkpoint advance: after ALL events in a file sent successfully, "
                "save checkpoint. On sink failure: checkpoint does NOT advance, next tick "
                "re-pulls same file. At-least-once semantics - SIEM must dedupe."
            )},
        ],
    ),
    (
        "fortisiem-constraints",
        [
            {"role": "user", "content": "What are the FortiSIEM receive constraints?"},
            {"role": "assistant", "content": (
                "FortiSIEM specifics (researched via Fortinet docs during brainstorm):\n\n"
                "MESSAGE SIZE LIMITS (hard, RFC-based):\n"
                "- UDP/514: 1024 bytes max per datagram. Collector must truncate + log WARN.\n"
                "- TCP/1470: 8192 bytes max. Same truncate+warn.\n"
                "- TLS/6514: 8192 bytes max.\n\n"
                "JSON PARSING:\n"
                "- Basic JSON parser handles FLAT single objects only. Nested objects and arrays "
                "NOT auto-parsed. That's why collector pre-flattens before send.\n"
                "- Arrays stringified to avoid FortiSIEM treating them as multiple events.\n"
                "- Custom XML parser needed for full structured parsing (we ship one).\n\n"
                "HTTP INGESTION:\n"
                "- Endpoint: https://<fsm>/rawupload?vendor=X&model=Y&reptIp=...&reptName=...\n"
                "- Supports batched NDJSON POST. No documented batch size limit.\n"
                "- splitJsonEvent() on server side splits batched events.\n\n"
                "CUSTOM PARSERS (shipped in fortisiem_parser/):\n"
                "- IllumioPCE_Auditable.xml: matches 'illumio-pce audit auditable' recognizer\n"
                "- IllumioPCE_Summaries.xml: matches 'illumio-pce summary'\n"
                "- Maps src_ip -> srcIpAddr, dst_ip -> destIpAddr, pd -> policyDecision, etc.\n"
                "- Install via FortiSIEM GUI: Admin > Device Support > Parsers > Upload XML"
            )},
        ],
    ),
    (
        "offline-deployment",
        [
            {"role": "user", "content": "How does offline deployment work when target has no Python?"},
            {"role": "assistant", "content": (
                "Target host requirement: x86_64 CPU + Linux (glibc 2.17+) or Windows 10+. "
                "NO Python, NO pip, NO internet required on target.\n\n"
                "CORE TECHNIQUE: ship python-build-standalone (Astral's portable Python, same "
                "one uv/rye use). Tarball extract = usable Python with pip. No registry changes, "
                "no PATH changes, no admin needed for Python itself (Windows admin only needed "
                "to register NSSM service).\n\n"
                "PYTHON VERSION LOCKED: 3.11.9 (PBS tag 20240415 or newer). Build host and "
                "target must match major.minor - cannot mix 3.10 and 3.11.\n\n"
                "PLATFORM: bundles are PER-PLATFORM (cannot cross-build). pydantic-core is "
                "Rust-compiled with manylinux or win_amd64 wheels. Linux bundle must build on "
                "Linux; Windows bundle on Windows.\n\n"
                "BUNDLE CONTENTS (~80 MB each):\n"
                "- app/ (collector.py + all modules)\n"
                "- python-runtime.tar.gz (~30 MB)\n"
                "- wheels/ (all deps via pip download --platform)\n"
                "- install.sh or install.ps1\n"
                "- (Windows only) nssm-2.24.zip\n"
                "- (Linux only) systemd/illumio-collector.service\n"
                "- VERSION file\n\n"
                "Linux service uses /opt/illumio-collector/python/bin/python3 in ExecStart.\n"
                "Windows NSSM points to C:\\illumio-collector\\python\\python.exe.\n\n"
                "Build scripts: scripts/build_offline_bundle.{sh,ps1}. "
                "Install scripts: scripts/install.{sh,ps1}."
            )},
        ],
    ),
    (
        "resolved-defaults",
        [
            {"role": "user", "content": "Frozen defaults for v1 implementation"},
            {"role": "assistant", "content": (
                "Spec §14 open questions frozen for v1 (plan header confirms):\n\n"
                "1. max_files_per_tick = 1000 (prevents first-run flood; ~1000 files x ~50 "
                "events = 50k events TLS can handle in 1 sec).\n\n"
                "2. CEF mapper SHIPPED in v1 as secondary option (not default). Includes "
                "mappings/auditable.yaml and mappings/summaries.yaml field maps. Not required "
                "for basic usage - syslog_json default works out of the box.\n\n"
                "3. initial_lookback_hours default = 0 (fresh start = from now). User sets "
                "24 for 1-day backfill, 720 for 30 days.\n\n"
                "OTHER FROZEN DEFAULTS:\n"
                "- flatten: true, separator: '_', max_depth: 10, array_strategy: stringify\n"
                "- retry_backoff_sec: [1, 2, 4], max_retries: 3, timeout_sec: 10\n"
                "- UDP 1024 byte cap (truncate + warn)\n"
                "- TCP/TLS 8192 byte cap (truncate + warn)\n"
                "- HTTPS batch_size: 100, Content-Type: application/x-ndjson\n"
                "- logging: rotate 50 MB, keep 7 files, console on\n"
                "- poll_interval_sec minimum: 10\n"
                "- PRI for syslog header: 134 (local0.info)\n"
                "- appname: illumio-pce; procid: audit (auditable) or summary (pd*)"
            )},
        ],
    ),
    (
        "execution-state",
        [
            {"role": "user", "content": "Where am I in the workflow? What's next?"},
            {"role": "assistant", "content": (
                "As of 2026-04-20: brainstorming and writing-plans skills COMPLETE. "
                "Spec and plan both approved and committed. Code pushed to private GitHub repo: "
                "https://github.com/harry10148/illumio-s3-siem-collector (SSH remote, branch master).\n\n"
                "NEXT STEP: Execute the 29-task plan. User was offered two execution modes "
                "and needs to pick one when resuming:\n\n"
                "OPTION 1 (recommended by writing-plans skill): Subagent-Driven. "
                "Use superpowers:subagent-driven-development skill. Each task is a fresh "
                "subagent, main session reviews between tasks. Keeps main context clean.\n\n"
                "OPTION 2: Inline execution. Use superpowers:executing-plans skill. "
                "Run tasks in the same session with batch checkpoints.\n\n"
                "RESUME STEPS IN A NEW SESSION:\n"
                "1. git clone git@github.com:harry10148/illumio-s3-siem-collector.git\n"
                "2. cd illumio-s3-siem-collector\n"
                "3. Copy MSGI-S3-Access-Key.txt from secure local source (NOT in repo)\n"
                "4. pip install -r requirements-dev.txt\n"
                "5. Read docs/superpowers/plans/2026-04-20-illumio-s3-siem-collector.md\n"
                "6. Read docs/superpowers/specs/2026-04-20-illumio-s3-siem-collector-design.md\n"
                "7. Read Status.md and Task.md\n"
                "8. Start at plan Task 1\n\n"
                "REPO STATE: 12 files tracked, 1 commit on master. Nothing implemented yet - "
                "repo contains spec + plan + existing s3_log_checker.py + Illumio docs + "
                "requirements.txt + .gitignore. Actual collector code comes from executing the plan."
            )},
        ],
    ),
    (
        "real-bucket-data-facts",
        [
            {"role": "user", "content": "What did we learn from probing the real customer bucket?"},
            {"role": "assistant", "content": (
                "Real S3 bucket probed 2026-04-20 for Mingtai Fire & Marine "
                "(customer-granted access). Bucket: "
                "illumio-flow-ap-scp45-msig-mingtai-com-tw in ap-northeast-1.\n\n"
                "FILE COUNTS (probe result):\n"
                "- auditable/: 7,679 files, 4.0 MB, ~142/day\n"
                "- summaries/pd=0/ (allowed): 25,353 files, 14.4 MB, ~507/day\n"
                "- summaries/pd=1/ (potentially blocked): 43,792 files, 107.5 MB, ~876/day LARGEST\n"
                "- summaries/pd=2/ (blocked): 0 files - environment not yet in enforcement mode\n"
                "- summaries/pd=3/ (unknown): 7,984 files, ~190/day\n"
                "- Date range observed: 2026-03-02 to 2026-04-20 (~50 days retention)\n\n"
                "FILE FORMAT CONFIRMED:\n"
                "- 100% match pattern {YYYYMMDD}_{uuid}.jsonl.gz\n"
                "- 0 subdirectories (flat listing per prefix)\n"
                "- 0/10 mismatch between filename date and S3 LastModified\n"
                "- Single file contains 1-100 JSON Lines (gzipped)\n\n"
                "EVENT SCHEMA OBSERVED:\n"
                "- auditable: href, timestamp, pce_fqdn, created_by.{agent,ven}. "
                "NO src_ip/dst_ip (these are management events, not network traffic).\n"
                "- summaries: src_ip, dst_ip, proto, dst_port, pd, pd_qualifier, dir (I/O), "
                "un (user), pn (process name), fqdn, src_hostname, dst_hostname, "
                "optional src_labels/dst_labels (nested objects with app/env/role/loc/os).\n\n"
                "PCE: fqdn=ap-scp45.illum.io, org_id=4456569."
            )},
        ],
    ),
]

filters = {"AND": [{"user_id": USER}, {"metadata": {"project": PROJECT}}]}


def indexed(topic: str) -> bool:
    """Return True if a memory with this topic is retrievable from mem0."""
    r = client.search(
        query=topic.replace("-", " "),
        filters=filters, version="v2", limit=10,
    )
    items = r.get("results", []) if isinstance(r, dict) else r
    for m in items:
        if isinstance(m, dict):
            if (m.get("metadata") or {}).get("topic") == topic:
                return True
    return False


succeeded = []
failed = []

for topic, messages in TOPICS:
    print(f"\n[{topic}] saving ...")
    result = client.add(messages=messages, user_id=USER,
                        metadata={"project": PROJECT, "topic": topic})
    print(f"  -> {result}")
    print(f"  polling for extraction ...")
    for attempt in range(12):  # up to 120 seconds
        time.sleep(10)
        if indexed(topic):
            print(f"  ✓ indexed after ~{(attempt + 1) * 10}s")
            succeeded.append(topic)
            break
    else:
        print(f"  ✗ still not indexed after 120s")
        failed.append(topic)

print("\n" + "=" * 70)
print(f"Resave result: {len(succeeded)} ok, {len(failed)} still failing")
print("=" * 70)
for t in succeeded:
    print(f"  ✓ {t}")
for t in failed:
    print(f"  ✗ {t}")

sys.exit(len(failed))
