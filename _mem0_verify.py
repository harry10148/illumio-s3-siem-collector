"""Verify all 8 topics are indexed in mem0 after async extraction."""
import sys
import time

sys.stdout.reconfigure(encoding="utf-8")

from mem0 import MemoryClient

client = MemoryClient()
USER = "harry10148"
PROJECT = "illumio_s3_collector"

EXPECTED = {
    "project-overview",
    "architecture-decisions",
    "s3-pull-algorithm",
    "fortisiem-constraints",
    "offline-deployment",
    "resolved-defaults",
    "execution-state",
    "real-bucket-data-facts",
}

# Queries crafted to pull each topic from vector index
probes = [
    "illumio collector project overview goal Mingtai",
    "multi pipeline source mapper sink architecture syslog_json default",
    "S3 pull LastModified checkpoint key tiebreak UUID random",
    "FortiSIEM UDP 1024 TCP 8192 TLS 6514 JSON parser rawupload",
    "offline deployment python-build-standalone bundle NSSM systemd",
    "max_files_per_tick CEF shipped lookback default resolved frozen",
    "execution state plan 29 tasks subagent driven inline",
    "real bucket data facts 85000 files auditable pd0 pd1 pd2 pd3 probe",
]

filters = {
    "AND": [
        {"user_id": USER},
        {"metadata": {"project": PROJECT}},
    ]
}

print("Waiting 30s for any in-flight extractions...")
time.sleep(30)

found_topics = {}   # topic -> (id, memory text snippet)
for q in probes:
    r = client.search(query=q, filters=filters, version="v2", limit=10)
    items = r.get("results", []) if isinstance(r, dict) else r
    for m in items:
        if isinstance(m, dict):
            topic = (m.get("metadata") or {}).get("topic", "?")
            found_topics.setdefault(topic, (m.get("id"), m.get("memory", "")[:160]))

print("\n" + "=" * 70)
print(f"Found {len(found_topics)} topics out of {len(EXPECTED)} expected")
print("=" * 70)
for t in sorted(found_topics):
    mid, snippet = found_topics[t]
    status = "✓" if t in EXPECTED else "?"
    print(f"{status} [{t}]")
    print(f"    id: {mid}")
    print(f"    {snippet}...")

missing = EXPECTED - set(found_topics.keys())
if missing:
    print("\n" + "!" * 70)
    print(f"MISSING: {sorted(missing)}")
    print("!" * 70)
else:
    print("\n✓ All 8 expected topics are indexed and retrievable.")

print("\nExit code:", len(missing))
sys.exit(len(missing))
