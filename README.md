# Illumio S3 → SIEM Collector

Pull Illumio PCE logs (auditable events, traffic summaries) from an AWS S3
bucket, convert them to Syslog-JSON / CEF / JSON, and forward them to a SIEM
(designed for FortiSIEM) over UDP / TCP / TLS / HTTPS.

- Multi-pipeline: each log type can go to a different destination with its
  own poll interval, format, and filter.
- Built-in scheduler (APScheduler) — no external cron / Task Scheduler.
- Checkpoint via atomic JSON file; resumes after restart with at-least-once
  semantics (SIEM must tolerate duplicates; FortiSIEM rule-based dedup works).
- Offline-installable bundles for Linux and Windows: **target needs no
  Python, no pip, no internet**.

## Requirements

**Target host:**
- x86_64 CPU
- Linux (glibc ≥ 2.17) or Windows 10 / Server 2016+

That's it. No Python, no pip, no internet on the target.

**Build host (for packaging):** Python 3.11 + pip + internet.

## Quick start (dev)

```bash
pip install -r requirements-dev.txt
cp config.example.yaml config.yaml
$EDITOR config.yaml
python collector.py --config config.yaml --dry-run
python collector.py --config config.yaml --once audit-to-fortisiem
python collector.py --config config.yaml
```

## Offline install

### Linux

```bash
./scripts/build_offline_bundle.sh
# -> dist/illumio-collector-linux-x86_64-v1.0.tar.gz
```

On the target:
```bash
tar xzf illumio-collector-linux-x86_64-v1.0.tar.gz
cd bundle
sudo ./install.sh
sudo vi /etc/illumio-collector/config.yaml
sudo systemctl start illumio-collector
sudo journalctl -u illumio-collector -f
```

### Windows

PowerShell on build host:
```powershell
.\scripts\build_offline_bundle.ps1
# -> dist\illumio-collector-windows-x86_64-v1.0.zip
```

Administrator PowerShell on target:
```powershell
Expand-Archive illumio-collector-windows-x86_64-v1.0.zip C:\illumio-bundle
cd C:\illumio-bundle
.\install.ps1
notepad C:\illumio-collector\config.yaml
& "C:\illumio-collector\nssm\nssm-2.24\win64\nssm.exe" start IllumioCollector
Get-Content C:\illumio-collector\logs\collector.log -Wait
```

## Configuration

See `config.example.yaml` for every option with comments.

| Pipeline field | Purpose |
|---|---|
| `log_type` | `auditable`, `pd0`, `pd1`, `pd2`, or `pd3` |
| `poll_interval_sec` | how often to pull (min 10) |
| `max_files_per_tick` | bound on files processed per tick (default 1000) |
| `filter.expression` | safe Python-like boolean using `ev.*` |
| `mapper.format` | `syslog_json` (default), `cef`, or `json` |
| `mapper.flatten` | collapse nested JSON (default true) |
| `sink.type` | `udp`, `tcp`, `tls`, or `https` |

### Filter examples

```yaml
filter:
  expression: "ev.pd == 2"
  expression: "ev.dst_port in (22, 445, 3389)"
  expression: "ev.created_by.agent.hostname != 'healthcheck'"
  expression: "'login' in str(ev.notifications)"
```

### FortiSIEM setup

Import parsers from `fortisiem_parser/`. See
`fortisiem_parser/README.md` for step-by-step instructions.

## Operations

Checkpoints live at `<state_dir>/<pipeline_name>.json`. Delete to replay
from the configured `initial_lookback_hours`.

### Troubleshooting

```bash
python s3_log_checker.py --bucket <B> --fqdn <F> --org-id <ID> \
    --access-key <AK> --secret-key <SK>          # S3 connectivity
python collector.py --config config.yaml --dry-run
python collector.py --config config.yaml --once <pipeline-name>
```

### Upgrading

1. Stop the service
2. Re-run `install.sh` / `install.ps1` from the new bundle
3. Config (`/etc/illumio-collector/` or `C:\illumio-collector\config.yaml`)
   and state are preserved
4. Start the service

## Architecture

Full design: `docs/superpowers/specs/2026-04-20-illumio-s3-siem-collector-design.md`.

```
  Source (S3) -> Mapper (flatten + format) -> Sink (UDP/TCP/TLS/HTTPS)
                        |                          |
                    filter (opt)               retry + backoff
```

## License

TBD
