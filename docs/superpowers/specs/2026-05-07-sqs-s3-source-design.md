# SQS-based S3 Source Design

**Date:** 2026-05-07
**Status:** Approved (pending implementation plan)
**Author:** brainstorm session

## 1. Background and Motivation

The collector today supports only **generic S3 polling** (`sources/s3_source.py`):
each pipeline calls `list_objects_v2` against a date-prefixed S3 path on every
tick, comparing `LastModified` against a per-pipeline checkpoint. This works
but has three operational drawbacks at higher volume:

1. List-then-fetch latency — events arrive in S3 several seconds before the
   next polling tick discovers them.
2. List API cost grows linearly with bucket size and tick frequency.
3. The customer's Illumio tenant already publishes an **S3 Event Notification
   → SNS → SQS** delivery chain. The SQS queue is provisioned and named for
   them by Illumio (e.g. `illumio-flow-ap-scp45-msig-mingtai-com-tw`). Today
   the collector ignores that queue.

This spec adds a second source mode, `sqs_s3`, that consumes the existing
SQS queue and downloads objects on demand. Generic polling stays available
and unchanged; choice is per-deployment via `source.type`.

## 2. Constraints from Illumio's Deployment Shape

The customer is given **one SQS queue per PCE/tenant**, not per log type.
The queue receives all `s3:ObjectCreated:*` events for the tenant's bucket,
which means a single consumer must dispatch by S3 key path to the right
log-type-specific pipeline. This is a fact of the customer's setup, not a
collector design choice.

The S3 key layout (already encoded in `S3Source._LOG_TYPE_PATH`) is:

```
{fqdn}/org_id={org_id}/auditable/...                → log_type=auditable
{fqdn}/org_id={org_id}/summaries/pd=0/...           → log_type=pd0
{fqdn}/org_id={org_id}/summaries/pd=1/...           → log_type=pd1
{fqdn}/org_id={org_id}/summaries/pd=2/...           → log_type=pd2
{fqdn}/org_id={org_id}/summaries/pd=3/...           → log_type=pd3
```

The dispatcher reuses this mapping (do not duplicate; expose from `s3_source`).

## 3. Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│  collector.py                                                     │
│    cfg = load_config(...)                                         │
│    if cfg.source.type == "s3":                                    │
│        scheduler = build_polling_scheduler(cfg)                   │
│        scheduler.run_forever()                  ◄── unchanged     │
│    elif cfg.source.type == "sqs_s3":                              │
│        dispatcher = build_sqs_dispatcher(cfg)                     │
│        dispatcher.run_forever()                 ◄── new           │
└──────────────────────────────────────────────────────────────────┘

generic S3 path (existing, untouched):
   PipelineScheduler → Pipeline.tick() → S3Source.iter_new_files()

SQS path (new):
   SqsS3Dispatcher (one long-poll thread)
     → sqs.receive_message(WaitTimeSeconds=20, Max=10)
     → for each message:
         parse SNS-wrapped S3 event
         look up log_type by key prefix
         find matching SqsPipeline (by log_type)
         s3.get_object → gunzip
         SqsPipeline.process(body)
         on success: sqs.delete_message
```

**No code is shared between `Pipeline` and `SqsPipeline`.** The two classes
have similar inner loops (`for line in body.splitlines(): json → filter →
map → sink`), but the user explicitly chose independence over reuse, and the
duplication is small (~30 lines). `Pipeline` is not modified.

### 3.1 New components

| File | Purpose |
|------|---------|
| `core/sqs_pipeline.py` | `SqsPipeline` — given a decoded body, runs JSON-lines parse → filter → mapper.format → sink.send → sink.flush. Returns success bool and stats. **No checkpoint.** |
| `sources/sqs_s3_source.py` | `SqsS3Dispatcher` — long-poll consumer thread, log-type routing, visibility extension, delete on success, stop-flag for graceful shutdown. |

### 3.2 Modified components

| File | Change |
|------|--------|
| `core/config.py` | `SourceConfig` becomes a discriminated union: `S3SourceConfig` (current) ∪ `SqsS3SourceConfig` (new). |
| `core/pipeline.py::build_pipelines_from_config` | Branch by `cfg.source.type`. Generic returns `list[(Pipeline, poll_interval)]` as today. SQS returns `(SqsS3Dispatcher, list[SqsPipeline])`. |
| `collector.py` | Add the if/elif on `cfg.source.type` to dispatch to the right runner. Banner shows mode. |
| `config.example.yaml` | Add a commented-out SQS source example. |
| README, OPERATIONS | Document the new mode. |

### 3.3 Unchanged

Mappers, sinks, filters, the existing `Pipeline`, `PipelineScheduler`, and
`S3Source` are not touched. Pipeline schema (`pipelines.*.log_type`,
`mapper`, `sink`, `filter`) is identical between modes — `poll_interval_sec`
and `max_files_per_tick` are silently ignored under SQS (documented, no
validator).

## 4. Data Flow and Failure Handling

### 4.1 Happy path

1. `receive_message(WaitTimeSeconds=20, MaxNumberOfMessages=10)`.
2. For each message:
   1. Parse body. Illumio publishes via SNS, so the body is a JSON string
      containing the SNS envelope; the actual S3 event is in `Message`.
      Handle both raw S3 events and SNS-wrapped S3 events.
   2. Extract `bucket.name` and `object.key` from the first record.
      Confirm bucket matches `cfg.source.bucket` (defensive — reject otherwise).
   3. Look up `log_type` by walking `_LOG_TYPE_PATH` against the key, after
      stripping the `{fqdn}/org_id={org_id}/` prefix.
   4. Find the `SqsPipeline` registered for that `log_type`. If none, log INFO
      and delete (user explicitly did not enable that pipeline).
   5. `s3.get_object(Bucket=..., Key=...)`, `gzip.decompress` the body.
   6. Call `SqsPipeline.process(decoded_body)`. Returns True on full success
      (all events sent and flushed), False otherwise.
   7. On True: `sqs.delete_message(ReceiptHandle=...)`.
   8. On False: do not delete; SQS will redeliver after visibility timeout.

### 4.2 Visibility extension

If processing time approaches `visibility_timeout_sec / 2`, the **dispatcher**
calls `sqs.change_message_visibility(VisibilityTimeout=visibility_extension_sec)`.
The dispatcher records `start_time` when each message enters processing, and
before each major step (download, gunzip, hand-off to `SqsPipeline.process`)
checks `time.monotonic() - start_time` against the threshold and extends if
needed. The `SqsPipeline` itself remains a pure transformer with no SQS
awareness.

This guards against duplicates from SQS reissuing a message we are still
processing.

### 4.3 Failure matrix

| Stage | Failure | Action |
|---|---|---|
| SNS / S3 event JSON parse | Malformed message | log ERROR, **don't delete** (DLQ catches) |
| Bucket mismatch | Message bucket ≠ configured bucket | log ERROR, **don't delete** (configuration error or wrong queue) |
| log_type lookup by key | Key path doesn't match any known pattern | log WARN, **delete** (Illumio added a new log type — needs code change, but don't block queue) |
| log_type → pipeline lookup | log_type not enabled in config | log INFO, **delete** (user explicitly opted out) |
| `s3.get_object` | Network / permission | log ERROR, **don't delete** |
| gunzip | Corrupt object | log ERROR, **don't delete** (DLQ catches truly bad objects) |
| Mapper failure | Per-event JSON or schema error | increment `mapper_err`, **continue** to next event in same file |
| Sink send / flush | Network or sink config error | log ERROR, **don't delete** |

The general rule: only delete when we are sure the message is irrelevant to
us (unknown log_type, opted-out log_type). Every processing failure relies
on SQS redelivery + the queue's DLQ for permanent failures. This pushes
poison-message handling to AWS where it belongs.

### 4.4 Concurrency

A single consumer thread; messages within a `receive_message` batch of up
to 10 are processed sequentially. Each S3 object already contains many
events, so single-threaded throughput is enough for typical Illumio output
(summaries every ~10 minutes per `pd=N`, auditable somewhat denser).

`max_workers: int = 1` is added to `SqsS3SourceConfig` as a forward-compat
knob. The initial implementation honors only `max_workers=1`; values > 1
are accepted by config validation but log a warning that parallel mode is
not yet implemented. Adding parallelism later is a wrap-loop change, not
an architectural one.

### 4.5 Graceful shutdown

`SqsS3Dispatcher.run_forever()` blocks on the main thread and runs the
consumer loop in-thread (no separate worker thread is needed for
`max_workers=1`). A signal handler (`signal.signal(SIGTERM, ...)` plus
`KeyboardInterrupt`) sets `self._stop_event`.

On stop:

1. The consumer loop checks `_stop_event` at the top of each iteration. If
   set, it exits the loop **after** the current message finishes processing
   (do not abandon mid-message — that would cause a duplicate after
   visibility timeout).
2. After the loop exits, the main thread calls `sink.close()` for every
   registered `SqsPipeline`'s sink.
3. `run_forever()` returns; `collector.py` exits.

A bounded shutdown timeout (e.g. 30 seconds via `_stop_event.wait(30)` after
the loop is told to stop) is acceptable; a hard kill will simply cause SQS
redelivery, which is correct.

## 5. Configuration Schema

### 5.1 New `SqsS3SourceConfig`

```python
class SqsS3SourceConfig(BaseModel):
    type: Literal["sqs_s3"] = "sqs_s3"
    queue_url: str
    bucket: str                           # for defensive validation
    fqdn: str                             # for prefix matching
    org_id: str                           # for prefix matching
    visibility_timeout_sec: int = 60
    visibility_extension_sec: int = 60
    wait_time_sec: int = 20               # SQS max
    max_messages_per_receive: int = 10    # SQS max
    max_workers: int = 1                  # forward-compat; only 1 honored initially
```

`SourceConfig` becomes:

```python
SourceConfig = Annotated[
    Union[S3SourceConfig, SqsS3SourceConfig],
    Field(discriminator="type"),
]
```

### 5.2 Example config

```yaml
source:
  type: sqs_s3
  queue_url: https://sqs.ap-southeast-1.amazonaws.com/953953587837/illumio-flow-ap-scp45-msig-mingtai-com-tw
  bucket: illumio-flow-ap-scp45-msig-mingtai-com-tw
  fqdn:   ap-scp45.illum.io
  org_id: 123456
  # Optional knobs (defaults shown):
  # visibility_timeout_sec: 60
  # visibility_extension_sec: 60
  # wait_time_sec: 20
  # max_messages_per_receive: 10
  # max_workers: 1

aws:
  region: ap-southeast-1
  # profile: illumio
  # access_key: ...
  # secret_key: ...

# Pipelines section is identical to generic mode.
# In SQS mode, poll_interval_sec and max_files_per_tick are ignored.
pipelines:
  - name: audit
    enabled: true
    log_type: auditable
    mapper: { format: cef, mapping_file: mappings/auditable.yaml }
    sink: { type: tls, host: siem.example.com, port: 6514 }
  - name: blocked
    enabled: true
    log_type: pd2
    mapper: { format: cef, mapping_file: mappings/summaries.yaml }
    sink: { type: tls, host: siem.example.com, port: 6514 }
```

### 5.3 Authentication

SQS and S3 clients are constructed from the same boto3 `Session` already
built in `build_pipelines_from_config`. No new credential mechanism. The
session must have `sqs:ReceiveMessage`, `sqs:DeleteMessage`,
`sqs:ChangeMessageVisibility` on the queue, and `s3:GetObject` on the
bucket (existing requirement).

### 5.4 Banner

Banner output in `collector.py` shows the active mode:

```
Illumio S3 -> SIEM Collector v1.x
  source:    sqs_s3 (queue: ...mingtai-com-tw)
  bucket:    illumio-flow-ap-scp45-msig-mingtai-com-tw
  pce:       ap-scp45.illum.io / org_id=123456
  pipelines: 2 enabled (auditable, pd2)
```

## 6. Testing Strategy

Use `moto` to stub SQS and S3 in process. `requirements-dev.txt` currently
declares `moto[s3]>=5.0`; bump to `moto[s3,sqs]>=5.0` so the `mock_aws`
decorator covers SQS too.

`tests/test_sqs_pipeline.py`:

- Given a small JSON-lines body, mapper, and stub sink, `process()` returns
  True and produces the expected wire output.
- A mid-batch sink failure causes `process()` to return False.
- Mapper exception on one line increments `mapper_err` and processing continues.

`tests/test_sqs_s3_dispatcher.py`:

- Happy path: enqueue an SNS-wrapped S3 event for an `auditable` key →
  configured `SqsPipeline` receives the body → message is deleted from the
  fake SQS queue.
- Unknown key path → message is deleted with WARN logged.
- log_type with no enabled pipeline → message is deleted with INFO logged.
- `s3.get_object` raising → message is **not** deleted.
- Sink failure → message is **not** deleted.
- Slow handler → `change_message_visibility` is called.
- Stop event set → consumer thread exits cleanly after current message.

The full suite (currently 92 tests) must remain green; new tests bring it
to ≥ 100.

## 7. Implementation Roadmap

| Step | Files | Estimated LoC | Notes |
|---|---|---|---|
| 1 | `core/config.py` | +25 | Discriminated union + `SqsS3SourceConfig`. |
| 2 | `core/sqs_pipeline.py` | +80 | New `SqsPipeline` class. |
| 3 | `sources/sqs_s3_source.py` | +180 | New dispatcher + thread + routing. |
| 4 | `core/pipeline.py` | +60 / -5 | Factory branches into SQS path returning `(dispatcher, [SqsPipeline...])`. |
| 5 | `collector.py` | +25 / -5 | Mode dispatch + banner update. |
| 6 | `config.example.yaml` | +25 | Commented-out SQS example. |
| 7 | `tests/test_sqs_pipeline.py` + `tests/test_sqs_s3_dispatcher.py` | +250 | moto-based suite. |
| 8 | `README.md`, `docs/OPERATIONS.md` | +60 | New section per file. |

Total: ≈ 700 net LoC, single coherent feature, no decomposition needed.

## 8. Out of Scope

- **Raw SQS body as log payload.** This spec is for SNS-wrapped S3 event
  notifications only. If raw payload support is needed later, add a
  separate `sqs_raw` source type.
- **Multi-thread / worker pool.** `max_workers > 1` is reserved in schema
  but not implemented in this spec. Future change.
- **Per-pipeline SQS queue.** Illumio gives one queue per tenant; this
  spec assumes that shape. If a customer ever runs multiple tenants in one
  collector, they run multiple collectors today.
- **Cross-account SQS.** Out of scope; assumes the queue is in the same
  account as the bucket and the AWS credentials already used.
- **`s3_log_checker.py --test-sqs-config`** (configuration sanity helper).
  Nice-to-have, not in this spec.

## 9. Open Questions

None remaining at design time. Implementation may surface decisions about
log message wording or specific `moto` test fixtures that do not affect
the architecture.
