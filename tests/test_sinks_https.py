import json

import responses

from sinks.https_sink import HttpsSink


URL = "https://fsm.example.com/rawupload?vendor=Illumio&model=PCE"


@responses.activate
def test_https_batches_requests():
    responses.add(responses.POST, URL, status=200)
    sink = HttpsSink(url=URL, batch_size=3, max_retries=0)
    for i in range(3):
        sink.send(json.dumps({"i": i}).encode("utf-8"))
    sink.close()
    assert len(responses.calls) == 1
    body = responses.calls[0].request.body
    lines = body.decode("utf-8").strip().split("\n")
    assert len(lines) == 3


@responses.activate
def test_https_flushes_on_close():
    responses.add(responses.POST, URL, status=200)
    sink = HttpsSink(url=URL, batch_size=100, max_retries=0)
    sink.send(b'{"x":1}')
    sink.send(b'{"x":2}')
    assert len(responses.calls) == 0
    sink.close()
    assert len(responses.calls) == 1


@responses.activate
def test_https_retry_on_500_then_success():
    responses.add(responses.POST, URL, status=500)
    responses.add(responses.POST, URL, status=200)
    sink = HttpsSink(url=URL, batch_size=1, max_retries=1,
                     retry_backoff_sec=[0.01])
    assert sink.send(b'{"x":1}') is True
    sink.close()
    assert len(responses.calls) == 2


@responses.activate
def test_https_returns_false_after_all_retries():
    responses.add(responses.POST, URL, status=500)
    responses.add(responses.POST, URL, status=500)
    responses.add(responses.POST, URL, status=500)
    sink = HttpsSink(url=URL, batch_size=1, max_retries=2,
                     retry_backoff_sec=[0.01, 0.01])
    assert sink.send(b'{"x":1}') is False
    sink.close()


@responses.activate
def test_https_failed_flush_clears_buffer():
    """On permanent flush failure, buffer must be cleared so pipeline replay
    (driven by un-advanced checkpoint) does not produce duplicates."""
    responses.add(responses.POST, URL, status=500)
    sink = HttpsSink(url=URL, batch_size=1, max_retries=0)

    assert sink.send(b'{"x":1}') is False
    assert sink.buffer == []
    sink.session.close()


@responses.activate
def test_https_flush_api_sends_partial_batch():
    responses.add(responses.POST, URL, status=200)
    sink = HttpsSink(url=URL, batch_size=100, max_retries=0)
    sink.send(b'{"x":1}')
    sink.send(b'{"x":2}')

    assert sink.flush() is True
    assert sink.buffer == []
    sink.close()


@responses.activate
def test_flush_failure_clears_buffer_no_duplicates():
    """Simulate the duplication scenario:
    - tick 1 fills a batch and the flush fails permanently
    - pipeline does not advance the checkpoint and re-reads the same file
    - tick 2 sends the same events again and succeeds

    The SIEM must NOT see the failed batch's events on top of the replay.
    Concretely: total POST bodies for the second (successful) flush should
    contain exactly the replayed events, not the prior buffer concatenated.
    """
    # Tick 1: batch of 2 events, flush fails (500).
    responses.add(responses.POST, URL, status=500)
    # Tick 2: replay of the same 2 events, flush succeeds (200).
    responses.add(responses.POST, URL, status=200)

    sink = HttpsSink(url=URL, batch_size=2, max_retries=0)

    # Tick 1
    assert sink.send(b'{"i":1}') is True       # buffered
    assert sink.send(b'{"i":2}') is False      # batch flush -> 500 -> fail
    # Buffer must be cleared so a replay does not get appended to old events.
    assert sink.buffer == []

    # Tick 2: pipeline re-reads the same file and re-sends both events.
    assert sink.send(b'{"i":1}') is True
    assert sink.send(b'{"i":2}') is True       # batch flush -> 200 -> ok
    assert sink.buffer == []

    # Two POST calls total: one failed, one succeeded.
    assert len(responses.calls) == 2

    # Each POST body must contain exactly 2 NDJSON lines (the originals on
    # tick 1, the replay on tick 2). If the buffer were not cleared on
    # failure, the second body would contain 4 lines (duplicates).
    for call in responses.calls:
        lines = [ln for ln in call.request.body.decode("utf-8").split("\n") if ln]
        assert len(lines) == 2

    sink.close()
