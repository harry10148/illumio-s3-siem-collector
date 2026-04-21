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
def test_https_failed_flush_blocks_new_append_until_recovered():
    responses.add(responses.POST, URL, status=500)
    responses.add(responses.POST, URL, status=500)
    sink = HttpsSink(url=URL, batch_size=1, max_retries=0)

    assert sink.send(b'{"x":1}') is False
    assert len(sink.buffer) == 1
    assert sink.send(b'{"x":2}') is False
    assert len(sink.buffer) == 1
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
