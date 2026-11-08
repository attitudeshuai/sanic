from collections import namedtuple

import pytest

from sanic_testing.reusable import ReusableClient

from sanic import text
from tests.client import RawClient


SIMPLE_REQUEST = b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"


@pytest.fixture
def test_app(app):
    app.config.KEEP_ALIVE_TIMEOUT = 1

    @app.get("/")
    async def root(request):
        return text("ok")

    @app.get("/dup")
    async def duplicated(request):
        return text(request.headers.x_dup)

    return app


@pytest.fixture
def strict_app(test_app):
    test_app.config.REQUEST_MAX_HEADER_FIELD_SIZE = 64
    test_app.config.REQUEST_MAX_HEADER_FIELD_COUNT = 5
    return test_app


def _client(app, port):
    runner = ReusableClient(app, port=port)
    runner.run()
    raw = RawClient(runner.host, runner.port)
    runner._run(raw.connect())
    client = namedtuple("Client", ("raw", "send", "recv"))

    def send(msg):
        runner._run(raw.send(msg))

    def recv():
        return runner._run(raw.recv())

    try:
        yield client(raw, send, recv)
    finally:
        runner._run(raw.close())
        runner.stop()


@pytest.fixture
def client(test_app, port):
    yield from _client(test_app, port)


@pytest.fixture
def strict_client(strict_app, port):
    yield from _client(strict_app, port)


def test_oversized_header_field_431_and_close(strict_client):
    long_header = b"X-Long: " + b"a" * 100
    strict_client.send(
        b"GET / HTTP/1.1\r\nHost: localhost\r\n"
        + long_header
        + b"\r\n\r\n"
        + SIMPLE_REQUEST
    )

    response = strict_client.recv()

    assert response.count(b"HTTP/1.1") == 1
    assert b"431 Request Header Fields Too Large" in response
    assert b"connection: close" in response.lower()
    # The pipelined second request is never processed: the connection is
    # closed and reading again yields EOF.
    assert strict_client.recv() == b""


def test_too_many_header_fields_431_and_close(strict_client):
    fields = b"".join(f"X-{index}: v\r\n".encode() for index in range(6))
    strict_client.send(
        b"GET / HTTP/1.1\r\nHost: localhost\r\n"
        + fields
        + b"\r\n"
        + SIMPLE_REQUEST
    )

    response = strict_client.recv()

    assert response.count(b"HTTP/1.1") == 1
    assert b"431 Request Header Fields Too Large" in response
    assert b"connection: close" in response.lower()
    assert strict_client.recv() == b""


def test_illegal_header_byte_400_and_close(strict_client):
    strict_client.send(
        b"GET / HTTP/1.1\r\nX-Bad: ab\x00cd\r\n\r\n" + SIMPLE_REQUEST
    )

    response = strict_client.recv()

    assert response.count(b"HTTP/1.1") == 1
    assert b"400 Bad Request" in response
    assert b"Bad Request" in response
    assert b"connection: close" in response.lower()
    assert strict_client.recv() == b""


def test_bare_line_feed_400_and_close(strict_client):
    strict_client.send(b"GET / HTTP/1.1\nX-Bad: value\n\n" + SIMPLE_REQUEST)

    response = strict_client.recv()

    assert response.count(b"HTTP/1.1") == 1
    assert b"400 Bad Request" in response
    assert b"connection: close" in response.lower()
    assert strict_client.recv() == b""


def test_small_request_accepted_under_lowered_quotas(strict_client):
    strict_client.send(SIMPLE_REQUEST)

    response = strict_client.recv()

    assert b"200 OK" in response
    assert b"connection: keep-alive" in response.lower()


def test_normal_requests_reuse_connection(client):
    client.send(SIMPLE_REQUEST + SIMPLE_REQUEST)

    response = client.recv()

    assert response.count(b"HTTP/1.1") == 2
    assert response.count(b"200 OK") == 2
    assert response.lower().count(b"connection: keep-alive") == 2


def test_duplicate_headers_keep_merge_semantics(client):
    client.send(
        b"GET /dup HTTP/1.1\r\n"
        b"Host: localhost\r\n"
        b"X-Dup: one\r\n"
        b"X-Dup: two\r\n"
        b"\r\n"
    )

    response = client.recv()
    _, body = response.rsplit(b"\r\n\r\n", 1)

    assert body == b"one,two"
    assert b"200 OK" in response
    assert b"connection: keep-alive" in response.lower()


def test_duplicate_content_length_still_rejected(client):
    client.send(
        b"POST / HTTP/1.1\r\n"
        b"Host: localhost\r\n"
        b"Content-Length: 0\r\n"
        b"Content-Length: 0\r\n"
        b"\r\n"
        + SIMPLE_REQUEST
    )

    response = client.recv()

    assert response.count(b"HTTP/1.1") == 1
    assert b"400 Bad Request" in response
    assert b"connection: close" in response.lower()
    assert client.recv() == b""
