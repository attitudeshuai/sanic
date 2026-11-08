import asyncio

import pytest

from sanic_testing.reusable import ReusableClient

from sanic import Request, Sanic
from sanic.exceptions import ServerError
from sanic.helpers import is_protected_trailer_header
from sanic.response import text
from tests.client import RawClient


TRAILER_NAMES = ("X-Checksum", "X-Duration")


def build_app(app: Sanic) -> Sanic:
    app.config.KEEP_ALIVE_TIMEOUT = 5
    app.ctx.completed_connections = 0
    app.ctx.last_exception = None

    @app.signal("http.lifecycle.complete")
    async def _mark_connection_complete(**_):
        app.ctx.completed_connections += 1

    @app.signal("http.lifecycle.exception")
    async def _record_exception(**context):
        app.ctx.last_exception = context["exception"]

    @app.get("/trailers")
    async def with_trailers(request: Request):
        response = await request.respond(content_type="text/plain")
        response.declare_trailers(list(TRAILER_NAMES))
        await response.send("foo,")
        await asyncio.sleep(0.001)
        await response.send("bar")
        response.trailers["X-Checksum"] = "deadbeef"
        response.trailers["X-Duration"] = "42ms"
        await response.eof()

    @app.get("/plain")
    async def without_trailers(request: Request):
        response = await request.respond(content_type="text/plain")
        await response.send("foo,")
        await asyncio.sleep(0.001)
        await response.send("bar")
        await response.eof()

    @app.get("/args-style")
    async def args_style(request: Request):
        response = await request.respond(content_type="text/plain")
        response.declare_trailers("X-One", "X-Two")
        await response.send("ok")
        response.trailers["X-One"] = "1"
        response.trailers["X-Two"] = "2"
        await response.eof()

    @app.get("/boom")
    async def raises_mid_body(request: Request):
        response = await request.respond(content_type="text/plain")
        response.declare_trailers(["X-Checksum"])
        await response.send("partial")
        raise RuntimeError("boom")

    @app.get("/slow")
    async def slow_stream(request: Request):
        response = await request.respond(content_type="text/plain")
        response.declare_trailers(["X-Checksum"])
        await response.send("chunk-one")
        await asyncio.sleep(0.3)
        await response.send("chunk-two")
        response.trailers["X-Checksum"] = "cafe"
        await response.eof()

    @app.get("/missing")
    async def missing_value(request: Request):
        response = await request.respond(content_type="text/plain")
        response.declare_trailers(["X-Checksum"])
        await response.send("body")
        await response.eof()

    @app.get("/undeclared")
    async def undeclared_value(request: Request):
        response = await request.respond(content_type="text/plain")
        response.declare_trailers(["X-Checksum"])
        await response.send("body")
        response.trailers["X-Checksum"] = "cafe"
        response.trailers["X-Extra"] = "nope"
        await response.eof()

    @app.get("/protected")
    async def protected_name(request: Request):
        response = await request.respond(content_type="text/plain")
        response.declare_trailers([request.args.get("name", "host")])
        await response.send("body")
        await response.eof()

    @app.get("/fixed")
    async def fixed_length(request: Request):
        response = await request.respond(
            content_type="text/plain",
            headers={"Content-Length": "4"},
        )
        response.declare_trailers(["X-Checksum"])
        await response.send("body")
        await response.eof()

    @app.get("/bodyless")
    async def bodyless(request: Request):
        response = await request.respond(status=204)
        response.declare_trailers(["X-Checksum"])
        await response.eof()

    @app.get("/late")
    async def late_declaration(request: Request):
        response = await request.respond(content_type="text/plain")
        await response.send("body")
        response.declare_trailers(["X-Checksum"])
        await response.eof()

    @app.get("/redeclare")
    async def redeclare(request: Request):
        response = await request.respond(content_type="text/plain")
        response.declare_trailers(["X-Checksum"])
        if request.args.get("same"):
            response.declare_trailers(["x-checksum"])
        else:
            response.declare_trailers(["X-Duration"])
        await response.send("body")
        response.trailers["X-Checksum"] = "cafe"
        await response.eof()

    @app.get("/disabled")
    async def disabled(request: Request):
        request.app.config.TRAILERS_ENABLED = False
        response = await request.respond(content_type="text/plain")
        response.declare_trailers(["X-Checksum"])
        await response.eof()

    @app.route("/head", methods=["GET", "HEAD"])
    async def head_target(request: Request):
        response = await request.respond(content_type="text/plain")
        response.declare_trailers(["X-Checksum"])
        await response.send("body")
        response.trailers["X-Checksum"] = "cafe"
        await response.eof()

    @app.get("/single-shot")
    async def single_shot(request: Request):
        response = text("hello", content_type="text/plain")
        response.declare_trailers(["X-Checksum"])
        response.trailers["X-Checksum"] = "cafe"
        return response

    @app.get("/asgi-trail")
    async def asgi_trail(request: Request):
        response = await request.respond(content_type="text/plain")
        response.declare_trailers(["X-Checksum"])
        await response.send("body")
        await response.eof()

    return app


@pytest.fixture
def trailer_app(app):
    return build_app(app)


@pytest.fixture
def runner(trailer_app, port):
    client = ReusableClient(trailer_app, port=port)
    client.run()
    yield client
    client.stop()


@pytest.fixture
def raw(runner):
    client = RawClient(runner.host, runner.port)
    runner._run(client.connect())
    yield client
    runner._run(client.close())


def _get(path: str) -> bytes:
    return (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: localhost\r\n"
        f"Connection: keep-alive\r\n"
        f"\r\n"
    ).encode()


async def _read_idle(
    reader, idle: float = 0.25, timeout: float = 5.0
) -> bytes:
    chunks = []
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            break
        try:
            chunk = await asyncio.wait_for(
                reader.read(65536), timeout=min(idle, remaining)
            )
        except asyncio.TimeoutError:
            break
        if not chunk:
            break
        chunks.append(chunk)
    return b"".join(chunks)


async def _read_eof(reader, timeout: float = 5.0) -> bytes:
    return await asyncio.wait_for(reader.read(-1), timeout=timeout)


def test_trailers_sent_before_end_marker(runner, raw):
    runner._run(raw.send(_get("/trailers")))
    message = runner._run(_read_idle(raw.reader))

    header_block, _, body = message.partition(b"\r\n\r\n")
    assert b"200 OK" in header_block
    assert b"transfer-encoding: chunked" in header_block
    assert b"trailer: X-Checksum, X-Duration" in header_block
    assert b"content-length" not in header_block

    # The trailer section sits between the last chunk and the final
    # empty line that ends the chunked message.
    assert body == (
        b"4\r\nfoo,\r\n"
        b"3\r\nbar\r\n"
        b"0\r\n"
        b"X-Checksum: deadbeef\r\n"
        b"X-Duration: 42ms\r\n"
        b"\r\n"
    )


def test_trailer_arguments_style(runner, raw):
    runner._run(raw.send(_get("/args-style")))
    message = runner._run(_read_idle(raw.reader))
    _, _, body = message.partition(b"\r\n\r\n")
    assert body == (b"2\r\nok\r\n0\r\nX-One: 1\r\nX-Two: 2\r\n\r\n")


def test_connection_still_reusable_after_trailers(runner, raw):
    runner._run(raw.send(_get("/trailers")))
    first = runner._run(_read_idle(raw.reader))
    assert b"X-Checksum: deadbeef\r\n" in first

    runner._run(raw.send(_get("/trailers")))
    second = runner._run(_read_idle(raw.reader))
    assert second.count(b"HTTP/1.1 200 OK") == 1
    assert b"X-Duration: 42ms\r\n\r\n" in second


def test_no_trailers_byte_sequence_unchanged(runner, raw):
    runner._run(raw.send(_get("/plain")))
    message = runner._run(_read_idle(raw.reader))

    header_block, _, body = message.partition(b"\r\n\r\n")
    assert b"transfer-encoding: chunked" in header_block
    assert b"trailer" not in header_block
    assert b"X-Checksum" not in message

    # Same chunk boundaries and the plain 0\r\n\r\n terminator as a
    # response without any trailer declaration.
    assert body == (b"4\r\nfoo,\r\n3\r\nbar\r\n0\r\n\r\n")


def test_handler_failure_mid_body_emits_no_trailer_and_closes(runner, raw):
    runner._run(raw.send(_get("/boom")))
    message = runner._run(_read_eof(raw.reader))

    _, _, body = message.partition(b"\r\n\r\n")
    assert body.startswith(b"7\r\npartial\r\n")
    # The announcement header may name the field, but no trailer field
    # line may be written into the body section.
    assert b"X-Checksum:" not in body
    # The chunked message was never terminated: no last-chunk marker.
    assert b"0\r\n" not in body

    # The server closed the connection instead of keeping it alive, so
    # it cannot be reused for another request on this socket.
    extra = runner._run(_read_eof(raw.reader, timeout=2.0))
    assert extra == b""


def test_peer_disconnect_mid_body_no_half_trailer(runner, raw, trailer_app):
    runner._run(raw.send(_get("/slow")))
    partial = runner._run(_read_idle(raw.reader, idle=0.2))
    _, _, body = partial.partition(b"\r\n\r\n")
    assert b"chunk-one" in body
    assert b"X-Checksum:" not in body

    before = trailer_app.ctx.completed_connections
    runner._run(raw.close())

    async def wait_teardown():
        for _ in range(50):
            if trailer_app.ctx.completed_connections > before:
                return True
            await asyncio.sleep(0.05)
        return False

    # The server tears this connection down instead of returning it to
    # the keep-alive pool, and no (half) trailer section is ever written.
    assert runner._run(wait_teardown()) is True


def test_missing_trailer_value_does_not_emit_section(runner, raw):
    runner._run(raw.send(_get("/missing")))
    message = runner._run(_read_eof(raw.reader))
    _, _, body = message.partition(b"\r\n\r\n")
    assert body.startswith(b"4\r\nbody\r\n")
    assert b"X-Checksum:" not in body
    assert not body.endswith(b"\r\n\r\n")


def test_undeclared_trailer_value_does_not_emit_section(runner, raw):
    runner._run(raw.send(_get("/undeclared")))
    message = runner._run(_read_eof(raw.reader))
    _, _, body = message.partition(b"\r\n\r\n")
    assert b"X-Extra:" not in body
    assert b"X-Checksum:" not in body


@pytest.mark.parametrize(
    "name",
    ["content-length", "transfer-encoding", "host", "content-type", "trailer"],
)
def test_protected_trailer_name_rejected(trailer_app, name):
    _, response = trailer_app.test_client.get(f"/protected?name={name}")
    assert response.status == 500
    assert is_protected_trailer_header(name)


def test_fixed_length_declaration_rejected(trailer_app):
    _, response = trailer_app.test_client.get("/fixed")
    assert response.status == 500


def test_bodyless_declaration_rejected(trailer_app):
    _, response = trailer_app.test_client.get("/bodyless")
    assert response.status == 500


def test_declaration_after_body_start_rejected(trailer_app):
    # The response is already streaming, so the error cannot be turned
    # into a new HTTP response; the deterministic rejection is delivered
    # through the exception lifecycle instead.
    _, response = trailer_app.test_client.get("/late")
    assert response.status == 200
    assert isinstance(trailer_app.ctx.last_exception, ServerError)
    assert "after the response body has started" in str(
        trailer_app.ctx.last_exception
    )


def test_redeclaration_rules(trailer_app):
    _, response = trailer_app.test_client.get("/redeclare?same=1")
    assert response.status == 200

    _, response = trailer_app.test_client.get("/redeclare")
    assert response.status == 500


def test_disabled_feature_rejects_declaration(trailer_app):
    _, response = trailer_app.test_client.get("/disabled")
    assert response.status == 500


def test_head_declaration_rejected(trailer_app):
    _, response = trailer_app.test_client.head("/head")
    assert response.status == 500


def test_single_shot_response_declaration_rejected(trailer_app):
    _, response = trailer_app.test_client.get("/single-shot")
    assert response.status == 500


def test_happy_path_through_test_client(trailer_app):
    request, response = trailer_app.test_client.get("/trailers")
    assert response.status == 200
    assert response.text == "foo,bar"
    assert (
        response.headers["Trailer"].replace(" ", "") == "X-Checksum,X-Duration"
    )
    assert response.headers["Transfer-Encoding"] == "chunked"


@pytest.mark.asyncio
async def test_trailers_rejected_under_asgi(trailer_app):
    _, response = await trailer_app.asgi_client.get("/asgi-trail")
    assert response.status == 500


def test_declare_trailers_validates_names_directly(app):
    response = text("hello")
    with pytest.raises(ServerError):
        response.declare_trailers(["Content-Length"])
    with pytest.raises(ServerError):
        response.declare_trailers(["bad name"])
    with pytest.raises(ServerError):
        response.declare_trailers([])

    response.declare_trailers(["X-A", "x-a", "X-B"])
    assert response._trailer_names == ("X-A", "X-B")
    assert response.headers["trailer"] == "X-A, X-B"

    # Identical re-declaration is idempotent, a different one fails.
    response.declare_trailers(["x-b", "x-a"])
    with pytest.raises(ServerError):
        response.declare_trailers(["X-C"])
