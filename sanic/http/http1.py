from __future__ import annotations

from typing import TYPE_CHECKING, NoReturn


if TYPE_CHECKING:
    from sanic.request import Request
    from sanic.response import BaseHTTPResponse

from asyncio import CancelledError, sleep
from time import perf_counter

from sanic.compat import Header
from sanic.exceptions import (
    BadRequest,
    ExpectationFailed,
    HeaderFieldsTooLarge,
    PayloadTooLarge,
    RequestCancelled,
    ServerError,
    ServiceUnavailable,
)
from sanic.headers import format_http1_response
from sanic.helpers import has_message_body
from sanic.http.constants import Stage
from sanic.http.stream import Stream
from sanic.log import access_logger, error_logger, logger
from sanic.touchup import TouchUpMeta


HTTP_CONTINUE = b"HTTP/1.1 100 Continue\r\n\r\n"


class Http(Stream, metaclass=TouchUpMeta):
    """ "Internal helper for managing the HTTP/1.1 request/response cycle.

    Raises:
        BadRequest: If the request body or headers are malformed.
        Exception: If the request is malformed.
        ExpectationFailed: If the request is malformed.
        HeaderFieldsTooLarge: If the request headers exceed a quota.
        PayloadTooLarge: If the request body exceeds the size limit.
        RuntimeError: If the response status is invalid.
        ServerError: If the handler does not produce a response.
        ServerError: If the response is bigger than the content-length.
    """

    HEADER_CEILING = 16_384
    HEADER_MAX_SIZE = 0
    HEADER_MAX_FIELD_SIZE = HEADER_CEILING
    HEADER_MAX_FIELD_COUNT = HEADER_CEILING // 2
    __touchup__ = (
        "http1_request_header",
        "http1_response_header",
        "read",
    )
    __slots__ = [
        "_send",
        "_receive_more",
        "dispatch",
        "recv_buffer",
        "protocol",
        "expecting_continue",
        "stage",
        "keep_alive",
        "head_only",
        "request",
        "exception",
        "url",
        "request_body",
        "request_bytes",
        "request_bytes_left",
        "response",
        "response_func",
        "response_size",
        "response_bytes_left",
        "upgrade_websocket",
        "perft0",
    ]

    def __init__(self, protocol):
        self._send = protocol.send
        self._receive_more = protocol.receive_more
        self.recv_buffer = protocol.recv_buffer
        self.protocol = protocol
        self.keep_alive = True
        self.stage: Stage = Stage.IDLE
        self.dispatch = self.protocol.app.dispatch

    def init_for_request(self):
        """Init/reset all per-request variables."""
        self.exception = None
        self.expecting_continue: bool = False
        self.head_only = None
        self.request_body = None
        self.request_bytes = None
        self.request_bytes_left = None
        self.request_max_size = self.protocol.request_max_size
        self.request: Request = None
        self.response: BaseHTTPResponse = None
        self.upgrade_websocket = False
        self.url = None
        self.perft0 = None

    def __bool__(self):
        """Test if request handling is in progress"""
        return self.stage in (Stage.HANDLER, Stage.RESPONSE)

    async def http1(self):
        """HTTP 1.1 connection handler"""
        # Handle requests while the connection stays reusable
        while self.keep_alive and self.stage is Stage.IDLE:
            self.init_for_request()
            # Wait for incoming bytes (in IDLE stage)
            if not self.recv_buffer:
                await self._receive_more()
            self.stage = Stage.REQUEST
            try:
                # Receive and handle a request
                self.response_func = self.http1_response_header

                await self.http1_request_header()

                self.stage = Stage.HANDLER
                self.perft0 = perf_counter()
                self.request.conn_info = self.protocol.conn_info
                await self.protocol.request_handler(self.request)

                # Handler finished, response should've been sent
                if self.stage is Stage.HANDLER and not self.upgrade_websocket:
                    raise ServerError("Handler produced no response")

                if self.stage is Stage.RESPONSE:
                    await self.response.send(end_stream=True)
            except CancelledError as exc:
                # Write an appropriate response before exiting
                if not self.protocol.transport:
                    logger.info(
                        f"Request: {self.request.method} {self.request.url} "
                        "stopped. Transport is closed."
                    )
                    return
                e = (
                    RequestCancelled()
                    if self.protocol.conn_info.lost
                    else (self.exception or exc)
                )
                self.exception = None
                self.keep_alive = False
                await self.error_response(e)
            except Exception as e:
                # Write an error response
                await self.error_response(e)

            # Try to consume any remaining request body
            if self.request_body:
                if self.response and 200 <= self.response.status < 300:
                    error_logger.error(f"{self.request} body not consumed.")
                # Limit the size because the handler may have set it infinite
                self.request_max_size = min(
                    self.request_max_size, self.protocol.request_max_size
                )
                try:
                    async for _ in self:
                        pass
                except (BadRequest, PayloadTooLarge):
                    # The remaining body was malformed (e.g. a chunked
                    # trailer, which must never be reparsed as a smuggled
                    # request) or too large. Drop the connection. This little
                    # delay allows clients to push a small request into
                    # network buffers before we close the socket, so that
                    # they are then able to read the response.
                    await sleep(0.001)
                    self.keep_alive = False

            # Clean up to free memory and for the next request
            if self.request:
                self.request.stream = None
                if self.response:
                    self.response.stream = None

    async def http1_request_header(self):  # no cov
        """Receive and parse request header into self.request."""
        # Receive until full header is in buffer
        buf = self.recv_buffer
        pos = 0
        bytes_scanned = 0
        lines_scanned = 0
        field_count = 0

        while True:
            pos = buf.find(b"\r\n\r\n", pos)
            if pos != -1:
                break

            # Reject illegal bytes and bare CR/LF as soon as they arrive,
            # then enforce the per-field and field-count quotas on
            # complete lines, so reading stops before the terminating
            # blank line arrives whenever a rule is violated.
            bytes_scanned = self._scan_header_bytes(buf, bytes_scanned)
            lines_scanned, field_count = self._scan_header_quotas(
                buf, lines_scanned, field_count
            )

            pos = max(0, len(buf) - 3)
            if pos >= self.HEADER_MAX_SIZE:
                break

            await self._receive_more()

        if pos >= self.HEADER_MAX_SIZE:
            self._reject_header_too_large()

        # Framing, illegal byte and header quota validation. This only
        # adds rejection paths; accepted requests are parsed unchanged.
        head = buf[:pos]
        self._validate_request_head(head)

        # Parse header content
        try:
            raw_headers = head.decode(errors="surrogateescape")
            reqline, *split_headers = raw_headers.split("\r\n")
            method, self.url, protocol = reqline.split(" ")

            await self.dispatch(
                "http.lifecycle.read_head",
                inline=True,
                context={"head": bytes(head)},
            )

            if protocol == "HTTP/1.1":
                self.keep_alive = True
            elif protocol == "HTTP/1.0":
                self.keep_alive = False
            else:
                raise Exception  # Raise a Bad Request on try-except

            self.head_only = method.upper() == "HEAD"
            request_body = False
            headers = []

            for name, value in (h.split(":", 1) for h in split_headers):
                name, value = h = name.lower(), value.lstrip()

                if name in ("content-length", "transfer-encoding"):
                    if request_body:
                        raise ValueError(
                            "Duplicate Content-Length or Transfer-Encoding"
                        )
                    request_body = True
                elif name == "connection":
                    self.keep_alive = value.lower() == "keep-alive"

                headers.append(h)
        except Exception:
            self._reject_bad_header()

        if not self.protocol.app.config.KEEP_ALIVE:
            self.keep_alive = False

        headers_instance = Header(headers)
        self.upgrade_websocket = (
            headers_instance.getone("upgrade", "").lower() == "websocket"
        )

        try:
            url_bytes = self.url.encode("ASCII")
        except UnicodeEncodeError:
            raise BadRequest("URL may only contain US-ASCII characters.")

        # Prepare a Request object
        request = self.protocol.request_class(
            url_bytes=url_bytes,
            headers=headers_instance,
            head=bytes(head),
            version=protocol[5:],
            method=method,
            transport=self.protocol.transport,
            app=self.protocol.app,
        )
        self.protocol.request_class._current.set(request)
        await self.dispatch(
            "http.lifecycle.request",
            inline=True,
            context={"request": request},
        )

        # Prepare for request body
        self.request_bytes_left = self.request_bytes = 0
        if request_body:
            headers = request.headers
            expect = headers.getone("expect", None)

            if expect is not None:
                if expect.lower() == "100-continue":
                    self.expecting_continue = True
                else:
                    raise ExpectationFailed(f"Unknown Expect: {expect}")

            if headers.getone("transfer-encoding", None) == "chunked":
                self.request_body = "chunked"
                pos -= 2  # One CRLF stays in buffer
            else:
                self.request_body = True
                try:
                    self.request_bytes_left = self.request_bytes = (
                        self._safe_int(headers["content-length"])
                    )
                except Exception:
                    raise BadRequest("Bad content-length")

        # Remove header and its trailing CRLF
        del buf[: pos + 4]
        self.request, request.stream = request, self
        self.protocol.state["requests_count"] += 1

    def _scan_header_bytes(
        self, buf: bytearray, scanned: int
    ) -> int:
        """Reject illegal bytes and bare CR/LF on buffered header data.

        Every byte up to the pending final byte is validated once. The
        final byte is left for the next pass because a trailing CR may
        be the first half of a CRLF whose LF has not arrived yet.
        """
        end = len(buf) - 1
        index = scanned
        while index < end:
            byte = buf[index]
            if byte == 0x0A:
                # An LF must be the second half of a CRLF token.
                if index == 0 or buf[index - 1] != 0x0D:
                    self._reject_bad_header()
            elif byte == 0x0D:
                # A CR must be followed by an LF.
                if buf[index + 1] != 0x0A:
                    self._reject_bad_header()
            elif (byte < 0x20 and byte != 0x09) or byte == 0x7F:
                self._reject_bad_header()
            index += 1
        return max(scanned, end)

    def _scan_header_quotas(
        self, buf: bytearray, scanned: int, field_count: int
    ) -> tuple[int, int]:
        """Check completed header lines against per-field quotas.

        Only lines terminated by CRLF in the currently buffered data are
        inspected, so a partially received line is rechecked once it is
        complete. Reading is stopped immediately when a quota is exceeded,
        before the terminating blank line arrives.
        """
        last_eol = buf.rfind(b"\r\n")
        if last_eol <= scanned:
            return scanned, field_count

        lines = bytes(buf[scanned:last_eol]).split(b"\r\n")
        if scanned == 0:
            # The first line is the request line, not a header field.
            lines = lines[1:]

        for line in lines:
            if line[:1] in (b" ", b"\t"):
                # Obsolete line folding (a continuation line) is invalid.
                self._reject_bad_header()
            field_count += 1
            if field_count > self.HEADER_MAX_FIELD_COUNT:
                self._reject_header_too_large()
            if len(line) > self.HEADER_MAX_FIELD_SIZE:
                self._reject_header_too_large()

        return last_eol + 2, field_count

    def _validate_request_head(self, head: bytes) -> None:
        """Validate framing, byte content and quotas of a header block.

        This only adds rejection paths. Requests that pass validation go
        through the existing parsing logic with identical value, casing
        and multi-value semantics.
        """
        lines = head.split(b"\r\n")
        field_lines = lines[1:]

        if len(field_lines) > self.HEADER_MAX_FIELD_COUNT:
            self._reject_header_too_large()

        for line in lines:
            # After splitting on the CRLF token any remaining CR/LF is a
            # bare carriage return or line feed.
            if b"\r" in line or b"\n" in line:
                self._reject_bad_header()
            # Obsolete line folding: a continuation line starting with
            # SP/HT is not accepted.
            if line[:1] in (b" ", b"\t"):
                self._reject_bad_header()
            # Horizontal tab is the only allowed control character.
            # NUL, other controls and DEL are illegal bytes.
            for byte in line:
                if (byte < 0x20 and byte != 0x09) or byte == 0x7F:
                    self._reject_bad_header()

        for line in field_lines:
            if len(line) > self.HEADER_MAX_FIELD_SIZE:
                self._reject_header_too_large()

    def _reject_header_too_large(self) -> NoReturn:
        """Reject an over-quota header block and seal the connection."""
        self.keep_alive = False
        self.recv_buffer.clear()
        raise HeaderFieldsTooLarge("Request header exceeds the size limit")

    def _reject_bad_header(self) -> NoReturn:
        """Reject a malformed header block and seal the connection.

        Buffered bytes, including any pipelined request following the
        malformed header, are dropped so they can never be parsed as a
        new request on this connection.
        """
        self.keep_alive = False
        self.recv_buffer.clear()
        raise BadRequest("Bad Request")

    async def http1_response_header(
        self, data: bytes, end_stream: bool
    ) -> None:  # no cov
        """Format response header and send it."""
        res = self.response

        # Compatibility with simple response body
        if not data and getattr(res, "body", None):
            data, end_stream = res.body, True  # type: ignore

        size = len(data)
        headers = res.headers
        status = res.status
        self.response_size = size

        if not isinstance(status, int) or status < 200:
            raise RuntimeError(f"Invalid response status {status!r}")

        if not has_message_body(status):
            # Header-only response status
            self.response_func = None
            if (
                data
                or not end_stream
                or "content-length" in headers
                or "transfer-encoding" in headers
            ):
                data, size, end_stream = b"", 0, True
                headers.pop("content-length", None)
                headers.pop("transfer-encoding", None)
                logger.warning(
                    f"Message body set in response on {self.request.path}. "
                    f"A {status} response may only have headers, no body."
                )
        elif self.head_only and "content-length" in headers:
            self.response_func = None
        elif end_stream:
            # Non-streaming response (all in one block)
            headers["content-length"] = size
            self.response_func = None
        elif "content-length" in headers:
            # Streaming response with size known in advance
            self.response_bytes_left = int(headers["content-length"]) - size
            self.response_func = self.http1_response_normal
        else:
            # Length not known, use chunked encoding
            headers["transfer-encoding"] = "chunked"
            data = b"%x\r\n%b\r\n" % (size, data) if size else b""
            self.response_func = self.http1_response_chunked

        if self.head_only:
            # Head request: don't send body
            data = b""
            self.response_func = self.head_response_ignored

        headers["connection"] = "keep-alive" if self.keep_alive else "close"

        # This header may be removed or modified by the AltSvcCheck Touchup
        # service. At server start, we either remove this header from ever
        # being assigned, or we change the value as required.
        headers["alt-svc"] = ""

        ret = format_http1_response(status, res.processed_headers)
        if data:
            ret += data

        # Send a 100-continue if expected and not Expectation Failed
        if self.expecting_continue:
            self.expecting_continue = False
            if status != 417:
                ret = HTTP_CONTINUE + ret

        # Send response
        if self.protocol.access_log:
            self.log_response()

        await self._send(ret)
        self.stage = Stage.IDLE if end_stream else Stage.RESPONSE

    def head_response_ignored(self, data: bytes, end_stream: bool) -> None:
        """HEAD response: body data silently ignored."""
        if end_stream:
            self.response_func = None
            self.stage = Stage.IDLE

    async def http1_response_chunked(
        self, data: bytes, end_stream: bool
    ) -> None:
        """Format a part of response body in chunked encoding."""
        # Chunked encoding
        size = len(data)
        if end_stream:
            await self._send(
                b"%x\r\n%b\r\n0\r\n\r\n" % (size, data)
                if size
                else b"0\r\n\r\n"
            )
            self.response_func = None
            self.stage = Stage.IDLE
        elif size:
            await self._send(b"%x\r\n%b\r\n" % (size, data))

    async def http1_response_normal(
        self, data: bytes, end_stream: bool
    ) -> None:
        """Format / keep track of non-chunked response."""
        bytes_left = self.response_bytes_left - len(data)
        if bytes_left <= 0:
            if bytes_left < 0:
                raise ServerError("Response was bigger than content-length")

            await self._send(data)
            self.response_func = None
            self.stage = Stage.IDLE
        else:
            if end_stream:
                raise ServerError("Response was smaller than content-length")

            await self._send(data)
        self.response_bytes_left = bytes_left

    async def error_response(self, exception: Exception) -> None:
        """Handle response when exception encountered"""
        # Disconnect after an error if in any other state than handler
        if self.stage is not Stage.HANDLER:
            self.keep_alive = False

        # Request failure? Respond but then disconnect
        if self.stage is Stage.REQUEST:
            self.stage = Stage.HANDLER

        # From request and handler states we can respond, otherwise be silent
        if self.stage is Stage.HANDLER:
            app = self.protocol.app

            if self.request is None:
                self.create_empty_request()

            request_middleware = not isinstance(
                exception, (ServiceUnavailable, RequestCancelled)
            )
            try:
                await app.handle_exception(
                    self.request, exception, request_middleware
                )
            except Exception as e:
                await app.handle_exception(self.request, e, False)

    def create_empty_request(self) -> None:
        """Create an empty request object for error handling use.

        Current error handling code needs a request object that won't exist
        if an error occurred during before a request was received. Create a
        bogus response for error handling use.
        """

        # Reformat any URL already received with \xHH escapes for better logs
        url_bytes = (
            self.url.encode(errors="surrogateescape")
            .decode("ASCII", errors="backslashreplace")
            .encode("ASCII")
            if self.url
            else b"*"
        )

        # FIXME: Avoid this by refactoring error handling and response code
        self.request = self.protocol.request_class(
            url_bytes=url_bytes,
            headers=Header({}),
            version="1.1",
            method="NONE",
            transport=self.protocol.transport,
            app=self.protocol.app,
        )
        self.request.stream = self

    def log_response(self) -> None:
        """Helper method provided to enable the logging of responses in case if the `HttpProtocol.access_log` is enabled."""  # noqa: E501
        req, res = self.request, self.response
        extra = {
            "status": getattr(res, "status", 0),
            "byte": res.headers.get("content-length", 0)
            if res.headers.get("transfer-encoding") != "chunked"
            else "chunked",
            "host": f"{id(self.protocol.transport):X}"[-5:-1] + "unx",
            "request": "nil",
            "duration": (
                f" {1000 * (perf_counter() - self.perft0):.1f}ms"
                if self.perft0 is not None
                else ""
            ),
        }
        if ip := req.client_ip:
            extra["host"] = f"{ip}:{req.port}"
        extra["request"] = f"{req.method} {req.url}"
        access_logger.info("", extra=extra)

    # Request methods

    async def __aiter__(self):
        """Async iterate over request body."""
        while self.request_body:
            data = await self.read()

            if data:
                yield data

    async def read(self) -> bytes | None:  # no cov
        """Read some bytes of request body."""

        # Send a 100-continue if needed
        if self.expecting_continue:
            self.expecting_continue = False
            await self._send(HTTP_CONTINUE)

        # Receive request body chunk
        buf = self.recv_buffer
        if self.request_bytes_left == 0 and self.request_body == "chunked":
            # Process a chunk header: \r\n<size>[;<chunk extensions>]\r\n
            while True:
                pos = buf.find(b"\r\n", 3)

                if pos != -1:
                    break

                if len(buf) > 64:
                    self.keep_alive = False
                    raise BadRequest("Bad chunked encoding")

                await self._receive_more()

            try:
                raw = buf[2:pos].split(b";", 1)[0].decode()
                size = self._safe_int(raw, 16)
            except Exception:
                self.keep_alive = False
                raise BadRequest("Bad chunked encoding")

            if size <= 0:
                self.request_body = None

                if size < 0:
                    self.keep_alive = False
                    raise BadRequest("Bad chunked encoding")

                # Consume the leading CRLF, the terminating size line and
                # the CRLF that follows it.
                del buf[: pos + 2]

                # Only the empty line that ends the (empty) trailer section
                # may follow. Wait for it to arrive.
                while len(buf) < 2:
                    await self._receive_more()

                # Reject any trailer-part. Leaving trailer bytes in the
                # buffer would let them be reparsed as a smuggled request on
                # this keep-alive connection.
                if buf[:2] != b"\r\n":
                    self.keep_alive = False
                    raise BadRequest("Bad chunked encoding")

                # Consume the final empty line. Anything after it is a
                # legitimately pipelined next request.
                del buf[:2]
                return None

            # Remove CRLF, chunk size and the CRLF that follows
            del buf[: pos + 2]

            self.request_bytes_left = size
            self.request_bytes += size

        # Request size limit
        if self.request_bytes > self.request_max_size:
            self.keep_alive = False
            raise PayloadTooLarge("Request body exceeds the size limit")

        # End of request body?
        if not self.request_bytes_left:
            self.request_body = None
            return None

        # At this point we are good to read/return up to request_bytes_left
        if not buf:
            await self._receive_more()

        data = bytes(buf[: self.request_bytes_left])
        size = len(data)

        del buf[:size]

        self.request_bytes_left -= size

        await self.dispatch(
            "http.lifecycle.read_body",
            inline=True,
            context={"body": data},
        )

        return data

    # Response methods

    def respond(self, response: BaseHTTPResponse) -> BaseHTTPResponse:
        """Initiate new streaming response.

        Nothing is sent until the first send() call on the returned object, and
        calling this function multiple times will just alter the response to be
        given.
        """
        if self.stage is not Stage.HANDLER:
            self.stage = Stage.FAILED
            raise RuntimeError("Response already started")

        # Disconnect any earlier but unused response object
        if self.response is not None:
            self.response.stream = None

        # Connect and return the response
        self.response, response.stream = response, self
        return response

    @property
    def send(self):
        return self.response_func

    @classmethod
    def set_header_max_size(cls, *sizes: int):
        cls.HEADER_MAX_SIZE = min(
            *sizes,
            cls.HEADER_CEILING,
        )

    @classmethod
    def set_header_limits(
        cls,
        *,
        max_size: int,
        max_field_size: int,
        max_field_count: int,
        buffer_size: int | None = None,
        request_max_size: int | None = None,
    ) -> None:
        """Configure all request header parsing quotas."""
        sizes = [max_size, cls.HEADER_CEILING]
        if buffer_size is not None:
            sizes.append(buffer_size - 4096)
        if request_max_size is not None:
            sizes.append(request_max_size)
        cls.HEADER_MAX_SIZE = min(*sizes)
        cls.HEADER_MAX_FIELD_SIZE = max_field_size
        cls.HEADER_MAX_FIELD_COUNT = max_field_count

    @staticmethod
    def _safe_int(value: str, base: int = 10) -> int:
        if "-" in value or "+" in value or "_" in value:
            raise ValueError
        return int(value, base)
