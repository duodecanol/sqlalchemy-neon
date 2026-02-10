"""PostgreSQL wire protocol v3.0 over async byte streams.

Transport-agnostic implementation that can be used over WebSocket or any other
async byte transport. Supports startup, authentication (trust, cleartext, MD5,
SCRAM-SHA-256), simple query protocol, and extended query protocol.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
import struct
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable
from .errors import (
    NeonAuthenticationError,
    NeonConnectionError,
    NeonQueryError,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PG_PROTOCOL_VERSION = 196608  # (3 << 16) | 0

# Frontend (client -> server) message types
QUERY_MSG = ord("Q")
PARSE_MSG = ord("P")
BIND_MSG = ord("B")
DESCRIBE_MSG = ord("D")
EXECUTE_MSG = ord("E")
SYNC_MSG = ord("S")
TERMINATE_MSG = ord("X")
PASSWORD_MSG = ord("p")
CLOSE_MSG = ord("C")

# Backend (server -> client) message types
AUTH_MSG = ord("R")
PARAM_STATUS_MSG = ord("S")
BACKEND_KEY_MSG = ord("K")
READY_MSG = ord("Z")
ROW_DESC_MSG = ord("T")
DATA_ROW_MSG = ord("D")
COMMAND_COMPLETE_MSG = ord("C")
ERROR_RESPONSE_MSG = ord("E")
NOTICE_RESPONSE_MSG = ord("N")
PARSE_COMPLETE_MSG = ord("1")
BIND_COMPLETE_MSG = ord("2")
CLOSE_COMPLETE_MSG = ord("3")
NO_DATA_MSG = ord("n")
EMPTY_QUERY_MSG = ord("I")

# Authentication subtypes
AUTH_OK = 0
AUTH_CLEARTEXT = 3
AUTH_MD5 = 5
AUTH_SASL = 10
AUTH_SASL_CONTINUE = 11
AUTH_SASL_FINAL = 12

# Transaction status indicators
TXN_IDLE = ord("I")
TXN_IN_TRANSACTION = ord("T")
TXN_FAILED = ord("E")

# ErrorResponse / NoticeResponse field type -> name mapping
_ERROR_FIELD_NAMES: dict[int, str] = {
    ord("S"): "severity",
    ord("V"): "severity_nonlocalized",
    ord("C"): "code",
    ord("M"): "message",
    ord("D"): "detail",
    ord("H"): "hint",
    ord("P"): "position",
    ord("p"): "internal_position",
    ord("q"): "internal_query",
    ord("W"): "where",
    ord("s"): "schema",
    ord("t"): "table",
    ord("c"): "column",
    ord("d"): "datatype",
    ord("n"): "constraint",
    ord("F"): "file",
    ord("L"): "line",
    ord("R"): "routine",
}

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class FieldDescription:
    """Metadata for a single column in a query result."""

    name: str
    table_oid: int
    column_index: int
    type_oid: int
    type_size: int
    type_modifier: int
    format_code: int  # 0 = text, 1 = binary


@dataclass
class PGQueryResult:
    """Result of a query execution via the wire protocol."""

    fields: list[FieldDescription]
    rows: list[list[bytes | None]]
    command_tag: str


# ---------------------------------------------------------------------------
# Buffered reader
# ---------------------------------------------------------------------------


class _BufferedReader:
    """Buffered async reader for exact byte counts.

    WebSocket frame boundaries do NOT align with PG message boundaries.
    This reader maintains an internal buffer and serves exact byte counts.
    """

    __slots__ = ("_recv", "_buffer", "_pos")

    def __init__(self, recv_fn: Callable[[], Awaitable[bytes]]) -> None:
        self._recv = recv_fn
        self._buffer = bytearray()
        self._pos = 0

    async def read_exact(self, n: int) -> bytes:
        """Read exactly *n* bytes, pulling from the source as needed."""
        while (self._pos + n) > len(self._buffer):
            chunk = await self._recv()
            if not chunk:
                raise NeonConnectionError("Connection closed while reading")
            self._buffer.extend(chunk)

        result = bytes(self._buffer[self._pos : self._pos + n])
        self._pos += n

        # Compact periodically to avoid unbounded growth
        if self._pos > 65536:
            del self._buffer[: self._pos]
            self._pos = 0

        return result

    async def read_message(self) -> tuple[int, bytes]:
        """Read a complete backend message.

        Returns ``(message_type, payload)`` where *payload* does **not**
        include the 4-byte length field.
        """
        header = await self.read_exact(5)  # 1 type + 4 length
        msg_type = header[0]
        (length,) = struct.unpack("!I", header[1:5])
        payload_len = length - 4
        if payload_len < 0:
            raise NeonConnectionError(
                f"Invalid message length {length} for type {chr(msg_type)}"
            )
        payload = await self.read_exact(payload_len) if payload_len > 0 else b""
        return msg_type, payload


# ---------------------------------------------------------------------------
# Message builders (frontend -> backend)
# ---------------------------------------------------------------------------


def _build_startup_message(user: str, database: str, **params: str) -> bytes:
    """Build a StartupMessage (no type byte, special format)."""
    body = bytearray(struct.pack("!I", PG_PROTOCOL_VERSION))
    body += b"user\x00" + user.encode() + b"\x00"
    body += b"database\x00" + database.encode() + b"\x00"
    for key, value in params.items():
        body += key.encode() + b"\x00" + value.encode() + b"\x00"
    body += b"\x00"
    length = len(body) + 4
    return struct.pack("!I", length) + bytes(body)


def _build_message(msg_type: int, payload: bytes = b"") -> bytes:
    length = len(payload) + 4
    return bytes([msg_type]) + struct.pack("!I", length) + payload


def _build_query_message(sql: str) -> bytes:
    return _build_message(QUERY_MSG, sql.encode() + b"\x00")


def _build_parse_message(
    statement_name: str, sql: str, param_oids: list[int]
) -> bytes:
    payload = bytearray()
    payload += statement_name.encode() + b"\x00"
    payload += sql.encode() + b"\x00"
    payload += struct.pack("!H", len(param_oids))
    for oid in param_oids:
        payload += struct.pack("!I", oid)
    return _build_message(PARSE_MSG, bytes(payload))


def _build_bind_message(
    portal_name: str,
    statement_name: str,
    params: list[bytes | None],
) -> bytes:
    payload = bytearray()
    payload += portal_name.encode() + b"\x00"
    payload += statement_name.encode() + b"\x00"
    # Parameter format codes: 1 entry meaning "all use text format"
    payload += struct.pack("!HH", 1, 0)
    # Parameter values
    payload += struct.pack("!H", len(params))
    for param in params:
        if param is None:
            payload += struct.pack("!i", -1)
        else:
            payload += struct.pack("!I", len(param)) + param
    # Result format codes: 1 entry meaning "all use text format"
    payload += struct.pack("!HH", 1, 0)
    return _build_message(BIND_MSG, bytes(payload))


def _build_describe_message(describe_type: str, name: str = "") -> bytes:
    payload = describe_type.encode("ascii") + name.encode() + b"\x00"
    return _build_message(DESCRIBE_MSG, payload)


def _build_execute_message(portal_name: str = "", max_rows: int = 0) -> bytes:
    payload = portal_name.encode() + b"\x00" + struct.pack("!I", max_rows)
    return _build_message(EXECUTE_MSG, payload)


def _build_sync_message() -> bytes:
    return _build_message(SYNC_MSG)


def _build_terminate_message() -> bytes:
    return _build_message(TERMINATE_MSG)


def _build_password_message(password: str) -> bytes:
    return _build_message(PASSWORD_MSG, password.encode() + b"\x00")


def _build_sasl_initial_response(mechanism: str, data: bytes) -> bytes:
    payload = mechanism.encode() + b"\x00" + struct.pack("!I", len(data)) + data
    return _build_message(PASSWORD_MSG, payload)


def _build_sasl_response(data: bytes) -> bytes:
    return _build_message(PASSWORD_MSG, data)


# ---------------------------------------------------------------------------
# Response parsers
# ---------------------------------------------------------------------------


def _parse_error_fields(payload: bytes) -> dict[str, str]:
    """Parse ErrorResponse / NoticeResponse payload into field dict."""
    fields: dict[str, str] = {}
    pos = 0
    while pos < len(payload):
        field_type = payload[pos]
        pos += 1
        if field_type == 0:
            break
        null_pos = payload.index(0, pos)
        value = payload[pos:null_pos].decode("utf-8", errors="replace")
        pos = null_pos + 1
        name = _ERROR_FIELD_NAMES.get(field_type, f"unknown_{chr(field_type)}")
        fields[name] = value
    return fields


def _parse_row_description(payload: bytes) -> list[FieldDescription]:
    """Parse a RowDescription ('T') message payload."""
    pos = 0
    (num_fields,) = struct.unpack_from("!H", payload, pos)
    pos += 2
    fields: list[FieldDescription] = []
    for _ in range(num_fields):
        null_pos = payload.index(0, pos)
        name = payload[pos:null_pos].decode("utf-8", errors="replace")
        pos = null_pos + 1
        table_oid, col_idx, type_oid, type_size, type_mod, fmt = struct.unpack_from(
            "!IhIhih", payload, pos
        )
        pos += 18
        fields.append(
            FieldDescription(
                name=name,
                table_oid=table_oid,
                column_index=col_idx,
                type_oid=type_oid,
                type_size=type_size,
                type_modifier=type_mod,
                format_code=fmt,
            )
        )
    return fields


def _parse_data_row(payload: bytes) -> list[bytes | None]:
    """Parse a DataRow ('D') message payload."""
    pos = 0
    (num_cols,) = struct.unpack_from("!H", payload, pos)
    pos += 2
    values: list[bytes | None] = []
    for _ in range(num_cols):
        (col_len,) = struct.unpack_from("!i", payload, pos)
        pos += 4
        if col_len == -1:
            values.append(None)
        else:
            values.append(payload[pos : pos + col_len])
            pos += col_len
    return values


# ---------------------------------------------------------------------------
# SCRAM-SHA-256 authentication
# ---------------------------------------------------------------------------


class _ScramSHA256:
    """SCRAM-SHA-256 SASL authentication handler (RFC 5802 / RFC 7677)."""

    MECHANISM = "SCRAM-SHA-256"

    def __init__(self, user: str, password: str) -> None:
        self._user = user
        self._password = password
        self._client_nonce = base64.b64encode(secrets.token_bytes(24)).decode("ascii")
        self._client_first_bare = ""
        self._server_signature = b""

    def client_first_message(self) -> bytes:
        """Generate the client-first-message for SASLInitialResponse."""
        self._client_first_bare = f"n=,r={self._client_nonce}"
        return f"n,,{self._client_first_bare}".encode()

    def process_server_first(self, server_first: str) -> bytes:
        """Process server-first-message and return client-final-message."""
        parts: dict[str, str] = {}
        for item in server_first.split(","):
            key, _, value = item.partition("=")
            parts[key] = value

        server_nonce = parts["r"]
        salt = base64.b64decode(parts["s"])
        iterations = int(parts["i"])

        if not server_nonce.startswith(self._client_nonce):
            raise NeonAuthenticationError(
                "SCRAM: Server nonce does not start with client nonce"
            )

        salted_password = hashlib.pbkdf2_hmac(
            "sha256", self._password.encode(), salt, iterations
        )
        client_key = hmac.new(
            salted_password, b"Client Key", hashlib.sha256
        ).digest()
        stored_key = hashlib.sha256(client_key).digest()

        channel_binding = base64.b64encode(b"n,,").decode("ascii")
        client_final_without_proof = f"c={channel_binding},r={server_nonce}"
        auth_message = (
            f"{self._client_first_bare},{server_first},{client_final_without_proof}"
        )

        client_signature = hmac.new(
            stored_key, auth_message.encode(), hashlib.sha256
        ).digest()
        client_proof = bytes(a ^ b for a, b in zip(client_key, client_signature))

        server_key = hmac.new(
            salted_password, b"Server Key", hashlib.sha256
        ).digest()
        self._server_signature = hmac.new(
            server_key, auth_message.encode(), hashlib.sha256
        ).digest()

        proof_b64 = base64.b64encode(client_proof).decode("ascii")
        return f"{client_final_without_proof},p={proof_b64}".encode()

    def verify_server_final(self, server_final: str) -> None:
        """Verify the server-final-message signature."""
        if not server_final.startswith("v="):
            raise NeonAuthenticationError(
                f"SCRAM: Invalid server-final-message: {server_final}"
            )
        server_sig = base64.b64decode(server_final[2:])
        if not hmac.compare_digest(server_sig, self._server_signature):
            raise NeonAuthenticationError(
                "SCRAM: Server signature verification failed"
            )


# ---------------------------------------------------------------------------
# PGProtocol
# ---------------------------------------------------------------------------


class PGProtocol:
    """PostgreSQL wire protocol v3.0 handler.

    Transport-agnostic: *send_fn* and *recv_fn* provide the underlying
    byte stream (e.g. WebSocket binary frames).
    """

    def __init__(
        self,
        send_fn: Callable[[bytes], Awaitable[None]],
        recv_fn: Callable[[], Awaitable[bytes]],
    ) -> None:
        self._send = send_fn
        self._reader = _BufferedReader(recv_fn)
        self._server_params: dict[str, str] = {}
        self._backend_pid: int = 0
        self._backend_secret: int = 0
        self._txn_status: int = TXN_IDLE
        self._notices: list[dict[str, str]] = []

    @property
    def server_params(self) -> dict[str, str]:
        return dict(self._server_params)

    @property
    def transaction_status(self) -> str:
        return chr(self._txn_status)

    # ------------------------------------------------------------------
    # Startup & authentication
    # ------------------------------------------------------------------

    async def startup(
        self,
        user: str,
        password: str,
        database: str,
        **extra_params: str,
    ) -> dict[str, str]:
        """Perform PG startup handshake including authentication.

        Returns server parameter dict on success.
        """
        await self._send(_build_startup_message(user, database, **extra_params))
        await self._handle_authentication(user, password)

        # Read ParameterStatus*, BackendKeyData, ReadyForQuery
        while True:
            msg_type, payload = await self._reader.read_message()
            if msg_type == PARAM_STATUS_MSG:
                null1 = payload.index(0)
                key = payload[:null1].decode()
                null2 = payload.index(0, null1 + 1)
                value = payload[null1 + 1 : null2].decode()
                self._server_params[key] = value
            elif msg_type == BACKEND_KEY_MSG:
                self._backend_pid, self._backend_secret = struct.unpack("!II", payload)
            elif msg_type == READY_MSG:
                self._txn_status = payload[0]
                return dict(self._server_params)
            elif msg_type == ERROR_RESPONSE_MSG:
                ef = _parse_error_fields(payload)
                raise NeonAuthenticationError(
                    f"Startup failed: {ef.get('message', 'unknown')}"
                )
            elif msg_type == NOTICE_RESPONSE_MSG:
                self._notices.append(_parse_error_fields(payload))

    async def _handle_authentication(self, user: str, password: str) -> None:
        while True:
            msg_type, payload = await self._reader.read_message()

            if msg_type == ERROR_RESPONSE_MSG:
                ef = _parse_error_fields(payload)
                raise NeonAuthenticationError(
                    f"Authentication failed: {ef.get('message', 'unknown')}"
                )

            if msg_type != AUTH_MSG:
                raise NeonConnectionError(
                    f"Expected Authentication message, got {chr(msg_type)}"
                )

            (auth_type,) = struct.unpack_from("!I", payload)

            if auth_type == AUTH_OK:
                return

            if auth_type == AUTH_CLEARTEXT:
                await self._send(_build_password_message(password))

            elif auth_type == AUTH_MD5:
                salt = payload[4:8]
                inner = hashlib.md5(
                    password.encode() + user.encode()
                ).hexdigest()
                outer = "md5" + hashlib.md5(
                    inner.encode() + salt
                ).hexdigest()
                await self._send(_build_password_message(outer))

            elif auth_type == AUTH_SASL:
                await self._handle_sasl(user, password, payload[4:])

            else:
                raise NeonAuthenticationError(
                    f"Unsupported authentication method: {auth_type}"
                )

    async def _handle_sasl(
        self, user: str, password: str, mechanisms_payload: bytes
    ) -> None:
        # Parse mechanism names (null-separated, double-null terminated)
        mechanisms: list[str] = []
        pos = 0
        while pos < len(mechanisms_payload):
            null_pos = mechanisms_payload.index(0, pos)
            mech = mechanisms_payload[pos:null_pos].decode()
            if mech:
                mechanisms.append(mech)
            pos = null_pos + 1
            if pos < len(mechanisms_payload) and mechanisms_payload[pos] == 0:
                break

        if "SCRAM-SHA-256" not in mechanisms:
            raise NeonAuthenticationError(
                f"Server requires unsupported SASL mechanisms: {mechanisms}"
            )

        scram = _ScramSHA256(user, password)
        client_first = scram.client_first_message()
        await self._send(
            _build_sasl_initial_response("SCRAM-SHA-256", client_first)
        )

        # AuthenticationSASLContinue
        msg_type, payload = await self._reader.read_message()
        if msg_type != AUTH_MSG:
            raise NeonConnectionError("Expected SASL continue message")
        (auth_type,) = struct.unpack_from("!I", payload)
        if auth_type != AUTH_SASL_CONTINUE:
            raise NeonAuthenticationError("Expected SASLContinue")
        server_first = payload[4:].decode()

        client_final = scram.process_server_first(server_first)
        await self._send(_build_sasl_response(client_final))

        # AuthenticationSASLFinal
        msg_type, payload = await self._reader.read_message()
        if msg_type != AUTH_MSG:
            raise NeonConnectionError("Expected SASL final message")
        (auth_type,) = struct.unpack_from("!I", payload)
        if auth_type != AUTH_SASL_FINAL:
            raise NeonAuthenticationError("Expected SASLFinal")
        scram.verify_server_final(payload[4:].decode())

        # AuthenticationOk follows (handled by the caller's loop)

    # ------------------------------------------------------------------
    # Simple Query protocol
    # ------------------------------------------------------------------

    async def simple_query(self, sql: str) -> list[PGQueryResult]:
        """Execute using the Simple Query protocol (no parameters)."""
        await self._send(_build_query_message(sql))

        results: list[PGQueryResult] = []
        current_fields: list[FieldDescription] = []
        current_rows: list[list[bytes | None]] = []

        while True:
            msg_type, payload = await self._reader.read_message()

            if msg_type == ROW_DESC_MSG:
                current_fields = _parse_row_description(payload)
                current_rows = []
            elif msg_type == DATA_ROW_MSG:
                current_rows.append(_parse_data_row(payload))
            elif msg_type == COMMAND_COMPLETE_MSG:
                tag = payload[:-1].decode()
                results.append(
                    PGQueryResult(
                        fields=list(current_fields),
                        rows=list(current_rows),
                        command_tag=tag,
                    )
                )
                current_fields = []
                current_rows = []
            elif msg_type == EMPTY_QUERY_MSG:
                results.append(PGQueryResult(fields=[], rows=[], command_tag=""))
            elif msg_type == ERROR_RESPONSE_MSG:
                ef = _parse_error_fields(payload)
                await self._read_until_ready()
                raise NeonQueryError(
                    message=ef.get("message", "Query error"),
                    code=ef.get("code"),
                    detail=ef.get("detail"),
                    hint=ef.get("hint"),
                )
            elif msg_type == NOTICE_RESPONSE_MSG:
                self._notices.append(_parse_error_fields(payload))
            elif msg_type == READY_MSG:
                self._txn_status = payload[0]
                return results

    # ------------------------------------------------------------------
    # Extended Query protocol
    # ------------------------------------------------------------------

    async def extended_query(
        self,
        sql: str,
        params: list[bytes | None],
    ) -> PGQueryResult:
        """Execute using Extended Query protocol with $1-style parameters.

        All parameter and result values use text format.
        Uses unnamed prepared statement and portal.
        Parameter type OIDs are all 0 (server infers).
        """
        # Build all messages in a single buffer
        messages = bytearray()
        messages += _build_parse_message("", sql, [0] * len(params))
        messages += _build_bind_message("", "", params)
        messages += _build_describe_message("P", "")
        messages += _build_execute_message("", 0)
        messages += _build_sync_message()
        await self._send(bytes(messages))

        # Read responses
        fields: list[FieldDescription] = []
        rows: list[list[bytes | None]] = []
        command_tag = ""

        while True:
            msg_type, payload = await self._reader.read_message()

            if msg_type == PARSE_COMPLETE_MSG:
                continue
            elif msg_type == BIND_COMPLETE_MSG:
                continue
            elif msg_type == ROW_DESC_MSG:
                fields = _parse_row_description(payload)
            elif msg_type == NO_DATA_MSG:
                fields = []
            elif msg_type == DATA_ROW_MSG:
                rows.append(_parse_data_row(payload))
            elif msg_type == COMMAND_COMPLETE_MSG:
                command_tag = payload[:-1].decode()
            elif msg_type == ERROR_RESPONSE_MSG:
                ef = _parse_error_fields(payload)
                await self._read_until_ready()
                raise NeonQueryError(
                    message=ef.get("message", "Query error"),
                    code=ef.get("code"),
                    detail=ef.get("detail"),
                    hint=ef.get("hint"),
                )
            elif msg_type == NOTICE_RESPONSE_MSG:
                self._notices.append(_parse_error_fields(payload))
            elif msg_type == READY_MSG:
                self._txn_status = payload[0]
                return PGQueryResult(
                    fields=fields, rows=rows, command_tag=command_tag
                )

    # ------------------------------------------------------------------
    # Terminate
    # ------------------------------------------------------------------

    async def terminate(self) -> None:
        """Send Terminate message to close the connection gracefully."""
        try:
            await self._send(_build_terminate_message())
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _read_until_ready(self) -> None:
        """Consume messages until ReadyForQuery (resync after error)."""
        while True:
            msg_type, payload = await self._reader.read_message()
            if msg_type == READY_MSG:
                self._txn_status = payload[0]
                return
