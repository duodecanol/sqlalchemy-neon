"""Tests for PostgreSQL wire protocol implementation."""

from __future__ import annotations

import base64
import hashlib
import hmac
import struct

import pytest

from sqlalchemy_neon.errors import (
    NeonAuthenticationError,
    NeonConnectionError,
    NeonQueryError,
)
from sqlalchemy_neon.pg_protocol import (
    AUTH_CLEARTEXT,
    AUTH_MD5,
    AUTH_MSG,
    AUTH_OK,
    AUTH_SASL,
    AUTH_SASL_CONTINUE,
    AUTH_SASL_FINAL,
    BACKEND_KEY_MSG,
    COMMAND_COMPLETE_MSG,
    DATA_ROW_MSG,
    EMPTY_QUERY_MSG,
    ERROR_RESPONSE_MSG,
    NO_DATA_MSG,
    PARAM_STATUS_MSG,
    PARSE_COMPLETE_MSG,
    BIND_COMPLETE_MSG,
    PG_PROTOCOL_VERSION,
    READY_MSG,
    ROW_DESC_MSG,
    FieldDescription,
    PGProtocol,
    PGQueryResult,
    _BufferedReader,
    _ScramSHA256,
    _build_bind_message,
    _build_describe_message,
    _build_execute_message,
    _build_message,
    _build_parse_message,
    _build_password_message,
    _build_query_message,
    _build_startup_message,
    _build_sync_message,
    _build_terminate_message,
    _parse_data_row,
    _parse_error_fields,
    _parse_row_description,
)


# ---------------------------------------------------------------------------
# Helpers for building mock server responses
# ---------------------------------------------------------------------------


def _backend_msg(msg_type: int, payload: bytes = b"") -> bytes:
    length = len(payload) + 4
    return bytes([msg_type]) + struct.pack("!I", length) + payload


def _auth_ok() -> bytes:
    return _backend_msg(AUTH_MSG, struct.pack("!I", AUTH_OK))


def _auth_cleartext() -> bytes:
    return _backend_msg(AUTH_MSG, struct.pack("!I", AUTH_CLEARTEXT))


def _auth_md5(salt: bytes) -> bytes:
    return _backend_msg(AUTH_MSG, struct.pack("!I", AUTH_MD5) + salt)


def _param_status(key: str, value: str) -> bytes:
    payload = key.encode() + b"\x00" + value.encode() + b"\x00"
    return _backend_msg(PARAM_STATUS_MSG, payload)


def _backend_key(pid: int, secret: int) -> bytes:
    return _backend_msg(BACKEND_KEY_MSG, struct.pack("!II", pid, secret))


def _ready_for_query(status: str = "I") -> bytes:
    return _backend_msg(READY_MSG, status.encode("ascii"))


def _row_description(fields: list[tuple[str, int]]) -> bytes:
    """Build RowDescription from (name, type_oid) tuples."""
    payload = bytearray(struct.pack("!H", len(fields)))
    for name, type_oid in fields:
        payload += name.encode() + b"\x00"
        # table_oid(4), col_index(2), type_oid(4), type_size(2), type_mod(4), format(2)
        payload += struct.pack("!IhIhih", 0, 0, type_oid, -1, -1, 0)
    return _backend_msg(ROW_DESC_MSG, bytes(payload))


def _data_row(values: list[bytes | None]) -> bytes:
    payload = bytearray(struct.pack("!H", len(values)))
    for val in values:
        if val is None:
            payload += struct.pack("!i", -1)
        else:
            payload += struct.pack("!I", len(val)) + val
    return _backend_msg(DATA_ROW_MSG, bytes(payload))


def _command_complete(tag: str) -> bytes:
    return _backend_msg(COMMAND_COMPLETE_MSG, tag.encode() + b"\x00")


def _error_response(
    severity: str = "ERROR",
    code: str = "42601",
    message: str = "syntax error",
) -> bytes:
    payload = bytearray()
    payload += b"S" + severity.encode() + b"\x00"
    payload += b"C" + code.encode() + b"\x00"
    payload += b"M" + message.encode() + b"\x00"
    payload += b"\x00"
    return _backend_msg(ERROR_RESPONSE_MSG, bytes(payload))


def _parse_complete() -> bytes:
    return _backend_msg(PARSE_COMPLETE_MSG)


def _bind_complete() -> bytes:
    return _backend_msg(BIND_COMPLETE_MSG)


def _no_data() -> bytes:
    return _backend_msg(NO_DATA_MSG)


def _empty_query() -> bytes:
    return _backend_msg(EMPTY_QUERY_MSG)


def _startup_response_trust() -> bytes:
    """Full server response for trust auth startup."""
    return (
        _auth_ok()
        + _param_status("server_version", "16.0")
        + _param_status("client_encoding", "UTF8")
        + _backend_key(1234, 5678)
        + _ready_for_query()
    )


def _startup_response_cleartext(expected_password: str) -> bytes:
    """Server response sequence for cleartext auth (first message only)."""
    return _auth_cleartext()


class MockTransport:
    """Mock transport for protocol testing with pre-recorded responses."""

    def __init__(self, responses: list[bytes]) -> None:
        self.sent: list[bytes] = []
        self._data = bytearray()
        for r in responses:
            self._data.extend(r)
        self._pos = 0

    async def send(self, data: bytes) -> None:
        self.sent.append(data)

    async def recv(self) -> bytes:
        if self._pos >= len(self._data):
            raise NeonConnectionError("No more mock data")
        # Return remaining data in one chunk
        chunk = bytes(self._data[self._pos :])
        self._pos = len(self._data)
        return chunk


class ChunkedMockTransport:
    """Mock transport that delivers data in small chunks."""

    def __init__(self, data: bytes, chunk_size: int = 3) -> None:
        self.sent: list[bytes] = []
        self._data = data
        self._pos = 0
        self._chunk_size = chunk_size

    async def send(self, data: bytes) -> None:
        self.sent.append(data)

    async def recv(self) -> bytes:
        if self._pos >= len(self._data):
            raise NeonConnectionError("No more mock data")
        end = min(self._pos + self._chunk_size, len(self._data))
        chunk = self._data[self._pos : end]
        self._pos = end
        return chunk


# ===========================================================================
# Message construction tests
# ===========================================================================


class TestMessageConstruction:
    def test_startup_message_format(self):
        msg = _build_startup_message("testuser", "testdb")
        (length,) = struct.unpack("!I", msg[:4])
        assert length == len(msg)
        (protocol,) = struct.unpack_from("!I", msg, 4)
        assert protocol == PG_PROTOCOL_VERSION
        assert b"user\x00testuser\x00" in msg
        assert b"database\x00testdb\x00" in msg
        assert msg[-1] == 0

    def test_startup_message_extra_params(self):
        msg = _build_startup_message(
            "u", "d", application_name="myapp", client_encoding="UTF8"
        )
        assert b"application_name\x00myapp\x00" in msg
        assert b"client_encoding\x00UTF8\x00" in msg

    def test_query_message_format(self):
        msg = _build_query_message("SELECT 1")
        assert msg[0] == ord("Q")
        (length,) = struct.unpack_from("!I", msg, 1)
        assert length == len(msg) - 1
        assert msg[5:-1] == b"SELECT 1"
        assert msg[-1] == 0

    def test_parse_message_format(self):
        msg = _build_parse_message("", "SELECT $1", [0])
        assert msg[0] == ord("P")
        # Contains: \0 (empty name), "SELECT $1\0", 1 param, OID=0
        assert b"\x00SELECT $1\x00" in msg

    def test_bind_message_text_format(self):
        params = [b"hello", None, b"42"]
        msg = _build_bind_message("", "", params)
        assert msg[0] == ord("B")
        # Verify NULL is encoded as -1 length
        assert struct.pack("!i", -1) in msg
        # Verify text values
        assert b"hello" in msg
        assert b"42" in msg

    def test_describe_portal_message(self):
        msg = _build_describe_message("P", "")
        assert msg[0] == ord("D")
        assert msg[5:6] == b"P"

    def test_describe_statement_message(self):
        msg = _build_describe_message("S", "stmt1")
        assert msg[0] == ord("D")
        assert msg[5:6] == b"S"
        assert b"stmt1\x00" in msg

    def test_execute_message_fetch_all(self):
        msg = _build_execute_message("", 0)
        assert msg[0] == ord("E")
        # portal_name\0 + max_rows(4)
        assert msg[5] == 0  # empty portal name
        max_rows = struct.unpack_from("!I", msg, 6)[0]
        assert max_rows == 0

    def test_sync_message(self):
        msg = _build_sync_message()
        assert len(msg) == 5
        assert msg[0] == ord("S")
        (length,) = struct.unpack_from("!I", msg, 1)
        assert length == 4

    def test_terminate_message(self):
        msg = _build_terminate_message()
        assert len(msg) == 5
        assert msg[0] == ord("X")

    def test_password_message(self):
        msg = _build_password_message("secret")
        assert msg[0] == ord("p")
        assert b"secret\x00" in msg


# ===========================================================================
# Response parsing tests
# ===========================================================================


class TestResponseParsing:
    def test_parse_row_description(self):
        payload = bytearray(struct.pack("!H", 2))
        # Field 1: "id", type OID 23 (INT4)
        payload += b"id\x00"
        payload += struct.pack("!IhIhih", 0, 0, 23, 4, -1, 0)
        # Field 2: "name", type OID 25 (TEXT)
        payload += b"name\x00"
        payload += struct.pack("!IhIhih", 0, 1, 25, -1, -1, 0)

        fields = _parse_row_description(bytes(payload))
        assert len(fields) == 2
        assert fields[0].name == "id"
        assert fields[0].type_oid == 23
        assert fields[1].name == "name"
        assert fields[1].type_oid == 25

    def test_parse_data_row_with_values(self):
        payload = bytearray(struct.pack("!H", 2))
        val1 = b"42"
        payload += struct.pack("!I", len(val1)) + val1
        val2 = b"hello"
        payload += struct.pack("!I", len(val2)) + val2

        row = _parse_data_row(bytes(payload))
        assert row == [b"42", b"hello"]

    def test_parse_data_row_with_null(self):
        payload = bytearray(struct.pack("!H", 2))
        payload += struct.pack("!i", -1)  # NULL
        val = b"world"
        payload += struct.pack("!I", len(val)) + val

        row = _parse_data_row(bytes(payload))
        assert row == [None, b"world"]

    def test_parse_error_response(self):
        payload = (
            b"S"
            + b"ERROR\x00"
            + b"C"
            + b"42601\x00"
            + b"M"
            + b"syntax error\x00"
            + b"D"
            + b"some detail\x00"
            + b"\x00"
        )
        result = _parse_error_fields(payload)
        assert result["severity"] == "ERROR"
        assert result["code"] == "42601"
        assert result["message"] == "syntax error"
        assert result["detail"] == "some detail"

    def test_parse_error_response_minimal(self):
        payload = b"M" + b"oops\x00" + b"\x00"
        result = _parse_error_fields(payload)
        assert result["message"] == "oops"


# ===========================================================================
# Buffered reader tests
# ===========================================================================


class TestBufferedReader:
    @pytest.mark.asyncio
    async def test_read_exact_single_chunk(self):
        chunks = [b"hello world!"]
        idx = 0

        async def recv():
            nonlocal idx
            data = chunks[idx]
            idx += 1
            return data

        reader = _BufferedReader(recv)
        result = await reader.read_exact(12)
        assert result == b"hello world!"

    @pytest.mark.asyncio
    async def test_read_exact_across_chunks(self):
        chunks = [b"hel", b"lo wo", b"rld!"]
        idx = 0

        async def recv():
            nonlocal idx
            data = chunks[idx]
            idx += 1
            return data

        reader = _BufferedReader(recv)
        result = await reader.read_exact(12)
        assert result == b"hello world!"

    @pytest.mark.asyncio
    async def test_read_exact_multiple_reads_from_one_chunk(self):
        async def recv():
            return b"ABCDEF"

        reader = _BufferedReader(recv)
        assert await reader.read_exact(2) == b"AB"
        assert await reader.read_exact(3) == b"CDE"
        assert await reader.read_exact(1) == b"F"

    @pytest.mark.asyncio
    async def test_read_message(self):
        # Build a ReadyForQuery ('Z') message
        payload = b"I"
        raw = bytes([ord("Z")]) + struct.pack("!I", len(payload) + 4) + payload

        async def recv():
            return raw

        reader = _BufferedReader(recv)
        msg_type, msg_payload = await reader.read_message()
        assert msg_type == ord("Z")
        assert msg_payload == b"I"

    @pytest.mark.asyncio
    async def test_read_message_across_chunks(self):
        # CommandComplete with "SELECT 1\0"
        tag = b"SELECT 1\x00"
        raw = bytes([ord("C")]) + struct.pack("!I", len(tag) + 4) + tag
        # Split into tiny chunks
        transport = ChunkedMockTransport(raw, chunk_size=2)
        reader = _BufferedReader(transport.recv)
        msg_type, payload = await reader.read_message()
        assert msg_type == ord("C")
        assert payload == tag

    @pytest.mark.asyncio
    async def test_read_multiple_messages_in_one_chunk(self):
        # Two messages in one chunk
        raw = _parse_complete() + _bind_complete()

        async def recv():
            return raw

        reader = _BufferedReader(recv)
        t1, _ = await reader.read_message()
        t2, _ = await reader.read_message()
        assert t1 == PARSE_COMPLETE_MSG
        assert t2 == BIND_COMPLETE_MSG

    @pytest.mark.asyncio
    async def test_read_empty_chunk_raises(self):
        async def recv():
            return b""

        reader = _BufferedReader(recv)
        with pytest.raises(NeonConnectionError, match="Connection closed"):
            await reader.read_exact(1)


# ===========================================================================
# SCRAM-SHA-256 tests
# ===========================================================================


class TestScramSHA256:
    def test_client_first_message_format(self):
        scram = _ScramSHA256("user", "password")
        msg = scram.client_first_message()
        decoded = msg.decode()
        assert decoded.startswith("n,,n=,r=")

    def test_client_first_nonce_is_unique(self):
        s1 = _ScramSHA256("u", "p")
        s2 = _ScramSHA256("u", "p")
        assert s1.client_first_message() != s2.client_first_message()

    def test_full_scram_exchange(self):
        """Simulate a complete SCRAM exchange with known values."""
        user = "testuser"
        password = "testpassword"
        scram = _ScramSHA256(user, password)

        client_first = scram.client_first_message().decode()
        # Extract client nonce from client-first-message
        client_first_bare = client_first[3:]  # strip "n,,"
        client_nonce = client_first_bare.split(",")[1][2:]  # after "r="

        # Simulate server-first-message
        server_nonce = client_nonce + "servernonce123"
        salt = base64.b64encode(b"randomsalt").decode()
        iterations = 4096
        server_first = f"r={server_nonce},s={salt},i={iterations}"

        client_final = scram.process_server_first(server_first).decode()
        assert client_final.startswith("c=")
        assert f"r={server_nonce}" in client_final
        assert ",p=" in client_final

        # Verify server signature computation
        salted_password = hashlib.pbkdf2_hmac(
            "sha256", password.encode(), b"randomsalt", 4096
        )
        server_key = hmac.new(
            salted_password, b"Server Key", hashlib.sha256
        ).digest()

        channel_binding = base64.b64encode(b"n,,").decode()
        client_final_without_proof = f"c={channel_binding},r={server_nonce}"
        auth_message = f"{client_first_bare},{server_first},{client_final_without_proof}"
        expected_sig = hmac.new(
            server_key, auth_message.encode(), hashlib.sha256
        ).digest()
        server_final = "v=" + base64.b64encode(expected_sig).decode()

        # Should not raise
        scram.verify_server_final(server_final)

    def test_bad_server_nonce_raises(self):
        scram = _ScramSHA256("u", "p")
        scram.client_first_message()
        with pytest.raises(NeonAuthenticationError, match="nonce"):
            scram.process_server_first("r=completelydifferent,s=c2FsdA==,i=4096")

    def test_bad_server_signature_raises(self):
        scram = _ScramSHA256("u", "p")
        msg = scram.client_first_message().decode()
        nonce = msg[3:].split(",")[1][2:]
        server_first = f"r={nonce}extra,s={base64.b64encode(b's').decode()},i=4096"
        scram.process_server_first(server_first)
        with pytest.raises(NeonAuthenticationError, match="signature"):
            scram.verify_server_final("v=" + base64.b64encode(b"wrong").decode())


# ===========================================================================
# PGProtocol integration tests (with mock transport)
# ===========================================================================


class TestPGProtocolStartup:
    @pytest.mark.asyncio
    async def test_startup_trust_auth(self):
        transport = MockTransport([_startup_response_trust()])
        proto = PGProtocol(transport.send, transport.recv)
        params = await proto.startup("user", "pass", "testdb")

        assert params["server_version"] == "16.0"
        assert params["client_encoding"] == "UTF8"
        assert proto.transaction_status == "I"
        # Verify startup message was sent
        assert len(transport.sent) == 1

    @pytest.mark.asyncio
    async def test_startup_cleartext_auth(self):
        transport = MockTransport(
            [
                _auth_cleartext(),
                _auth_ok()
                + _param_status("server_version", "15.0")
                + _backend_key(111, 222)
                + _ready_for_query(),
            ]
        )
        proto = PGProtocol(transport.send, transport.recv)
        params = await proto.startup("user", "mypassword", "db")

        assert params["server_version"] == "15.0"
        # startup + password message
        assert len(transport.sent) == 2
        # Password message should contain the password
        assert b"mypassword\x00" in transport.sent[1]

    @pytest.mark.asyncio
    async def test_startup_md5_auth(self):
        salt = b"\x01\x02\x03\x04"
        transport = MockTransport(
            [
                _auth_md5(salt),
                _auth_ok()
                + _param_status("server_version", "14.0")
                + _backend_key(333, 444)
                + _ready_for_query(),
            ]
        )
        proto = PGProtocol(transport.send, transport.recv)
        params = await proto.startup("user", "pass", "db")

        assert params["server_version"] == "14.0"
        assert len(transport.sent) == 2
        # Verify MD5 hash format: "md5" + hex digest
        pw_msg = transport.sent[1]
        # Extract password from message (after type byte + length)
        payload_start = 5
        password_bytes = pw_msg[payload_start:-1]  # strip trailing \0
        assert password_bytes.startswith(b"md5")

    @pytest.mark.asyncio
    async def test_startup_error_raises(self):
        transport = MockTransport([_error_response(message="no such database")])
        proto = PGProtocol(transport.send, transport.recv)
        with pytest.raises(NeonAuthenticationError, match="no such database"):
            await proto.startup("user", "pass", "baddb")


class TestPGProtocolSimpleQuery:
    @pytest.mark.asyncio
    async def test_simple_query_select(self):
        transport = MockTransport([_startup_response_trust()])
        proto = PGProtocol(transport.send, transport.recv)
        await proto.startup("u", "p", "db")

        # Set up response for the query
        query_response = (
            _row_description([("v", 23)])
            + _data_row([b"42"])
            + _command_complete("SELECT 1")
            + _ready_for_query()
        )
        transport._data.extend(query_response)
        transport._pos = len(transport._data) - len(query_response)

        results = await proto.simple_query("SELECT 42 AS v")
        assert len(results) == 1
        assert results[0].command_tag == "SELECT 1"
        assert len(results[0].fields) == 1
        assert results[0].fields[0].name == "v"
        assert results[0].rows == [[b"42"]]

    @pytest.mark.asyncio
    async def test_simple_query_begin(self):
        transport = MockTransport([_startup_response_trust()])
        proto = PGProtocol(transport.send, transport.recv)
        await proto.startup("u", "p", "db")

        query_response = _command_complete("BEGIN") + _ready_for_query("T")
        transport._data.extend(query_response)
        transport._pos = len(transport._data) - len(query_response)

        results = await proto.simple_query("BEGIN")
        assert len(results) == 1
        assert results[0].command_tag == "BEGIN"
        assert results[0].rows == []
        assert proto.transaction_status == "T"

    @pytest.mark.asyncio
    async def test_simple_query_empty(self):
        transport = MockTransport([_startup_response_trust()])
        proto = PGProtocol(transport.send, transport.recv)
        await proto.startup("u", "p", "db")

        query_response = _empty_query() + _ready_for_query()
        transport._data.extend(query_response)
        transport._pos = len(transport._data) - len(query_response)

        results = await proto.simple_query("")
        assert len(results) == 1
        assert results[0].command_tag == ""

    @pytest.mark.asyncio
    async def test_simple_query_error(self):
        transport = MockTransport([_startup_response_trust()])
        proto = PGProtocol(transport.send, transport.recv)
        await proto.startup("u", "p", "db")

        query_response = (
            _error_response(code="42601", message="syntax error at position 1")
            + _ready_for_query()
        )
        transport._data.extend(query_response)
        transport._pos = len(transport._data) - len(query_response)

        with pytest.raises(NeonQueryError, match="syntax error"):
            await proto.simple_query("INVALID SQL")


class TestPGProtocolExtendedQuery:
    @pytest.mark.asyncio
    async def test_extended_query_with_params(self):
        transport = MockTransport([_startup_response_trust()])
        proto = PGProtocol(transport.send, transport.recv)
        await proto.startup("u", "p", "db")

        query_response = (
            _parse_complete()
            + _bind_complete()
            + _row_description([("id", 23), ("name", 25)])
            + _data_row([b"1", b"alice"])
            + _command_complete("SELECT 1")
            + _ready_for_query()
        )
        transport._data.extend(query_response)
        transport._pos = len(transport._data) - len(query_response)

        result = await proto.extended_query(
            "SELECT id, name FROM users WHERE id = $1", [b"1"]
        )
        assert result.command_tag == "SELECT 1"
        assert len(result.fields) == 2
        assert result.fields[0].name == "id"
        assert result.fields[1].name == "name"
        assert result.rows == [[b"1", b"alice"]]

    @pytest.mark.asyncio
    async def test_extended_query_no_rows(self):
        transport = MockTransport([_startup_response_trust()])
        proto = PGProtocol(transport.send, transport.recv)
        await proto.startup("u", "p", "db")

        query_response = (
            _parse_complete()
            + _bind_complete()
            + _no_data()
            + _command_complete("INSERT 0 1")
            + _ready_for_query()
        )
        transport._data.extend(query_response)
        transport._pos = len(transport._data) - len(query_response)

        result = await proto.extended_query(
            "INSERT INTO t(v) VALUES ($1)", [b"hello"]
        )
        assert result.command_tag == "INSERT 0 1"
        assert result.fields == []
        assert result.rows == []

    @pytest.mark.asyncio
    async def test_extended_query_null_params(self):
        transport = MockTransport([_startup_response_trust()])
        proto = PGProtocol(transport.send, transport.recv)
        await proto.startup("u", "p", "db")

        query_response = (
            _parse_complete()
            + _bind_complete()
            + _row_description([("v", 25)])
            + _data_row([None])
            + _command_complete("SELECT 1")
            + _ready_for_query()
        )
        transport._data.extend(query_response)
        transport._pos = len(transport._data) - len(query_response)

        result = await proto.extended_query("SELECT $1::text AS v", [None])
        assert result.rows == [[None]]

    @pytest.mark.asyncio
    async def test_extended_query_error_resyncs(self):
        transport = MockTransport([_startup_response_trust()])
        proto = PGProtocol(transport.send, transport.recv)
        await proto.startup("u", "p", "db")

        query_response = (
            _error_response(code="42P01", message="relation does not exist")
            + _ready_for_query()
        )
        transport._data.extend(query_response)
        transport._pos = len(transport._data) - len(query_response)

        with pytest.raises(NeonQueryError, match="relation does not exist"):
            await proto.extended_query("SELECT * FROM nonexistent", [])

        # Protocol should be resynced (ReadyForQuery consumed)
        assert proto.transaction_status == "I"

    @pytest.mark.asyncio
    async def test_extended_query_multiple_rows(self):
        transport = MockTransport([_startup_response_trust()])
        proto = PGProtocol(transport.send, transport.recv)
        await proto.startup("u", "p", "db")

        query_response = (
            _parse_complete()
            + _bind_complete()
            + _row_description([("n", 23)])
            + _data_row([b"1"])
            + _data_row([b"2"])
            + _data_row([b"3"])
            + _command_complete("SELECT 3")
            + _ready_for_query()
        )
        transport._data.extend(query_response)
        transport._pos = len(transport._data) - len(query_response)

        result = await proto.extended_query("SELECT generate_series(1,3) AS n", [])
        assert len(result.rows) == 3
        assert result.rows[0] == [b"1"]
        assert result.rows[1] == [b"2"]
        assert result.rows[2] == [b"3"]


class TestPGProtocolTerminate:
    @pytest.mark.asyncio
    async def test_terminate_sends_message(self):
        transport = MockTransport([_startup_response_trust()])
        proto = PGProtocol(transport.send, transport.recv)
        await proto.startup("u", "p", "db")

        await proto.terminate()
        last_msg = transport.sent[-1]
        assert last_msg[0] == ord("X")
        assert len(last_msg) == 5

    @pytest.mark.asyncio
    async def test_terminate_ignores_errors(self):
        """Terminate should not raise even if send fails."""

        async def failing_send(data: bytes) -> None:
            raise ConnectionError("already closed")

        async def recv() -> bytes:
            return b""

        proto = PGProtocol(failing_send, recv)
        # Should not raise
        await proto.terminate()


class TestPGProtocolChunkedTransport:
    @pytest.mark.asyncio
    async def test_startup_over_chunked_transport(self):
        """Startup works when server data arrives in tiny chunks."""
        data = _startup_response_trust()
        transport = ChunkedMockTransport(data, chunk_size=3)
        proto = PGProtocol(transport.send, transport.recv)
        params = await proto.startup("u", "p", "db")
        assert params["server_version"] == "16.0"
