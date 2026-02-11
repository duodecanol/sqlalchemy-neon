import asyncio
import os
import aiohttp
import struct
from urllib.parse import urlparse


def build_ws_url(connection_string):
    parsed = urlparse(connection_string)
    host = parsed.hostname
    port = parsed.port or 5432
    return f"wss://{host}/v2"


async def main():
    connection_string = os.environ.get("NEON_DATABASE_URL")
    if not connection_string:
        print("NEON_DATABASE_URL not set")
        return

    parsed = urlparse(connection_string)
    user = parsed.username
    password = parsed.password
    database = (parsed.path or "/neondb").lstrip("/")

    ws_url = build_ws_url(connection_string)
    print(f"Connecting to {ws_url}...")

    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(ws_url, protocols=["neon-v2"]) as ws:
            print("Connected!")

            # 1. Startup Message
            version = 196608
            body = bytearray(struct.pack("!I", version))
            body += b"user\x00" + user.encode() + b"\x00"
            body += b"database\x00" + database.encode() + b"\x00"
            body += b"\x00"
            length = len(body) + 4
            startup_msg = struct.pack("!I", length) + bytes(body)

            # 2. Password Message (Cleartext)
            pwd_enc = password.encode() + b"\x00"
            p_len = len(pwd_enc) + 4
            password_msg = b"p" + struct.pack("!I", p_len) + pwd_enc

            # 3. Query Message (Simple Query)
            query = "SELECT 'pipeline_success' as status"
            q_enc = query.encode() + b"\x00"
            q_len = len(q_enc) + 4
            query_msg = b"Q" + struct.pack("!I", q_len) + q_enc

            # Pipeline them!
            full_msg = startup_msg + password_msg + query_msg
            print(f"Sending {len(full_msg)} bytes (Startup + Password + Query)")
            await ws.send_bytes(full_msg)

            while True:
                msg = await ws.receive()
                if msg.type == aiohttp.WSMsgType.BINARY:
                    data = msg.data
                    print(f"[RCV] {len(data)} bytes")

                    # Parse messages in the buffer
                    pos = 0
                    while pos < len(data):
                        msg_type = data[pos]
                        length = struct.unpack("!I", data[pos + 1 : pos + 5])[0]
                        total_len = length + 1
                        payload = data[pos + 5 : pos + total_len]

                        type_char = chr(msg_type)
                        print(f"  Type: {type_char} ({msg_type}), Len: {length}")

                        if msg_type == ord("R"):  # Auth
                            auth_type = struct.unpack("!I", payload[:4])[0]
                            print(f"    Auth Type: {auth_type}")
                        elif msg_type == ord("Z"):  # ReadyForQuery
                            print("    ReadyForQuery")
                        elif msg_type == ord("T"):  # RowDescription
                            print("    RowDescription")
                        elif msg_type == ord("D"):  # DataRow
                            # parsing simplisticly
                            n_cols = struct.unpack("!H", payload[:2])[0]
                            val_len = struct.unpack("!i", payload[2:6])[0]
                            val = payload[6 : 6 + val_len].decode()
                            print(f"    DataRow: {val}")
                        elif msg_type == ord("C"):  # CommandComplete
                            print(f"    CommandComplete: {payload[:-1].decode()}")
                            return
                        elif msg_type == ord("E"):  # Error
                            print(f"    Error: {payload}")
                            return

                        pos += total_len

                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break


if __name__ == "__main__":
    asyncio.run(main())
