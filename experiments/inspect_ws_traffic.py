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

def decode_pg_msg(data):
    if len(data) < 5:
        return f"Partial/Unknown: {data}"
    
    msg_type = data[0]
    length = struct.unpack("!I", data[1:5])[0]
    payload = data[5:]
    
    type_char = chr(msg_type)
    return f"Type: {type_char} ({msg_type}), Length: {length}, Payload: {payload[:20]}..."

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
    print(f"Connecting to {ws_url} with protocol 'neon-v2'...")

    # Define authentication logic
    async def handle_auth(ws, data, user, password):
         # Expect AuthenticationCleartextPassword (3) for this test based on previous run
         # or SASL if configured differently.
         # The previous run showed Type: R, Length: 8, Payload: \x00\x00\x00\x03 (Cleartext)
         
         if len(data) >= 9 and data[0] == ord('R'):
             auth_type = struct.unpack("!I", data[5:9])[0]
             if auth_type == 3: # Cleartext
                 print("  Auth requested: Cleartext")
                 # Send PasswordMessage ('p')
                 # format: 'p' + invalid_length + password + \0
                 pwd_enc = password.encode() + b"\x00"
                 length = len(pwd_enc) + 4
                 msg = b'p' + struct.pack("!I", length) + pwd_enc
                 print(f"  Sending Password ({len(msg)} bytes)")
                 await ws.send_bytes(msg)
                 return True
             elif auth_type == 0: # AuthOK
                 print("  Auth OK!")
                 return True
             else:
                 print(f"  Auth requested type: {auth_type} (Not implemented in this script)")
                 return False
         return False

    async with aiohttp.ClientSession() as session:
        # Try specifically with the neon-v2 protocol to check for custom framing
        try:
            async with session.ws_connect(ws_url, protocols=["neon-v2"]) as ws: 
                print("Connected with neon-v2!")
                
                # Startup Message
                version = 196608
                body = bytearray(struct.pack("!I", version))
                body += b"user\x00" + user.encode() + b"\x00"
                body += b"database\x00" + database.encode() + b"\x00"
                body += b"\x00"
                length = len(body) + 4
                startup_msg = struct.pack("!I", length) + bytes(body)
                
                print(f"Sending Startup Message: {len(startup_msg)} bytes")
                await ws.send_bytes(startup_msg)
                
                while True:
                    msg = await ws.receive()
                    if msg.type == aiohttp.WSMsgType.BINARY:
                        data = msg.data
                        print(f"\n[RCV] {len(data)} bytes: {data.hex()[:50]}...")
                        decoded = decode_pg_msg(data)
                        print(f"      Parsed: {decoded}")
                        
                        if len(data) > 0 and data[0] == ord('R'):
                            await handle_auth(ws, data, user, password)
                            
                        if len(data) > 0 and data[0] == ord('Z'): # ReadyForQuery
                            print("ReadyForQuery received. Sending Query...")
                            query = "SELECT 1"
                            q_msg = b'Q' + struct.pack("!I", len(query) + 5) + query.encode() + b"\x00"
                            await ws.send_bytes(q_msg)
                            
                        if len(data) > 0 and data[0] == ord('C'): # CommandComplete
                             # Let's peek if we can send another query immediately?
                             # Or just finish
                             print("Command Complete. Test Finished.")
                             break
                             
                    elif msg.type == aiohttp.WSMsgType.CLOSED:
                        print("Connection closed")
                        break
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        print("Connection error")
                        break
        except Exception as e:
            print(f"Failed to connect or run with neon-v2: {e}")

if __name__ == "__main__":
    asyncio.run(main())
