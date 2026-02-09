import httpx

url = "https://example.com"

with httpx.Client() as client:
    response = client.get(url)
    print(f"{response.http_version = }")
    print(f"{response.status_code = }")
    print(response.headers)

# response.http_version = "HTTP/1.1"
# response.status_code = 200
# Headers(
#     {
#         "date": "Fri, 06 Feb 2026 10:06:58 GMT",
#         "content-type": "text/html",
#         "transfer-encoding": "chunked",
#         "connection": "keep-alive",
#         "content-encoding": "gzip",
#         "last-modified": "Wed, 04 Feb 2026 05:22:19 GMT",
#         "allow": "GET, HEAD",
#         "age": "10620",
#         "cf-cache-status": "HIT",
#         "vary": "Accept-Encoding",
#         "server": "cloudflare",
#         "cf-ray": "9c99d681ab7cea9b-ICN",
#     }
# )
print("=" * 88)

with httpx.Client(http2=True) as client:
    response = client.get(url)
    print(f"{response.http_version = }")
    print(f"{response.status_code = }")
    print(response.headers)

# response.http_version = "HTTP/2"
# response.status_code = 200
# Headers(
#     {
#         "date": "Fri, 06 Feb 2026 10:25:24 GMT",
#         "content-type": "text/html",
#         "content-encoding": "gzip",
#         "last-modified": "Wed, 04 Feb 2026 05:22:19 GMT",
#         "allow": "GET, HEAD",
#         "age": "11726",
#         "cf-cache-status": "HIT",
#         "vary": "Accept-Encoding",
#         "server": "cloudflare",
#         "cf-ray": "9c99f1827f5e326f-ICN",
#     }
# )
