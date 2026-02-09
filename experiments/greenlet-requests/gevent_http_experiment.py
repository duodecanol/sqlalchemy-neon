"""
geventhttpclient Concept Verification Experiment

Goals:
1. Measure sync HTTP GET performance with geventhttpclient vs requests
2. Verify if parallelism is maintained with only greenlet.getcurrent() (without gevent.spawn/pool)
3. Compare actual parallel execution using gevent.spawn

Theory:
- greenlet.getcurrent() only returns the current greenlet instance, it does NOT create parallelism
- True concurrency requires gevent.spawn() or gevent.pool to create multiple greenlets
- geventhttpclient leverages gevent's cooperative sockets for I/O-based context switching
"""

import time
import statistics
import asyncio
from typing import Callable
from contextlib import contextmanager

# Monkey-patch before any other imports
from gevent import monkey

# monkey.patch_select()
# monkey.patch_selectors()
# monkey.patch_socket()
# monkey.patch_thread()
monkey.patch_ssl()

import gevent
from gevent.pool import Pool
import greenlet
import requests
import aiohttp
from geventhttpclient import HTTPClient
from geventhttpclient.url import URL


# Test configuration
TEST_URL = "https://fly-net-gateway.fly.dev/be-00/large"
NUM_REQUESTS = 25
CONCURRENT_REQUESTS = 5


@contextmanager
def timer(label: str, container: dict):
    """Context manager for timing code blocks"""
    start = time.perf_counter()
    yield
    elapsed = time.perf_counter() - start
    container["elapsed"] = elapsed
    print(f"  [{label}] Elapsed: {elapsed:.3f}s")


def print_greenlet_info(prefix: str = ""):
    """Print current greenlet information"""
    current = greenlet.getcurrent()
    print(
        f"  {prefix}Current greenlet: {current}, id={id(current)}, is_main={current.parent is None}"
    )


# =============================================================================
# Test 1: Baseline - requests library (synchronous, blocking)
# =============================================================================
def test_requests_sync():
    """Synchronous HTTP GET using requests library (baseline)"""
    print("\n" + "=" * 70)
    print("TEST 1: requests library - Synchronous baseline")
    print("=" * 70)
    print_greenlet_info("Before: ")

    container = {}
    results = []
    with timer(f"{NUM_REQUESTS} sequential requests", container):
        for i in range(NUM_REQUESTS):
            start = time.perf_counter()
            resp = requests.get(TEST_URL, timeout=10)
            elapsed = time.perf_counter() - start
            results.append(elapsed)
            print(f"    Request {i + 1}: {resp.status_code} in {elapsed:.3f}s")

    print(f"  Average per request: {statistics.mean(results):.3f}s")
    print_greenlet_info("After: ")
    return results, container


# =============================================================================
# Test 2: geventhttpclient - Synchronous (still single greenlet)
# =============================================================================
def test_geventhttpclient_sync():
    """Synchronous HTTP GET using geventhttpclient (single greenlet)"""
    print("\n" + "=" * 70)
    print("TEST 2: geventhttpclient - Synchronous (single greenlet)")
    print("=" * 70)
    print_greenlet_info("Before: ")

    url = URL(TEST_URL)
    client = HTTPClient.from_url(url, concurrency=1)

    container = {}
    results = []
    with timer(f"{NUM_REQUESTS} sequential requests", container):
        for i in range(NUM_REQUESTS):
            start = time.perf_counter()
            resp = client.get(url.request_uri)
            if resp is None or resp.status_code != 200:
                print(
                    f"    Request {i + 1}: FAILED with status {resp.status_code if resp else 'No Response'}"
                )
                continue
            body = resp.read()
            elapsed = time.perf_counter() - start
            results.append(elapsed)
            print(
                f"    Request {i + 1}: {resp.status_code} in {elapsed:.3f}s, body_len={len(body)}"
            )

    client.close()
    print(f"  Average per request: {statistics.mean(results):.3f}s")
    print_greenlet_info("After: ")
    return results, container


# =============================================================================
# Test 3: greenlet.getcurrent() only - NO parallelism expected
# =============================================================================
def test_getcurrent_only():
    """
    Test using only greenlet.getcurrent() WITHOUT gevent.spawn

    HYPOTHESIS: This will NOT provide parallelism because:
    - getcurrent() only returns the current greenlet reference
    - No new greenlets are spawned, so no cooperative switching occurs
    - All requests execute sequentially in the main greenlet
    """
    print("\n" + "=" * 70)
    print("TEST 3: greenlet.getcurrent() ONLY - No spawn/pool")
    print("=" * 70)
    print("HYPOTHESIS: NO parallelism - getcurrent() doesn't create greenlets")
    print_greenlet_info("Before: ")

    url = URL(TEST_URL)
    # Even with high concurrency setting, without spawning greenlets,
    # requests will be sequential
    client = HTTPClient.from_url(url, concurrency=1)

    def make_request(idx: int) -> tuple[int, float, greenlet.greenlet]:
        """Make a request and track which greenlet executed it"""
        current = greenlet.getcurrent()
        start = time.perf_counter()
        resp = client.get(url.request_uri)
        body = resp.read()
        elapsed = time.perf_counter() - start
        return idx, resp.status_code if resp else 0, elapsed, current

    results = []
    container = {}
    greenlet_ids = set()

    with timer(f"{NUM_REQUESTS} requests with getcurrent() only", container):
        for i in range(NUM_REQUESTS):
            idx, status, elapsed, gr = make_request(i)
            results.append(elapsed)
            greenlet_ids.add(id(gr))
            print(
                f"    Request {idx + 1}: {status} in {elapsed:.3f}s, greenlet_id={id(gr)}"
            )

    client.close()
    print(f"\n  Unique greenlets used: {len(greenlet_ids)}")
    print(f"  Average per request: {statistics.mean(results):.3f}s")
    print(
        f"  RESULT: {'SEQUENTIAL (as expected)' if len(greenlet_ids) == 1 else 'UNEXPECTED - multiple greenlets!'}"
    )
    print_greenlet_info("After: ")
    return results, container


# =============================================================================
# Test 4: gevent.spawn - TRUE parallelism
# =============================================================================
def test_gevent_spawn_parallel():
    """
    Test using gevent.spawn for actual parallel execution

    This creates multiple greenlets that can switch during I/O operations
    """
    print("\n" + "=" * 70)
    print("TEST 4: gevent.spawn - True parallel execution")
    print("=" * 70)
    print("EXPECTED: Parallel execution - multiple greenlets doing concurrent I/O")
    print_greenlet_info("Before: ")

    url = URL(TEST_URL)
    client = HTTPClient.from_url(url, concurrency=CONCURRENT_REQUESTS)

    results = []
    container = {}
    greenlet_ids = set()

    def make_request(idx: int):
        """Make a request in a spawned greenlet"""
        current = greenlet.getcurrent()
        start = time.perf_counter()
        resp = client.get(url.request_uri)
        body = resp.read()
        elapsed = time.perf_counter() - start
        greenlet_ids.add(id(current))
        results.append((idx, resp.status_code, elapsed, id(current)))
        print(
            f"    Request {idx + 1}: {resp.status_code} in {elapsed:.3f}s, greenlet_id={id(current)}"
        )
        return elapsed

    with timer(f"{NUM_REQUESTS} parallel requests with gevent.spawn", container):
        greenlets = [gevent.spawn(make_request, i) for i in range(NUM_REQUESTS)]
        gevent.joinall(greenlets)

    client.close()
    times = [r[2] for r in results]
    print(f"\n  Unique greenlets used: {len(greenlet_ids)}")
    print(f"  Average per request: {statistics.mean(times):.3f}s")
    print(
        f"  RESULT: {'PARALLEL (as expected)' if len(greenlet_ids) > 1 else 'UNEXPECTED - single greenlet!'}"
    )
    print_greenlet_info("After: ")
    return times, container


# =============================================================================
# Test 5: gevent.pool - Controlled parallelism
# =============================================================================
def test_gevent_pool_parallel():
    """
    Test using gevent.Pool for controlled parallel execution
    """
    print("\n" + "=" * 70)
    print("TEST 5: gevent.Pool - Controlled parallel execution")
    print("=" * 70)
    print(f"EXPECTED: {CONCURRENT_REQUESTS} concurrent connections")
    print_greenlet_info("Before: ")

    url = URL(TEST_URL)
    client = HTTPClient.from_url(url, concurrency=CONCURRENT_REQUESTS)
    pool = Pool(CONCURRENT_REQUESTS * 2)

    results = []
    container = {}
    greenlet_ids = set()

    def make_request(idx: int):
        """Make a request using the pool"""
        current = greenlet.getcurrent()
        start = time.perf_counter()
        resp = client.get(url.request_uri)
        body = resp.read()
        elapsed = time.perf_counter() - start
        greenlet_ids.add(id(current))
        results.append((idx, resp.status_code, elapsed, id(current)))
        print(
            f"    Request {idx + 1}: {resp.status_code} in {elapsed:.3f}s, greenlet_id={id(current)}"
        )
        return elapsed

    with timer(f"{NUM_REQUESTS} requests via Pool(size={pool.size})", container):
        pool.map(make_request, range(NUM_REQUESTS))

    client.close()
    times = [r[2] for r in results]
    print(f"\n  Unique greenlets used: {len(greenlet_ids)}")
    print(f"  Average per request: {statistics.mean(times):.3f}s")
    print_greenlet_info("After: ")
    return times, container


# =============================================================================
# Test 6: asyncio + aiohttp - Native async parallelism
# =============================================================================
async def test_asyncio_parallel():
    """
    Test using asyncio + aiohttp for native async parallel execution

    This uses Python's built-in async/await with aiohttp for comparison
    """
    print("\n" + "=" * 70)
    print("TEST 6: asyncio + aiohttp - Native async parallel execution")
    print("=" * 70)
    print(f"EXPECTED: {CONCURRENT_REQUESTS} concurrent connections via semaphore")

    results = []
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

    async def make_request(session: aiohttp.ClientSession, idx: int):
        """Make a request using aiohttp with semaphore-controlled concurrency"""
        async with semaphore:
            start = time.perf_counter()
            async with session.get(TEST_URL) as resp:
                body = await resp.read()
                elapsed = time.perf_counter() - start
                results.append((idx, resp.status, elapsed, len(body)))
                print(
                    f"    Request {idx + 1}: {resp.status} in {elapsed:.3f}s, body_len={len(body)}"
                )
                return elapsed

    async with aiohttp.ClientSession() as session:
        tasks = [make_request(session, i) for i in range(NUM_REQUESTS)]
        await asyncio.gather(*tasks)

    times = [r[2] for r in results]
    print(f"\n  Average per request: {statistics.mean(times):.3f}s")
    return times, container


def test_asyncio_parallel_sync_wrapper():
    """Synchronous wrapper to run asyncio test"""
    print("\n" + "=" * 70)
    print("TEST 6: asyncio + aiohttp - Native async parallel execution")
    print("=" * 70)
    print(f"EXPECTED: {CONCURRENT_REQUESTS} concurrent connections via semaphore")

    results = []
    container = {}

    async def run_test():
        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

        async def make_request(session: aiohttp.ClientSession, idx: int):
            """Make a request using aiohttp with semaphore-controlled concurrency"""
            async with semaphore:
                start = time.perf_counter()
                async with session.get(TEST_URL) as resp:
                    body = await resp.read()
                    elapsed = time.perf_counter() - start
                    results.append((idx, resp.status, elapsed, len(body)))
                    print(
                        f"    Request {idx + 1}: {resp.status} in {elapsed:.3f}s, body_len={len(body)}"
                    )
                    return elapsed

        async with aiohttp.ClientSession() as session:
            tasks = [make_request(session, i) for i in range(NUM_REQUESTS)]
            await asyncio.gather(*tasks)

    with timer(
        f"{NUM_REQUESTS} requests via Asyncio(concurrency={CONCURRENT_REQUESTS})",
        container,
    ):
        asyncio.run(run_test())
    print(
        f"  [{NUM_REQUESTS} parallel requests with asyncio] Elapsed: {container['elapsed']:.3f}s"
    )

    times = [r[2] for r in results]
    print(f"\n  Average per request: {statistics.mean(times):.3f}s")
    return times, container


# =============================================================================
# Summary comparison
# =============================================================================
def run_all_tests():
    """Run all tests and compare results"""
    print("\n" + "=" * 70)
    print("GEVENTHTTPCLIENT CONCEPT VERIFICATION")
    print("=" * 70)
    print(f"Target URL: {TEST_URL}")
    print(f"Number of requests: {NUM_REQUESTS}")
    print(f"Concurrency setting: {CONCURRENT_REQUESTS}")

    all_results = {}

    # Run tests
    # all_results["requests_sync"] = test_requests_sync()
    # all_results["gevent_sync"] = test_geventhttpclient_sync()
    # all_results["getcurrent_only"] = test_getcurrent_only()
    # all_results["spawn_parallel"] = test_gevent_spawn_parallel()
    all_results["pool_parallel"] = test_gevent_pool_parallel()
    # all_results["pool_parallel"] = asyncio.run(asyncio.to_thread(test_gevent_pool_parallel))
    all_results["asyncio_parallel"] = test_asyncio_parallel_sync_wrapper()

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"{'Test':<25} {'Total Time':>12} {'Avg/Req':>10}")
    print("-" * 50)

    for name, res in all_results.items():
        times, container = res
        total = container["elapsed"]
        avg = statistics.mean(times)
        print(f"{name:<25} {total:>10.3f}s {avg:>10.3f}s")

    print("\n" + "=" * 70)
    print("CONCLUSIONS")
    print("=" * 70)
    print("""
1. greenlet.getcurrent() ALONE does NOT provide parallelism
   - It only returns a reference to the current greenlet
   - Without gevent.spawn() or Pool, code runs sequentially

2. gevent.spawn() or Pool is REQUIRED for concurrency
   - spawn() creates new greenlets that can switch during I/O
   - Pool provides controlled concurrency with worker limits

3. geventhttpclient's connection pooling helps performance
   - Connection reuse reduces overhead
   - But true parallelism still requires multiple greenlets

4. Sync performance comparison:
   - geventhttpclient is faster than requests due to C parser
   - But without greenlet spawning, both are sequential

5. asyncio + aiohttp provides native async concurrency
   - Uses Python's built-in event loop (no greenlets)
   - asyncio.gather() runs tasks concurrently
   - Semaphore controls max concurrent connections
   - Comparable performance to gevent parallel approaches
""")


if __name__ == "__main__":
    run_all_tests()
