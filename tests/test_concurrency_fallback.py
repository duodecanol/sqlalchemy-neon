import asyncio
from sqlalchemy_neon.concurrency import await_only_allow_missing


async def my_coro():
    return "result"


def test_no_greenlet():
    coro = my_coro()
    # Should return the coroutine itself because we are not in a child greenlet
    result = await_only_allow_missing(coro)

    if asyncio.iscoroutine(result):
        print("SUCCESS: Returned coroutine as expected (fallback)")
        # Clean up coroutine to avoid warning
        asyncio.run(result)
    else:
        print(f"FAILURE: Returned {result} instead of coroutine")


if __name__ == "__main__":
    test_no_greenlet()
