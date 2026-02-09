import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy_neon import NeonAsyncSession, NotSupportedError


@pytest.mark.asyncio
async def test_neon_async_session_run_sync_error(require_neon):
    """Verify that NeonAsyncSession.run_sync raises NotSupportedError."""
    neon_url = require_neon.replace("postgresql://", "postgresql+neonhttp_async://")
    engine = create_async_engine(neon_url)

    async with engine.connect() as conn:
        async with NeonAsyncSession(conn) as session:

            def sync_func():
                return "sync result"

            # Should raise NotSupportedError
            with pytest.raises(NotSupportedError) as excinfo:
                await session.run_sync(sync_func)

            assert "NeonAsyncSession.run_sync() is not supported" in str(excinfo.value)

    await engine.dispose()
