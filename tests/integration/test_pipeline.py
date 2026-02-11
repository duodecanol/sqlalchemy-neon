import pytest
import os
from sqlalchemy_neon.neon_http_client import AsyncNeonWebSocketClient
from sqlalchemy_neon.errors import NeonAuthenticationError, NeonQueryError

@pytest.mark.asyncio
async def test_pipeline_happy_path():
    """Test that a query executes successfully with pipelining."""
    connection_string = os.environ.get("NEON_DATABASE_URL")
    if not connection_string:
        pytest.skip("NEON_DATABASE_URL not set")

    # Manually use the client to ensure we are testing the query method logic
    async with AsyncNeonWebSocketClient(connection_string) as client:
        # Client starts unconnected.
        # First query should trigger pipeline.
        result = await client.query("SELECT 1 as val")
        assert result.row_count == 1
        assert result.rows[0]["val"] == 1
        
        # Subsequent query should use established connection
        result2 = await client.query("SELECT 2 as val")
        assert result2.row_count == 1
        assert result2.rows[0]["val"] == 2

@pytest.mark.asyncio
async def test_pipeline_auth_fail():
    """Test that pipelining fails correctly with wrong password."""
    connection_string = os.environ.get("NEON_DATABASE_URL")
    if not connection_string:
        pytest.skip("NEON_DATABASE_URL not set")
    
    # Modify connection string to have wrong password
    from urllib.parse import urlparse, urlunparse
    parsed = urlparse(connection_string)
    # Reconstruct with wrong password (simple string replace for simplicity if no special chars)
    # But let's use replace
    new_netloc = f"{parsed.username}:wrongpass@{parsed.hostname}"
    if parsed.port:
        new_netloc += f":{parsed.port}"
        
    wrong_conn_str = parsed._replace(netloc=new_netloc).geturl()
    
    with pytest.raises(NeonAuthenticationError):
        async with AsyncNeonWebSocketClient(wrong_conn_str) as client:
            await client.query("SELECT 1")

@pytest.mark.asyncio
async def test_pipeline_query_error():
    """Test that pipeline connects but returns query error if SQL is bad."""
    connection_string = os.environ.get("NEON_DATABASE_URL")
    if not connection_string:
        pytest.skip("NEON_DATABASE_URL not set")

    async with AsyncNeonWebSocketClient(connection_string) as client:
        # First query is bad SQL
        with pytest.raises(NeonQueryError):
            await client.query("SELECT * FROM non_existent_table")
            
        # Verify subsequent query works (connection remains valid)
        result = await client.query("SELECT 1 as val")
        assert result.rows[0]["val"] == 1
