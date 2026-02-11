import pytest
import sqlalchemy as sa
import json
import random
import string
import uuid
import os
from sqlalchemy_neon import NeonNativeAsyncEngine
from testsupport.models import ComplexData

def generate_complex_json(depth=5, items_per_level=3):
    """Generate a deeply nested and complex JSON object."""
    if depth <= 0:
        return {
            "id": str(uuid.uuid4()),
            "value": random.random(),
            "text": "".join(random.choices(string.printable, k=100)),
            "active": random.choice([True, False, None]),
            "weird_chars": "🚀 💫 漢字 日本語 한국어  ' \" \\ \b \f \n \r \t",
            "escaped": "newline\nand \"quote\"",
        }
    
    data = {}
    for i in range(items_per_level):
        key = f"key_{depth}_{i}_{''.join(random.choices(string.ascii_lowercase, k=5))}"
        if random.random() > 0.3:
            data[key] = generate_complex_json(depth - 1, items_per_level)
        else:
            data[key] = [generate_complex_json(depth - 1, 2) for _ in range(2)]
    
    return data

@pytest.mark.asyncio
async def test_large_complex_json_insertion(neondb: NeonNativeAsyncEngine):
    """
    Test insertion of a large and complicated JSON payload.
    This aims to reproduce or verify fixes for the 400 error observed with long payloads.
    """
    # Create the table if it doesn't exist (though usually neondb fixture handles migrations or base)
    # The models are usually already reflected in the db for these tests.
    
    # Generate a really large payload
    # Depth 4, 4 items per level is around 70-100KB
    complex_obj = generate_complex_json(depth=4, items_per_level=4)
    payload_size = len(json.dumps(complex_obj))
    print(f"\nGenerated complex JSON payload size: {payload_size / 1024:.2f} KB")

    name_val = f"complex_test_{uuid.uuid4().hex[:8]}"
    
    # Create the instance
    entry = ComplexData(
        name=name_val,
        data_json=complex_obj,
        data_jsonb=complex_obj,
        metadata_jsonb={"test_run": True, "size": payload_size, "weird": "🚀"}
    )
    
    # Add to database
    await neondb.add(entry)
    
    assert entry.id is not None
    
    # Fetch back and verify
    result = await neondb.execute(
        sa.select(ComplexData).where(ComplexData.id == entry.id)
    )
    fetched = result.scalar_one()
    
    assert fetched.name == name_val
    assert fetched.data_jsonb == complex_obj
    assert fetched.data_json == complex_obj
    assert fetched.metadata_jsonb["weird"] == "🚀"
    
    # Partial update test
    updated_metadata = {"test_run": False, "new_field": "partially updated", "original_size": payload_size, "emoji": "✨"}
    await neondb.execute(
        sa.update(ComplexData)
        .where(ComplexData.id == entry.id)
        .values(metadata_jsonb=updated_metadata)
    )
    
    result = await neondb.execute(
        sa.select(ComplexData).where(ComplexData.id == entry.id)
    )
    fetched_updated = result.scalar_one()
    assert fetched_updated.metadata_jsonb == updated_metadata
    
    # Cleanup
    await neondb.execute(sa.delete(ComplexData).where(ComplexData.id == entry.id))

@pytest.mark.asyncio
async def test_very_large_json_payload(neondb: NeonNativeAsyncEngine):
    """Test a truly large payload to push limits (e.g. > 2MB)."""
    # Depth 7, 4 items per level should generate a large but manageable blob (~3MB)
    large_obj = generate_complex_json(depth=7, items_per_level=4)
    payload_size = len(json.dumps(large_obj))
    print(f"\nGenerated massive JSON payload size: {payload_size / 1024:.2f} KB")
    
    name_val = f"massive_test_{uuid.uuid4().hex[:8]}"
    entry = ComplexData(name=name_val, data_jsonb=large_obj)
    
    await neondb.add(entry)
    
    result = await neondb.execute(
        sa.select(ComplexData).where(ComplexData.id == entry.id)
    )
    fetched = result.scalar_one()
    assert len(json.dumps(fetched.data_jsonb)) == payload_size
    
    await neondb.execute(sa.delete(ComplexData).where(ComplexData.id == entry.id))

@pytest.mark.asyncio
async def test_many_parameters(neondb: NeonNativeAsyncEngine):
    """Test a query with a large number of parameters."""
    num_params = 1000
    params = {f"p_{i}": f"val_{i}" for i in range(num_params)}
    
    # Construct a query using these params
    # This is a bit contrived but tests batch params
    stmt = sa.text(f"SELECT " + ", ".join([f":p_{i}" for i in range(num_params)]))
    
    result = await neondb.execute(stmt, params)
    row = result.first()
    assert len(row) == num_params

@pytest.mark.asyncio
async def test_massive_parameters(neondb: NeonNativeAsyncEngine):
    """Test a query with a larger number of parameters (below 1664 target list limit)."""
    num_params = 1000
    params = {f"p_{i}": i for i in range(num_params)}
    
    # Simple SELECT of all params
    stmt = sa.text(f"SELECT " + ", ".join([f":p_{i}" for i in range(num_params)]))
    
    result = await neondb.execute(stmt, params)
    row = result.first()
    assert len(row) == num_params

@pytest.mark.asyncio
async def test_large_json_websocket(require_neon):
    """Test large JSON payload over WebSocket transport."""
    from sqlalchemy_neon import create_neon_native_async_engine
    
    engine = create_neon_native_async_engine(
        require_neon,
        transport="websocket"
    )
    
    try:
        # Generate medium-large payload (say 1MB)
        obj = generate_complex_json(depth=6, items_per_level=5)
        print(f"\nWebSocket payload size: {len(json.dumps(obj))/1024:.2f} KB")
        
        name_val = f"ws_test_{uuid.uuid4().hex[:8]}"
        entry = ComplexData(name=name_val, data_jsonb=obj)
        
        await engine.add(entry)
        
        result = await engine.execute(
            sa.select(ComplexData).where(ComplexData.id == entry.id)
        )
        fetched = result.scalar_one()
        assert fetched.data_jsonb == obj
        
        await engine.execute(sa.delete(ComplexData).where(ComplexData.id == entry.id))
    finally:
        await engine.dispose()

@pytest.mark.asyncio
async def test_orm_object_large_json(neondb: NeonNativeAsyncEngine):
    """
    Test insertion of a large JSON payload using an ORM object via neondb.add().
    This directly uses the ORM-style interaction mentioned by the user.
    """
    # Generate ~20MB payload to really push limits
    # Depth 8, items 6 is massive. Let's aim for ~10MB first to avoid client timeout.
    large_obj = generate_complex_json(depth=6, items_per_level=7)
    payload_size = len(json.dumps(large_obj))
    print(f"\nORM object JSON payload size: {payload_size / 1024:.2f} KB")
    
    name_val = f"orm_test_{uuid.uuid4().hex[:8]}"
    
    # Create the ORM instance
    entry = ComplexData(
        name=name_val,
        data_jsonb=large_obj,
        data_json=large_obj,
        metadata_jsonb={"source": "orm_test", "size": payload_size}
    )
    
    # Use ORM add()
    await neondb.add(entry)
    
    assert entry.id is not None
    
    # Verify by fetching back
    result = await neondb.execute(
        sa.select(ComplexData).where(ComplexData.id == entry.id)
    )
    fetched = result.scalar_one()
    
    assert fetched.name == name_val
    assert len(json.dumps(fetched.data_jsonb)) == payload_size
    
    # Cleanup
    await neondb.execute(sa.delete(ComplexData).where(ComplexData.id == entry.id))

@pytest.mark.asyncio
async def test_extreme_mixed_payload(neondb: NeonNativeAsyncEngine):
    """Test a mix of large JSON, large bytea, and large text."""
    # 2MB of random bytes
    random_bytes = os.urandom(2 * 1024 * 1024)
    # 2MB of text
    large_text = "lorem ipsum " * (2 * 1024 * 1024 // 12)
    # 2MB of JSON
    large_json = generate_complex_json(depth=6, items_per_level=6)
    
    name_val = f"extreme_test_{uuid.uuid4().hex[:8]}"
    entry = ComplexData(
        name=name_val,
        data_jsonb=large_json,
        data_bytea=random_bytes,
        data_text=large_text
    )
    
    print(f"\nExtreme mixed payload: 2MB bytea, 2MB text, ~{len(json.dumps(large_json))/1024:.2f}KB JSON")
    
    await neondb.add(entry)
    assert entry.id is not None
    
    result = await neondb.execute(
        sa.select(ComplexData).where(ComplexData.id == entry.id)
    )
    fetched = result.scalar_one()
    
    assert fetched.data_bytea == random_bytes
    assert fetched.data_text == large_text
    assert fetched.data_jsonb == large_json
    
    await neondb.execute(sa.delete(ComplexData).where(ComplexData.id == entry.id))

@pytest.mark.asyncio
async def test_deep_json_payload(neondb: NeonNativeAsyncEngine):
    """Test a very deep JSON structure to check for recursion/depth limits."""
    # Generate 150 levels deep
    curr = {"val": "bottom"}
    for i in range(150):
        curr = {f"level_{i}": curr}
    
    name_val = f"deep_test_{uuid.uuid4().hex[:8]}"
    entry = ComplexData(name=name_val, data_jsonb=curr)
    
    # This might fail with 400 if the proxy has depth limits
    await neondb.add(entry)
    
    result = await neondb.execute(
        sa.select(ComplexData).where(ComplexData.id == entry.id)
    )
    fetched = result.scalar_one()
    assert fetched.data_jsonb == curr
    
    await neondb.execute(sa.delete(ComplexData).where(ComplexData.id == entry.id))

@pytest.mark.asyncio
async def test_orm_add_all_large_json(neondb: NeonNativeAsyncEngine):
    """
    Test insertion of multiple large JSON payloads using neondb.add_all().
    This tests batching of large objects, which might trigger size-related errors.
    """
    # Generate 3 objects of ~1MB each
    objs = []
    names = []
    payload_total = 0
    for i in range(3):
        data = generate_complex_json(depth=6, items_per_level=5)
        payload_total += len(json.dumps(data))
        name = f"add_all_test_{i}_{uuid.uuid4().hex[:8]}"
        names.append(name)
        objs.append(ComplexData(name=name, data_jsonb=data, data_json=data))
    
    print(f"\nORM add_all total payload size: {payload_total / 1024:.2f} KB")
    
    # Use ORM add_all()
    await neondb.add_all(objs)
    
    # Verify each was inserted
    for obj in objs:
        assert obj.id is not None
    
    # Fetch and verify one randomly
    target_name = names[1]
    result = await neondb.execute(
        sa.select(ComplexData).where(ComplexData.name == target_name)
    )
    fetched = result.scalar_one()
    assert fetched.name == target_name
    assert fetched.data_jsonb == objs[1].data_jsonb
    
    # Cleanup
    await neondb.execute(sa.delete(ComplexData).where(ComplexData.name.in_(names)))
