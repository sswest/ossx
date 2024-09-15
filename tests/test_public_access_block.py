import asyncio

import pytest
import oss2

from tests.common import bucket, service


@pytest.mark.skipif(oss2.__version__ < "2.19.0", reason="oss2 version is too low")
@pytest.mark.asyncio
async def test_public_access_block_normal(service):
    # case 1
    result = await service.put_public_access_block(True)
    assert result.status == 200

    # Sleep for a period of time and wait for status updates
    await asyncio.sleep(3)

    result = await service.get_public_access_block()
    assert result.status == 200
    assert result.block_public_access is True

    result = await service.delete_public_access_block()
    assert result.status == 204
    # Sleep for a period of time and wait for status updates
    await asyncio.sleep(3)

    result = await service.get_public_access_block()
    assert result.status == 200
    assert result.block_public_access is False

    # case 2
    result = await service.put_public_access_block()
    assert result.status == 200

    # Sleep for a period of time and wait for status updates
    await asyncio.sleep(3)

    result = await service.get_public_access_block()
    assert result.status == 200
    assert result.block_public_access is False


@pytest.mark.skipif(oss2.__version__ < "2.19.0", reason="oss2 version is too low")
@pytest.mark.asyncio
async def test_bucket_public_access_block_normal(bucket):
    # case 1
    result = await bucket.put_bucket_public_access_block(True)
    assert result.status == 200

    # Sleep for a period of time and wait for status updates
    await asyncio.sleep(3)

    result = await  bucket.get_bucket_public_access_block()
    assert result.status == 200
    assert result.block_public_access is True

    result = await bucket.delete_bucket_public_access_block()
    assert result.status == 204

    # Sleep for a period of time and wait for status updates
    await asyncio.sleep(3)

    result = await bucket.get_bucket_public_access_block()
    assert result.status == 200
    assert result.block_public_access is False

    # case 2
    result = await bucket.put_bucket_public_access_block()
    assert result.status == 200

    # Sleep for a period of time and wait for status updates
    await asyncio.sleep(3)

    result = await bucket.get_bucket_public_access_block()
    assert result.status == 200
    assert result.block_public_access is False


@pytest.mark.skipif(oss2.__version__ < "2.19.0", reason="oss2 version is too low")
@pytest.mark.asyncio
async def test_access_point_public_access_block_normal(bucket):
    oss_access_point_name = "oss-access-point-name-test"

    try:
        await bucket.put_access_point_public_access_block(oss_access_point_name, True)
    except oss2.exceptions.ServerError as e:
        assert e.status == 404
        assert e.message == "Accesspoint not found"

    try:
        await bucket.get_access_point_public_access_block(oss_access_point_name)
    except oss2.exceptions.ServerError as e:
        assert e.status == 404
        assert e.message == "Accesspoint not found"

    try:
        await bucket.delete_access_point_public_access_block(oss_access_point_name)
    except oss2.exceptions.ServerError as e:
        assert e.status == 404
        assert e.message == "Accesspoint not found"
