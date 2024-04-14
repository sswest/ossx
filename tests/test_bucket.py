import logging
import os

import pytest

from ossx.bucket import AsyncBucket
from oss2 import Auth
from oss2.api import logger

logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())


OSS_ACCESS_KEY_ID = os.getenv("OSS_ACCESS_KEY_ID")
OSS_ACCESS_KEY_SECRET = os.getenv("OSS_ACCESS_KEY_SECRET")
OSS_ENDPOINT = os.getenv("OSS_ENDPOINT")
OSS_BUCKET_NAME = os.getenv("OSS_BUCKET_NAME")
OSS_PREFIX = os.getenv("OSS_PREFIX")
auth = Auth(OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET)


@pytest.fixture
def bucket():
    return AsyncBucket(auth, OSS_ENDPOINT, OSS_BUCKET_NAME)


@pytest.mark.asyncio
async def test_list_objects(bucket: AsyncBucket):
    max_keys = 10
    objs = await bucket.list_objects(prefix=OSS_PREFIX, max_keys=max_keys)
    count = 0
    for obj in objs.object_list:
        assert obj.key.startswith(OSS_PREFIX)
        count += 1
    assert count <= max_keys


@pytest.mark.asyncio
async def test_list_objects_v2(bucket: AsyncBucket):
    max_keys = 10
    objs = await bucket.list_objects_v2(prefix=OSS_PREFIX, max_keys=max_keys)
    count = 0
    for obj in objs.object_list:
        assert obj.key.startswith(OSS_PREFIX)
        count += 1
    assert count <= max_keys


@pytest.mark.asyncio
async def test_put_object(bucket: AsyncBucket):
    key = f"{OSS_PREFIX}/test.txt"
    data = b"hello world"
    result = await bucket.put_object(key, data)
    assert result.status == 200
    obj = await bucket.get_object(key)
    assert await obj.read() == data


@pytest.mark.asyncio
async def test_put_object_from_file(bucket: AsyncBucket):
    key = f"{OSS_PREFIX}/example.jpg"
    filename = "tests/example.jpg"
    result = await bucket.put_object_from_file(key, filename)
    assert result.status == 200
    obj = await bucket.get_object(key)
    with open(filename, "rb") as f:
        assert await obj.read() == f.read()


@pytest.mark.asyncio
async def test_head_object(bucket: AsyncBucket):
    key = f"{OSS_PREFIX}/test.txt"
    data = b"hello world"
    result = await bucket.put_object(key, data)
    assert result.status == 200
    obj = await bucket.head_object(key)
    assert obj.content_length == len(data)


@pytest.mark.asyncio
async def test_get_object_meta(bucket: AsyncBucket):
    key = f"{OSS_PREFIX}/test.txt"
    data = b"hello world"
    result = await bucket.put_object(key, data)
    assert result.status == 200
    obj = await bucket.get_object_meta(key)
    assert obj.content_length == len(data)


@pytest.mark.asyncio
async def test_object_exists(bucket: AsyncBucket):
    key = f"{OSS_PREFIX}/test.txt"
    data = b"hello world"
    result = await bucket.put_object(key, data)
    assert result.status == 200
    assert await bucket.object_exists(key)
    assert not await bucket.object_exists(f"{OSS_PREFIX}/not-exists.txt")


@pytest.mark.asyncio
async def test_copy_object(bucket: AsyncBucket):
    source_key = f"{OSS_PREFIX}/test.txt"
    target_key = f"{OSS_PREFIX}/test-copy.txt"
    data = b"hello world"
    result = await bucket.put_object(source_key, data)
    assert result.status == 200
    result = await bucket.copy_object(bucket.bucket_name, source_key, target_key)
    assert result.status == 200
    obj = await bucket.get_object(target_key)
    assert await obj.read() == data


@pytest.mark.asyncio
async def test_update_object_meta(bucket: AsyncBucket):
    key = f"{OSS_PREFIX}/test.txt"
    data = b"hello world"
    result = await bucket.put_object(key, data)
    assert result.status == 200
    headers = {"x-oss-meta-pytest": "yes"}
    result = await bucket.update_object_meta(key, headers)
    assert result.status == 200
    obj = await bucket.head_object(key)
    assert obj.headers["x-oss-meta-pytest"] == "yes"


@pytest.mark.asyncio
async def test_delete_object(bucket: AsyncBucket):
    key = f"{OSS_PREFIX}/test.txt"
    data = b"hello world"
    result = await bucket.put_object(key, data)
    assert result.status == 200
    assert await bucket.object_exists(key)
    result = await bucket.delete_object(key)
    assert result.status == 204
    assert not await bucket.object_exists(key)
