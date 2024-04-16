import logging
import os

import pytest
from oss2 import Auth
from oss2.api import logger
from oss2.headers import OSS_TRAFFIC_LIMIT

from ossx import AsyncBucket

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
    filename = "tests/mock_data/example.jpg"
    result = await bucket.put_object_from_file(key, filename)
    assert result.status == 200
    obj = await bucket.get_object(key)
    with open(filename, "rb") as f:
        data = f.read()
        assert await obj.read() == data
    result = await bucket.head_object(key)
    assert result.content_length == os.path.getsize(filename)


@pytest.mark.asyncio
async def test_put_object_with_url(bucket: AsyncBucket):
    OBJECT_SIZE_200KB = 200 * 1024
    content = b"a" * OBJECT_SIZE_200KB

    key = f"{OSS_PREFIX}/traffic-limit-test-put-object-200KB"

    params = dict()
    url = bucket.sign_url("PUT", key, 60, params=params)

    result = await bucket.put_object_with_url(url, content)
    assert result.status == 200

    result = await bucket.head_object(key)
    assert result.content_length == OBJECT_SIZE_200KB


@pytest.mark.asyncio
async def test_put_object_with_url_from_file(bucket: AsyncBucket):
    LIMIT_100KB = 100 * 1024 * 8

    key = f"{OSS_PREFIX}/traffic-limit-test-put-object"
    local_file_name = "tests/mock_data/example.jpg"

    params = dict()
    params[OSS_TRAFFIC_LIMIT] = str(LIMIT_100KB)
    url = bucket.sign_url("PUT", key, 60, params=params)

    result = await bucket.put_object_with_url_from_file(url, local_file_name)
    assert result.status == 200

    result = await bucket.head_object(key)
    assert result.content_length == os.path.getsize(local_file_name)


@pytest.mark.asyncio
async def test_append_object(bucket: AsyncBucket):
    key = f"{OSS_PREFIX}/append_object.txt"
    if await bucket.object_exists(key):
        await bucket.delete_object(key)
    data = b"hello"
    result = await bucket.append_object(key, 0, data)
    assert result.status == 200
    obj = await bucket.get_object(key)
    assert await obj.read() == data
    position = result.next_position
    text = " ossx!"
    result = await bucket.append_object(key, position, text)
    assert result.status == 200
    obj = await bucket.get_object(key)
    assert await obj.read() == b"hello ossx!"


@pytest.mark.asyncio
async def test_get_object(bucket: AsyncBucket):
    key = f"{OSS_PREFIX}/get_object.txt"
    data = b"hello world" * 1000
    result = await bucket.put_object(key, data)
    assert result.status == 200
    obj = await bucket.get_object(key)
    assert await obj.read() == data
    byte_range = (11, 21)
    obj = await bucket.get_object(key, byte_range=byte_range)
    assert await obj.read() == data[byte_range[0] : byte_range[1] + 1]


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
