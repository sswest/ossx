import os

import pytest
from oss2.headers import OSS_TRAFFIC_LIMIT

from ossx import AsyncBucket
from tests.common import OSS_PREFIX, bucket


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
    assert obj.status == 206
    assert await obj.read() == data[byte_range[0] : byte_range[1] + 1]


@pytest.mark.asyncio
async def test_get_object_to_file(bucket: AsyncBucket):
    key = f"{OSS_PREFIX}/get_object_to_file.txt"
    data = b"hello world" * 1000
    result = await bucket.put_object(key, data)
    assert result.status == 200
    filename = "tests/mock_data/get_object_to_file.txt"
    result = await bucket.get_object_to_file(key, filename)
    assert result.status == 200
    with open(filename, "rb") as f:
        assert f.read() == data
    byte_range = (11, 21)
    filename = "tests/mock_data/get_object_to_file.txt"
    result = await bucket.get_object_to_file(key, filename, byte_range=byte_range)
    assert result.status == 206
    with open(filename, "rb") as f:
        assert f.read() == data[byte_range[0] : byte_range[1] + 1]
    os.remove(filename)


@pytest.mark.asyncio
async def test_get_object_with_url_to_file_chunked(bucket):
    # object后缀为txt, length >= 1024, 指定Accept-Encoding接收，服务器将会以chunked模式传输数据
    key = f"{OSS_PREFIX}/test_get_object_with_url_to_file_chunked.txt"
    content = b"a" * 1024
    await bucket.put_object(key, content)

    filename = f"tests/mock_Data/test_get_object_with_url_to_file_chunked-local.txt"
    url = bucket.sign_url("GET", key, 240)
    result = await bucket.get_object_with_url_to_file(
        url, filename, headers={"Accept-Encoding": "gzip"}
    )

    assert result.headers["Transfer-Encoding"] == "chunked"

    with open(filename, "rb") as f:
        assert f.read() == content

    await bucket.delete_object(key)
    os.remove(filename)


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
