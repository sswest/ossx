import asyncio
import hashlib

import oss2
import pytest

from ossx import AsyncService
from ossx.iterators import (
    BucketIterator,
    LiveChannelIterator,
    MultipartUploadIterator,
    ObjectIterator,
    ObjectUploadIterator,
    PartIterator,
)

from .common import (
    OSS_BUCKET_NAME,
    bucket,
    service,
    delete_keys,
    random_bytes,
    random_key,
    random_string,
)


@pytest.mark.asyncio
async def test_bucket_iterator(service):
    assert OSS_BUCKET_NAME in [b.name async for b in BucketIterator(service, max_keys=2)]


@pytest.mark.asyncio
async def test_object_iterator(bucket):
    prefix = random_key()
    object_list = []
    dir_list = []

    fs = []
    # 准备文件
    for i in range(20):
        object_list.append(prefix + random_string(16))
        fs.append(bucket.put_object(object_list[-1], random_bytes(10)))

    # 准备目录
    for i in range(5):
        dir_list.append(prefix + random_string(5) + "/")
        fs.append(bucket.put_object(dir_list[-1] + random_string(5), random_bytes(3)))

    await asyncio.gather(*fs)

    # 验证
    objects_got = []
    dirs_got = []
    async for info in ObjectIterator(bucket, prefix, delimiter="/", max_keys=4):
        if info.is_prefix():
            dirs_got.append(info.key)
        else:
            objects_got.append(info.key)

            result = await bucket.head_object(info.key)
            assert result.last_modified == info.last_modified

    assert sorted(object_list) == objects_got
    assert sorted(dir_list) == dirs_got

    await delete_keys(bucket, object_list)


@pytest.mark.asyncio
async def test_object_iterator_chinese(bucket):
    for prefix in [random_key(suffix="中+文"), random_key(suffix="中+文")]:
        await bucket.put_object(prefix, b"content of object")
        object_got = [_ async for _ in ObjectIterator(bucket, prefix=prefix, max_keys=1)][0].key
        assert oss2.to_string(prefix) == object_got
        await bucket.delete_object(prefix)


@pytest.mark.asyncio
async def test_upload_iterator(bucket):
    prefix = random_key()
    key = prefix + random_string(16)

    upload_list = []
    dir_list = []

    # 准备分片上传
    for i in range(10):
        m = await bucket.init_multipart_upload(key)
        upload_list.append(m.upload_id)

    # 准备碎片目录
    for i in range(4):
        dir_list.append(prefix + random_string(5) + "/")
        await bucket.init_multipart_upload(dir_list[-1] + random_string(5))

    # 验证
    uploads_got = []
    dirs_got = []
    async for u in MultipartUploadIterator(bucket, prefix=prefix, delimiter="/", max_uploads=2):
        if u.is_prefix():
            dirs_got.append(u.key)
        else:
            uploads_got.append(u.upload_id)

    assert sorted(upload_list) == uploads_got
    assert sorted(dir_list) == dirs_got


@pytest.mark.asyncio
async def test_upload_iterator_chinese(bucket):
    upload_list = []

    p = random_key()
    prefix_list = [p + "中文-阿+里-巴*巴", p + "中文-四/十*大%盗"]
    for prefix in prefix_list:
        m = await bucket.init_multipart_upload(prefix)
        upload_list.append(m.upload_id)

    uploads_got = []
    for prefix in prefix_list:
        listed = [_ async for _ in MultipartUploadIterator(bucket, prefix=prefix, max_uploads=1)]
        uploads_got.append(listed[0].upload_id)

    assert sorted(upload_list) == sorted(uploads_got)


@pytest.mark.asyncio
async def test_object_upload_iterator(bucket):
    # target_object是想要列举的文件，而intact_object则不是。
    # 这里intact_object故意以target_object为前缀
    target_object = random_key()
    intact_object = random_key()

    target_list = []
    intact_list = []

    # 准备分片
    for i in range(10):
        target_list.append((await bucket.init_multipart_upload(target_object)).upload_id)
        intact_list.append((await bucket.init_multipart_upload(intact_object)).upload_id)

    # 验证：max_uploads能被分片数整除
    uploads_got = []
    async for u in ObjectUploadIterator(bucket, target_object, max_uploads=5):
        uploads_got.append(u.upload_id)

    assert sorted(target_list) == uploads_got

    # 验证：max_uploads不能被分片数整除
    uploads_got = []
    async for u in ObjectUploadIterator(bucket, target_object, max_uploads=3):
        uploads_got.append(u.upload_id)

    assert sorted(target_list) == uploads_got

    # 清理
    for upload_id in target_list:
        await bucket.abort_multipart_upload(target_object, upload_id)

    for upload_id in intact_list:
        await bucket.abort_multipart_upload(intact_object, upload_id)


@pytest.mark.asyncio
async def test_part_iterator(bucket):
    for key in [random_string(16), "中文+_)(*&^%$#@!前缀", "中文+_)(*&^%$#@!前缀"]:
        upload_id = (await bucket.init_multipart_upload(key)).upload_id

        # 准备分片
        part_list = []
        for part_number in [1, 3, 6, 7, 9, 10]:
            content = random_string(128 * 1024)
            etag = hashlib.md5(oss2.to_bytes(content)).hexdigest().upper()
            part_list.append(oss2.models.PartInfo(part_number, etag, len(content)))

            await bucket.upload_part(key, upload_id, part_number, content)

        # 验证
        parts_got = []
        async for part_info in PartIterator(bucket, key, upload_id):
            parts_got.append(part_info)

        assert len(part_list) == len(parts_got)

        for i in range(len(part_list)):
            assert part_list[i].part_number == parts_got[i].part_number
            assert part_list[i].etag == parts_got[i].etag
            assert part_list[i].size == parts_got[i].size

        await bucket.abort_multipart_upload(key, upload_id)


@pytest.mark.asyncio
async def test_live_channel_iterator(bucket):
    prefix = random_key(prefix="pytest-live-")
    channel_name_list = []

    channel_target = oss2.models.LiveChannelInfoTarget(playlist_name="test.m3u8")
    channel_info = oss2.models.LiveChannelInfo(target=channel_target)
    # 准备频道
    for i in range(20):
        channel_name_list.append(prefix + random_string(16))
        await bucket.create_live_channel(channel_name_list[-1], channel_info)

    # 验证
    live_channel_got = []
    async for info in LiveChannelIterator(bucket, prefix, max_keys=4):
        live_channel_got.append(info.name)

        result = await bucket.get_live_channel(info.name)
        assert result.description == info.description

    assert sorted(channel_name_list) == live_channel_got

    for live_channel in channel_name_list:
        await bucket.delete_live_channel(live_channel)
