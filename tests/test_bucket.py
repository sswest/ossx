import asyncio
import random
import string

import pytest
import oss2

from ossx.models import BucketTlsVersion
from ossx import AsyncBucket
from tests.common import OSS_BUCKET_NAME, OSS_ENDPOINT, auth, bucket, service


@pytest.mark.asyncio
async def test_create_delete_bucket():
    bucket_name = f"{OSS_BUCKET_NAME}-{''.join(random.choices(string.ascii_lowercase, k=6))}"
    bucket = AsyncBucket(auth, OSS_ENDPOINT, bucket_name)
    result = await bucket.create_bucket()
    assert result.status == 200
    result = await bucket.delete_bucket()
    assert result.status == 204


@pytest.mark.skipif(oss2.__version__ < "2.18.5", reason="oss2 version is too low")
@pytest.mark.asyncio
async def test_bucket_archive_direct_read(bucket):
    result = await bucket.put_bucket_archive_direct_read(True)
    assert result.status == 200

    get_result = await bucket.get_bucket_archive_direct_read()
    assert get_result.enabled

    result2 = await bucket.put_bucket_archive_direct_read()
    assert result2.status == 200

    get_result2 = await bucket.get_bucket_archive_direct_read()
    assert get_result2.enabled is False

    try:
        await bucket.put_bucket_archive_direct_read("aa")
    except oss2.exceptions.ServerError as e:
        assert e.details["Code"] == "MalformedXML"


@pytest.mark.skipif(oss2.__version__ < "2.18.5", reason="oss2 version is too low")
@pytest.mark.asyncio
async def test_https_config_normal(bucket):
    https_config = BucketTlsVersion(True, ["TLSv1.2", "TLSv1.3"])
    result = await bucket.put_bucket_https_config(https_config)
    assert result.status == 200

    result2 = await bucket.get_bucket_https_config()
    assert result2.status == 200
    assert result2.tls_enabled
    assert result2.tls_version == ["TLSv1.2", "TLSv1.3"]

    https_config2 = BucketTlsVersion()
    result3 = await bucket.put_bucket_https_config(https_config2)
    assert result3.status == 200

    result4 = await bucket.get_bucket_https_config()
    assert result4.status == 200
    assert result4.tls_enabled is False
    assert result4.tls_version == []


@pytest.mark.skipif(oss2.__version__ < "2.18.5", reason="oss2 version is too low")
@pytest.mark.asyncio
async def test_https_config_exception_1(bucket):
    try:
        https_config = BucketTlsVersion(True)
        await bucket.put_bucket_https_config(https_config)
        assert False
    except oss2.exceptions.ServerError as e:
        assert e.code == "MalformedXML"


@pytest.mark.skipif(oss2.__version__ < "2.18.5", reason="oss2 version is too low")
@pytest.mark.asyncio
async def test_https_config_exception_2(bucket):
    try:
        https_config = BucketTlsVersion(True, ["aaa", "bbb"])
        await bucket.put_bucket_https_config(https_config)
        assert False
    except oss2.exceptions.ServerError as e:
        assert e.code == "MalformedXML"


@pytest.mark.skipif(oss2.__version__ < "2.18.5", reason="oss2 version is too low")
@pytest.mark.asyncio
async def test_https_config_exception_3(bucket):
    https_config = BucketTlsVersion(True, ["TLSv1.2", "TLSv1.2"])
    result = await bucket.put_bucket_https_config(https_config)
    assert result.status == 200

    result2 = await bucket.get_bucket_https_config()
    assert result2.status == 200
    assert result2.tls_enabled
    assert result2.tls_version == ["TLSv1.2"]


@pytest.mark.skipif(oss2.__version__ < "2.18.5", reason="oss2 version is too low")
@pytest.mark.asyncio
async def test_bucket_data_redundancy_transition_normal(service, bucket):
    try:
        result = await bucket.create_bucket_data_redundancy_transition("ZRS")
    except oss2.exceptions.ServerError as e:
        assert e.code == "BucketDataRedundancyTransitionTaskNotSupport"
        return
    assert result.status == 200

    get_result = await bucket.get_bucket_data_redundancy_transition(result.task_id)
    assert get_result.status == 200
    assert get_result.task_id is not None
    assert get_result.transition_status is not None

    list_user_result = await service.list_user_data_redundancy_transition(
        continuation_token="", max_keys=10
    )
    assert list_user_result.status == 200
    model = list_user_result.data_redundancy_transitions[0]
    assert model.bucket == OSS_BUCKET_NAME
    assert model.task_id == get_result.task_id
    assert model.create_time == get_result.create_time
    assert model.start_time == get_result.start_time
    assert model.end_time == get_result.end_time
    assert model.transition_status == get_result.transition_status
    assert model.estimated_remaining_time == get_result.estimated_remaining_time
    assert model.process_percentage == get_result.process_percentage

    list_bucket_result = await bucket.list_bucket_data_redundancy_transition()
    assert list_bucket_result.status == 200
    model = list_bucket_result.data_redundancy_transitions[0]
    assert model.bucket == OSS_BUCKET_NAME
    assert model.task_id == get_result.task_id
    assert model.create_time == get_result.create_time
    assert model.start_time == get_result.start_time
    assert model.end_time == get_result.end_time
    assert model.transition_status == get_result.transition_status
    assert model.estimated_remaining_time == get_result.estimated_remaining_time
    assert model.process_percentage == get_result.process_percentage

    del_result = await bucket.delete_bucket_data_redundancy_transition(result.task_id)
    assert del_result.status == 204


@pytest.mark.skipif(oss2.__version__ < "2.19.0", reason="oss2 version is too low")
@pytest.mark.asyncio
async def test_bucket_stat_param_deep_cold_archive(service):
    bucket_name = OSS_BUCKET_NAME + "-test-stat-deep-cold"
    bucket = AsyncBucket(auth, OSS_ENDPOINT, bucket_name)
    await bucket.create_bucket(oss2.BUCKET_ACL_PRIVATE)

    await asyncio.sleep(1)
    list_buckets = await service.list_buckets(prefix=bucket.bucket_name)
    for b in list_buckets.buckets:
        assert bucket.bucket_name in b.name

    key = "b.txt"
    await bucket.put_object(key, "content")
    await asyncio.sleep(1)

    result = await bucket.get_bucket_stat()
    assert result.object_count == 1
    assert result.multi_part_upload_count == 0
    assert result.storage_size_in_bytes == 7
    assert result.live_channel_count == 0
    assert result.last_modified_time is not None
    assert result.standard_storage == 7
    assert result.standard_object_count == 1
    assert result.infrequent_access_storage == 0
    assert result.infrequent_access_real_storage == 0
    assert result.infrequent_access_object_count == 0
    assert result.archive_storage == 0
    assert result.archive_real_storage == 0
    assert result.archive_object_count == 0
    assert result.cold_archive_storage == 0
    assert result.cold_archive_real_storage == 0
    assert result.cold_archive_object_count == 0
    assert result.deep_cold_archive_storage == 0
    assert result.deep_cold_archive_real_storage == 0
    assert result.deep_cold_archive_object_count == 0

    await bucket.delete_object(key)
    await bucket.delete_bucket()

    await asyncio.sleep(1)
    with pytest.raises(oss2.exceptions.NoSuchBucket):
        await bucket.delete_bucket()
