import oss2
import pytest

from ossx.models import QoSConfiguration
from tests.common import bucket, service, OSS_TEST_UID, TEST_QOS_AND_RESOURCE_POOL


@pytest.mark.skipif(oss2.__version__ < "2.19.0" or not TEST_QOS_AND_RESOURCE_POOL, reason="oss2 version is too low")
@pytest.mark.asyncio
async def test_qos_and_resource_pool(service, bucket):
    uid = OSS_TEST_UID
    resource_pool_name = ""

    # put bucket requester qos info
    qos_info = QoSConfiguration(
        total_upload_bw=100,
        intranet_upload_bw=6,
        extranet_upload_bw=12,
        total_download_bw=110,
        intranet_download_bw=20,
        extranet_download_bw=50,
        total_qps=300,
        intranet_qps=160,
        extranet_qps=170,
    )

    result = await bucket.put_bucket_requester_qos_info(uid, qos_info)
    assert result.status == 200

    # get bucket requester qos info
    result = await bucket.get_bucket_requester_qos_info(uid)
    assert result.requester == uid
    assert result.qos_configuration.total_upload_bw == 100
    assert result.qos_configuration.intranet_upload_bw == 6
    assert result.qos_configuration.extranet_upload_bw == 12
    assert result.qos_configuration.total_download_bw == 110
    assert result.qos_configuration.intranet_download_bw == 20
    assert result.qos_configuration.extranet_download_bw == 50
    assert result.qos_configuration.total_qps == 300
    assert result.qos_configuration.intranet_qps == 160
    assert result.qos_configuration.extranet_qps == 170

    # list bucket requester qos infos
    result = await bucket.list_bucket_requester_qos_infos()
    assert result.status == 200
    assert result.continuation_token == ""
    assert result.next_continuation_token == ""
    assert result.is_truncated == False
    assert result.requester_qos_info[0].qos_configuration.total_upload_bw == 100
    assert result.requester_qos_info[0].qos_configuration.intranet_upload_bw == 6
    assert result.requester_qos_info[0].qos_configuration.extranet_upload_bw == 12
    assert result.requester_qos_info[0].qos_configuration.total_download_bw == 110
    assert result.requester_qos_info[0].qos_configuration.intranet_download_bw == 20
    assert result.requester_qos_info[0].qos_configuration.extranet_download_bw == 50
    assert result.requester_qos_info[0].qos_configuration.total_qps == 300
    assert result.requester_qos_info[0].qos_configuration.intranet_qps == 160
    assert result.requester_qos_info[0].qos_configuration.extranet_qps == 170

    # delete bucket requester qos info
    result = await bucket.delete_bucket_requester_qos_info(uid)
    assert result.status == 204

    # list resource pools
    result = await service.list_resource_pools()
    assert result.status == 200
    assert result.region == OSS_REGION
    assert result.owner == uid
    assert result.continuation_token == ""
    assert result.next_continuation_token == ""
    assert result.is_truncated == False
    assert result.resource_pool[0].name is not None
    assert result.resource_pool[0].create_time is not None

    # get resource pool info
    try:
        await service.get_resource_pool_info(resource_pool_name)
    except oss2.exceptions.ServerError as e:
        assert e.status == 400
        assert e.message == "Resource pool name is empty"

    # list resource pool buckets
    try:
        await service.list_resource_pool_buckets(resource_pool_name)
    except oss2.exceptions.ServerError as e:
        assert e.status == 400
        assert e.message == "Resource pool name is empty"

    # put resource pool requester qos info
    qos_info = QoSConfiguration(
        total_upload_bw=200,
        intranet_upload_bw=16,
        extranet_upload_bw=112,
        total_download_bw=210,
        intranet_download_bw=120,
        extranet_download_bw=150,
        total_qps=400,
        intranet_qps=260,
        extranet_qps=270,
    )
    try:
        await service.put_resource_pool_requester_qos_info(uid, resource_pool_name, qos_info)
    except oss2.exceptions.ServerError as e:
        assert e.status == 400
        assert e.message == "Resource pool name is empty"

    # get resource pool requester qos info
    try:
        await service.get_resource_pool_requester_qos_info(uid, resource_pool_name)
    except oss2.exceptions.ServerError as e:
        assert e.status == 400
        assert e.message == "Resource pool name is empty"

    # list resource pool requester qos infos
    try:
        await service.list_resource_pool_requester_qos_infos(resource_pool_name)
    except oss2.exceptions.ServerError as e:
        assert e.status == 400
        assert e.message == "Resource pool name is empty"

    # delete resource pool requester qos infos
    try:
        await service.delete_resource_pool_requester_qos_info(uid, resource_pool_name)
    except oss2.exceptions.ServerError as e:
        assert e.status == 400
        assert e.message == "Resource pool name is empty"
