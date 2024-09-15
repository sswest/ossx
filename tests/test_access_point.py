import asyncio

import pytest
import oss2

from ossx.models import CreateAccessPointRequest
from tests.common import bucket, service


@pytest.mark.skipif(oss2.__version__ < "2.19.0", reason="oss2 version is too low")
@pytest.mark.asyncio
async def test_access_point(service, bucket):
    accessPointName = "test-ap-py-5"
    try:
        access_point = CreateAccessPointRequest(accessPointName, "internet")
        # access_point = CreateAccessPointRequest(accessPointName, 'vpc', vpc)
        result = await bucket.create_access_point(access_point)
        assert result.status == 200
        print("create_access_point")

        get_result = await bucket.get_access_point(accessPointName)
        assert get_result.status == 200
        assert get_result.access_point_name == accessPointName
        assert get_result.bucket == bucket.bucket_name
        assert get_result.account_id is not None
        assert get_result.network_origin == "internet"
        assert get_result.access_point_arn == result.access_point_arn
        assert get_result.creation_date is not None
        assert get_result.alias == result.alias
        assert get_result.access_point_status is not None
        assert get_result.endpoints.public_endpoint is not None
        assert get_result.endpoints.internal_endpoint is not None
        print("get_access_point")

        list_result = await bucket.list_bucket_access_points(max_keys=10, continuation_token="")
        assert list_result.max_keys == 10
        assert list_result.is_truncated is not None
        assert list_result.access_points[0].access_point_name == accessPointName
        assert list_result.access_points[0].bucket == bucket.bucket_name
        assert list_result.access_points[0].network_origin is not None
        assert list_result.access_points[0].alias == result.alias
        assert list_result.access_points[0].status is not None
        print("list_bucket_access_points")

        list_result2 = await service.list_access_points(max_keys=10, continuation_token="")

        assert list_result2.max_keys == 10
        assert list_result2.is_truncated is not None
        assert list_result2.access_points[0].access_point_name == accessPointName
        assert list_result2.access_points[0].bucket == bucket.bucket_name
        assert list_result2.access_points[0].network_origin is not None
        assert list_result2.access_points[0].alias == result.alias
        assert list_result2.access_points[0].status is not None
        print("list_access_points")

    except Exception as e:
        print("Exception: {0}".format(e))
    finally:
        num = 1
        while True:
            count = 0
            list_result = await service.list_access_points(max_keys=100)

            if num > 180:
                break

            for ap in list_result.access_points:
                if ap.access_point_name.startswith("test-ap-py-"):
                    count += 1
                    if ap.status == "enable":
                        print("status: " + ap.status)
                        del_result = await bucket.delete_access_point(ap.access_point_name)
                        assert del_result.status == 204
                        print("delete_access_point")

            num += 1
            if count == 0:
                break
            await asyncio.sleep(3)
