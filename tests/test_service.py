import logging
import os

import pytest
from oss2 import Auth
from oss2.api import logger
from oss2.exceptions import AccessDenied
from oss2.models import DescribeRegionsResult, GetUserQosInfoResult, ListBucketsResult

from ossx import AsyncService

logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())


OSS_ACCESS_KEY_ID = os.getenv("OSS_ACCESS_KEY_ID")
OSS_ACCESS_KEY_SECRET = os.getenv("OSS_ACCESS_KEY_SECRET")
OSS_ENDPOINT = os.getenv("OSS_ENDPOINT")
OSS_BUCKET_NAME = os.getenv("OSS_BUCKET_NAME")
OSS_PREFIX = os.getenv("OSS_PREFIX")
auth = Auth(OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET)


@pytest.mark.asyncio
async def test_list_buckets():
    service = AsyncService(auth, OSS_ENDPOINT)
    result = await service.list_buckets()
    assert isinstance(result, ListBucketsResult)
    assert result.status == 200
    if OSS_BUCKET_NAME:
        assert OSS_BUCKET_NAME in [bucket.name for bucket in result.buckets]


@pytest.mark.asyncio
async def test_get_user_qos_info():
    service = AsyncService(auth, OSS_ENDPOINT)
    with pytest.raises(AccessDenied):
        result = await service.get_user_qos_info()
        assert isinstance(result, GetUserQosInfoResult)


@pytest.mark.asyncio
async def test_describe_regions():
    service = AsyncService(auth, OSS_ENDPOINT)
    result = await service.describe_regions()
    assert isinstance(result, DescribeRegionsResult)
    assert result.status == 200
