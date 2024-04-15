import logging
import os
import random
import string

import pytest
from oss2 import Auth
from oss2.api import logger

from ossx import AsyncBucket

logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())


OSS_ACCESS_KEY_ID = os.getenv("OSS_ACCESS_KEY_ID")
OSS_ACCESS_KEY_SECRET = os.getenv("OSS_ACCESS_KEY_SECRET")
OSS_ENDPOINT = os.getenv("OSS_ENDPOINT")
OSS_BUCKET_NAME = os.getenv("OSS_BUCKET_NAME")
OSS_PREFIX = os.getenv("OSS_PREFIX")
auth = Auth(OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET)


@pytest.mark.asyncio
async def test_create_delete_bucket():
    bucket_name = f"{OSS_BUCKET_NAME}-{''.join(random.choices(string.ascii_lowercase, k=6))}"
    bucket = AsyncBucket(auth, OSS_ENDPOINT, bucket_name)
    result = await bucket.create_bucket()
    assert result.status == 200
    result = await bucket.delete_bucket()
    assert result.status == 204
