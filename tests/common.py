import logging
import os

import pytest
from oss2 import Auth, Bucket
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
sync_bucket = Bucket(auth, OSS_ENDPOINT, OSS_BUCKET_NAME)


@pytest.fixture
def bucket():
    return AsyncBucket(auth, OSS_ENDPOINT, OSS_BUCKET_NAME)
