import logging
import os
import random
import string

import oss2
import pytest
from oss2 import Auth, Bucket
from oss2.api import logger

from ossx import AsyncBucket, AsyncService

logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

OSS_ACCESS_KEY_ID = os.getenv("OSS_ACCESS_KEY_ID")
OSS_ACCESS_KEY_SECRET = os.getenv("OSS_ACCESS_KEY_SECRET")
OSS_ENDPOINT = os.getenv("OSS_ENDPOINT")
OSS_BUCKET_NAME = os.getenv("OSS_BUCKET_NAME")
OSS_PREFIX = os.getenv("OSS_PREFIX")
OSS_TEST_UID = os.getenv("OSS_TEST_UID")
auth = Auth(OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET)
sync_bucket = Bucket(auth, OSS_ENDPOINT, OSS_BUCKET_NAME)


TEST_QOS_AND_RESOURCE_POOL = False  # 该功能为邀测功能，未对全部用户开放


@pytest.fixture
def bucket():
    return AsyncBucket(auth, OSS_ENDPOINT, OSS_BUCKET_NAME)


@pytest.fixture
def service():
    return AsyncService(auth, OSS_ENDPOINT)


def random_string(n):
    return "".join(random.choice(string.ascii_lowercase) for i in range(n))


def random_key(prefix=f"{OSS_PREFIX}/", suffix=""):
    key = prefix + random_string(12) + suffix

    return key


def random_bytes(n):
    return oss2.to_bytes(random_string(n))


async def delete_keys(bucket, key_list):
    if not key_list:
        return

    n = 100
    grouped = [key_list[i : i + n] for i in range(0, len(key_list), n)]
    for g in grouped:
        await bucket.batch_delete_objects(g)
