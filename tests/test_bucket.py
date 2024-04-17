import random
import string

import pytest

from ossx import AsyncBucket
from tests.common import OSS_BUCKET_NAME, OSS_ENDPOINT, auth


@pytest.mark.asyncio
async def test_create_delete_bucket():
    bucket_name = f"{OSS_BUCKET_NAME}-{''.join(random.choices(string.ascii_lowercase, k=6))}"
    bucket = AsyncBucket(auth, OSS_ENDPOINT, bucket_name)
    result = await bucket.create_bucket()
    assert result.status == 200
    result = await bucket.delete_bucket()
    assert result.status == 204
