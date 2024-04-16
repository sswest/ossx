import json
import logging
import os

import oss2
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


class SelectJsonObjectTestHelper(object):
    def __init__(self, bucket):
        self.bucket = bucket
        self.scannedSize = 0

    def select_call_back(self, consumed_bytes, total_bytes=None):
        self.scannedSize = consumed_bytes

    async def test_select_json_object(self, sql, input_format):
        is_gzip = False
        if input_format["Json_Type"] == "DOCUMENT":
            if "CompressionType" in input_format and input_format["CompressionType"] == "GZIP":
                key = "sample_json.json.gz"
                local_file = "tests/mock_data/sample_json.json.gz"
                is_gzip = True
            else:
                key = "sample_json.json"
                local_file = "tests/mock_data/sample_json.json"
        else:
            key = "sample_json_lines.json"
            local_file = "tests/mock_data/sample_json_lines.json"

        result = await self.bucket.put_object_from_file(key, local_file)


        # set OutputrecordDelimiter with ',' to make json load the result correctly
        input_format["OutputRecordDelimiter"] = ","

        if input_format["Json_Type"] == "LINES":
            result = await self.bucket.create_select_object_meta(key, {"Json_Type": "LINES"})

        result = await self.bucket.head_object(key)
        file_size = result.content_length

        result = await self.bucket.select_object(key, sql, self.select_call_back, input_format)
        content = b""
        while True:
            chunk = await result.read()
            if not chunk:
                break
            content += chunk

        assert result.status == 206
        assert len(content) > 0

        if (
            "SplitRange" not in input_format
            and "LineRange" not in input_format
            and is_gzip is False
        ):
            assert self.scannedSize == file_size

        return content

    async def test_select_json_object_invalid_request(self, sql, input_format):
        if input_format["Json_Type"] == "DOCUMENT":
            key = "sample_json.json"
            local_file = "tests/mock_data/sample_json.json"
        else:
            key = "sample_json_lines.json"
            local_file = "tests/mock_data/sample_json_lines.json"

        result = self.bucket.put_object_from_file(key, local_file)
        if input_format["Json_Type"] == "LINES":
            result = await self.bucket.create_select_object_meta(key, input_format)

        try:
            result = await self.bucket.select_object(key, sql, None, input_format)
            assert result.status == 400
        except oss2.exceptions.ServerError as e:
            assert e.status == 400


@pytest.mark.asyncio
async def test_select_json_document_democrat_senators(bucket):
    print("test_select_json_document_democrat_senators")
    helper = SelectJsonObjectTestHelper(bucket)
    input_format = {"Json_Type": "DOCUMENT", "CompressionType": "None"}
    content = await helper.test_select_json_object(
        "select * from ossobject.objects[*] where party = 'Democrat'", input_format
    )

    content = content[0 : len(content) - 1]  # remove the last ','
    content = b"[" + content + b"]"  # make json parser happy
    result = json.loads(content.decode("utf-8"))

    result_index = 0
    with open("tests/mock_data/sample_json.json") as json_file:
        data = json.load(json_file)
        for row in data["objects"]:
            if row["party"] == "Democrat":
                assert result[result_index] == row
                result_index += 1
