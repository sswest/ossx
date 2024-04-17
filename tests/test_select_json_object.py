import json

import oss2
import pytest
from oss2.exceptions import SelectOperationClientError

from tests.common import OSS_PREFIX, bucket


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
                key = f"{OSS_PREFIX}/sample_json.json.gz"
                local_file = "tests/mock_data/sample_json.json.gz"
                is_gzip = True
            else:
                key = f"{OSS_PREFIX}/sample_json.json"
                local_file = "tests/mock_data/sample_json.json"
        else:
            key = f"{OSS_PREFIX}/sample_json_lines.json"
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
        async for chunk in result:
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
            key = f"{OSS_PREFIX}/sample_json.json"
            local_file = "tests/mock_data/sample_json.json"
        else:
            key = f"{OSS_PREFIX}/sample_json_lines.json"
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


@pytest.mark.asyncio
async def test_select_json_lines_democrat_senators(bucket):
    print("test_select_json_lines_democrat_senators")
    helper = SelectJsonObjectTestHelper(bucket)
    input_format = {"Json_Type": "LINES"}
    content = await helper.test_select_json_object(
        "select * from ossobject where party = 'Democrat'", input_format
    )

    content = content[0 : len(content) - 1]  # remove the last ','
    content = b"[" + content + b"]"  # make json parser happy
    result = json.loads(content.decode("utf-8"))

    result2 = []
    with open("tests/mock_data/sample_json.json") as json_file:
        data = json.load(json_file)
        for row in data["objects"]:
            if row["party"] == "Democrat":
                result2.append(row)


@pytest.mark.asyncio
async def test_select_json_object_like(bucket):
    print("test_select_json_object_like")
    helper = SelectJsonObjectTestHelper(bucket)
    select_params = {"Json_Type": "LINES"}
    content = await helper.test_select_json_object(
        "select person.firstname, person.lastname from ossobject where person.birthday like '1959%'",
        select_params,
    )
    content = content[0 : len(content) - 1]  # remove the last ','
    content = b"[" + content + b"]"  # make json parser happy
    result = json.loads(content.decode("utf-8"))

    index = 0
    with open("tests/mock_data/sample_json.json") as json_file:
        data = json.load(json_file)
        for row in data["objects"]:
            select_row = {}
            if row["person"]["birthday"].startswith("1959"):
                select_row["firstname"] = row["person"]["firstname"]
                select_row["lastname"] = row["person"]["lastname"]
                assert result[index] == select_row
                index += 1


@pytest.mark.asyncio
async def test_select_gzip_json_object_like(bucket):
    print("test_select_gzip_json_object_like")
    helper = SelectJsonObjectTestHelper(bucket)
    select_params = {"Json_Type": "DOCUMENT", "CompressionType": "GZIP"}
    content = await helper.test_select_json_object(
        "select person.firstname, person.lastname from ossobject.objects[*] "
        "where person.birthday like '1959%'",
        select_params,
    )
    content = content[0 : len(content) - 1]  # remove the last ','
    content = b"[" + content + b"]"  # make json parser happy
    result = json.loads(content.decode("utf-8"))

    index = 0
    with open("tests/mock_data/sample_json.json") as json_file:
        data = json.load(json_file)
        for row in data["objects"]:
            select_row = {}
            if row["person"]["birthday"].startswith("1959"):
                select_row["firstname"] = row["person"]["firstname"]
                select_row["lastname"] = row["person"]["lastname"]
                assert result[index] == select_row
                index += 1


@pytest.mark.asyncio
async def test_select_json_object_with_output_raw(bucket):
    key = f"{OSS_PREFIX}/test_select_json_object_with_output_raw"
    content = '{"key":"abc"}'
    await bucket.put_object(key, content.encode("utf_8"))
    select_params = {"OutputRawData": "true", "Json_Type": "DOCUMENT"}
    result = await bucket.select_object(key, "select key from ossobject", None, select_params)
    content = b""
    async for chunk in result:
        content += chunk

    assert content == '{"key":"abc"}\n'.encode("utf-8")


@pytest.mark.asyncio
async def test_select_json_object_with_crc(bucket):
    key = f"{OSS_PREFIX}/test_select_json_object_with_crc"
    content = '{"key":"abc"}\n'
    await bucket.put_object(key, content)
    select_params = {"EnablePayloadCrc": True, "Json_Type": "DOCUMENT"}
    result = await bucket.select_object(
        key, "select * from ossobject where true", None, select_params
    )
    content = await result.read()

    assert content == '{"key":"abc"}\n'.encode("utf-8")


@pytest.mark.asyncio
async def test_select_json_object_with_skip_partial_data(bucket):
    key = f"{OSS_PREFIX}/test_select_json_object_with_skip_partial_data"
    content = '{"key":"abc"},{"key2":"def"}'
    await bucket.put_object(key, content.encode("utf_8"))
    select_params = {
        "SkipPartialDataRecord": "false",
        "Json_Type": "LINES",
        "MaxSkippedRecordsAllowed": 100,
    }
    result = await bucket.select_object(key, "select key from ossobject", None, select_params)
    content = b""
    try:
        async for chunk in result:
            content += chunk
    except oss2.exceptions.ServerError:
        print("expected error occurs")

    assert content == '{"key":"abc"}\n{}\n'.encode("utf-8")


@pytest.mark.asyncio
async def test_select_json_object_with_invalid_parameter(bucket):
    key = f"{OSS_PREFIX}/test_select_json_object_with_invalid_parameter"
    content = '{"key":"abc"}\n'
    await bucket.put_object(key, content.encode("utf_8"))
    select_params = {
        "EnablePayloadCrc": True,
        "Json_Type": "DOCUMENT",
        "unsupported_param": True,
    }
    try:
        await bucket.select_object(key, "select * from ossobject where true", None, select_params)
        assert False
    except SelectOperationClientError:
        print("expected error occurs")


@pytest.mark.asyncio
async def test_create_json_meta_with_invalid_parameter(bucket):
    key = f"{OSS_PREFIX}/test_create_json_meta_with_invalid_parameter"
    content = '{"key":"abc"}\n'
    await bucket.put_object(key, content.encode("utf_8"))
    select_params = {"EnablePayloadCrc": True, "Json_Type": "DOCUMENT"}

    try:
        await bucket.create_select_object_meta(key, select_params)
        assert False
    except SelectOperationClientError:
        print("expected error occurs")


@pytest.mark.asyncio
async def test_create_json_object_meta_invalid_request2(bucket):
    key = f"{OSS_PREFIX}/sample_json.json"
    await bucket.put_object_from_file(key, "tests/mock_data/sample_json.json")
    format = {"Json_Type": "invalid", "CompressionType": "None", "OverwriteifExists": "True"}
    try:
        await bucket.create_select_object_meta(key, format)
        assert False
    except SelectOperationClientError:
        print("expected error occured")


@pytest.mark.asyncio
async def test_select_json_object_line_range(bucket):
    print("test_select_json_object_line_range")
    helper = SelectJsonObjectTestHelper(bucket)

    select_params = {"LineRange": (10, 50), "Json_Type": "LINES"}
    content = await helper.test_select_json_object(
        "select person.firstname as aaa as firstname, person.lastname, extra from ossobject'",
        select_params,
    )
    content = content[0 : len(content) - 1]  # remove the last ','
    content = b"[" + content + b"]"  # make json parser happy
    result = json.loads(content.decode("utf-8"))

    result_index = 0
    result2 = []
    with open("tests/mock_data/sample_json.json") as json_file:
        data = json.load(json_file)
        index = 0
        for row in data["objects"]:
            select_row = {}
            if index >= 10 and index < 50:
                select_row["firstname"] = row["person"]["firstname"]
                select_row["lastname"] = row["person"]["lastname"]
                select_row["extra"] = row["extra"]
                assert result[result_index] == select_row
                result_index += 1
            elif index >= 50:
                break
            index += 1


@pytest.mark.asyncio
async def test_select_json_object_int_aggregation(bucket):
    print("test_select_json_object_int_aggregation")
    helper = SelectJsonObjectTestHelper(bucket)
    select_params = {"Json_Type": "DOCUMENT"}
    content = await helper.test_select_json_object(
        "select avg(cast(person.cspanid as int)), max(cast(person.cspanid as int)), min("
        "cast(person.cspanid as int)) from ossobject.objects[*] where person.cspanid = 1011723",
        select_params,
    )
    assert content == b'{"_1":1011723,"_2":1011723,"_3":1011723},'


@pytest.mark.asyncio
async def test_select_json_object_float_aggregation(bucket):
    print("test_select_json_object_float_aggregation")
    helper = SelectJsonObjectTestHelper(bucket)
    select_params = {"Json_Type": "DOCUMENT"}
    content = await helper.test_select_json_object(
        "select avg(cast(person.cspanid as double)), max(cast(person.cspanid as double)), "
        "min(cast(person.cspanid as double)) from ossobject.objects[*]",
        select_params,
    )
    print(content)


@pytest.mark.asyncio
async def test_select_json_object_concat(bucket):
    print("test_select_json_object_concat")
    helper = SelectJsonObjectTestHelper(bucket)
    select_params = {"Json_Type": "DOCUMENT"}
    content = await helper.test_select_json_object(
        "select person from ossobject.objects[*] where (person.firstname || person.lastname)"
        " = 'JohnKennedy'",
        select_params,
    )
    content = content[0 : len(content) - 1]  # remove the last ','
    content = b"[" + content + b"]"  # make json parser happy
    result = json.loads(content.decode("utf-8"))

    result_index = 0
    with open("tests/mock_data/sample_json.json") as json_file:
        data = json.load(json_file)
        for row in data["objects"]:
            if row["person"]["firstname"] == "John" and row["person"]["lastname"] == "Kennedy":
                assert result[result_index]["person"] == row["person"]
                result_index += 1


@pytest.mark.asyncio
async def test_select_json_object_complicate_condition(bucket):
    print("test_select_json_object_complicate_condition")
    helper = SelectJsonObjectTestHelper(bucket)
    select_params = {"Json_Type": "LINES"}
    content = await helper.test_select_json_object(
        "select person.firstname, person.lastname, congress_numbers from ossobject where startdate"
        " > '2017-01-01' and senator_rank = 'junior' or state = 'CA' and party = 'Republican' ",
        select_params,
    )
    content = content[0 : len(content) - 1]  # remove the last ','
    content = b"[" + content + b"]"  # make json parser happy
    result = json.loads(content.decode("utf-8"))

    result_index = 0
    with open("tests/mock_data/sample_json.json") as json_file:
        data = json.load(json_file)
        for row in data["objects"]:
            if (
                row["startdate"] > "2017-01-01"
                and row["senator_rank"] == "junior"
                or row["state"] == "CA"
                and row["party"] == "Republican"
            ):
                assert result[result_index]["firstname"] == row["person"]["firstname"]
                assert result[result_index]["lastname"] == row["person"]["lastname"]
                assert result[result_index]["congress_numbers"] == row["congress_numbers"]
                result_index += 1


@pytest.mark.asyncio
async def test_select_json_object_invalid_sql(bucket):
    print("test_select_json_object_invalid_sql")
    helper = SelectJsonObjectTestHelper(bucket)

    select_params = {"Json_Type": "LINES"}
    await helper.test_select_json_object_invalid_request(
        "select * from ossobject where avg(cast(person.birthday as int)) > 2016",
        select_params,
    )
    await helper.test_select_json_object_invalid_request("", select_params)
    await helper.test_select_json_object_invalid_request(
        "select person.lastname || person.firstname from ossobject", select_params
    )
    await helper.test_select_json_object_invalid_request(
        "select * from ossobject group by person.firstname", select_params
    )
    await helper.test_select_json_object_invalid_request(
        "select * from ossobject order by _1", select_params
    )
    await helper.test_select_json_object_invalid_request(
        "select * from ossobject oss join s3object s3 on oss.CityName = s3.CityName",
        select_params,
    )


@pytest.mark.asyncio
async def test_select_json_object_with_invalid_data(bucket):
    print("test_select_json_object_with_invalid_data")
    key = f"{OSS_PREFIX}/invalid_json.json"
    await bucket.put_object_from_file(key, "tests/mock_data/invalid_sample_data.csv")
    input_format = {"Json_Type": "DOCUMENT"}

    try:
        result = await bucket.select_object(
            key, "select _1 from ossobject.objects[*]", None, input_format
        )
        assert False
    except oss2.exceptions.ServerError:
        print("Got the exception. Ok.")


@pytest.mark.asyncio
async def test_select_json_object_none_range(bucket):
    print("test_select_json_object_none_range")
    key = f"{OSS_PREFIX}/sample_json.json"
    await bucket.put_object_from_file(key, "tests/mock_data/sample_json.json")
    select_params = {"Json_Type": "LINES"}
    await bucket.create_select_object_meta(key, select_params)

    input_formats = [
        {"SplitRange": (None, None)},
        {"LineRange": (None, None)},
        {"SplitRange": None},
        {"LineRange": None},
    ]

    for input_format in input_formats:
        input_format["Json_Type"] = "LINES"
        result = await bucket.select_object(key, "select * from ossobject", None, input_format)
        content = b""
        async for chunk in result:
            content += chunk

        assert len(content) > 0


@pytest.mark.asyncio
async def test_select_json_object_parse_num_as_string(bucket):
    key = f"{OSS_PREFIX}/test_select_json_object_parse_num_as_string"
    await bucket.put_object(key, b'{"a":123456789.123456789}')
    result = await bucket.select_object(
        key,
        sql="select a from ossobject where cast(a as decimal) = 123456789.1234567890",
        select_params={"ParseJsonNumberAsString": "true", "Json_Type": "DOCUMENT"},
    )
    content = b""
    async for chunk in result:
        content += chunk
    assert content == b'{"a":123456789.123456789}\n'
