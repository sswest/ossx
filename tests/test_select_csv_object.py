import calendar
import csv
import re
import time

from oss2.exceptions import (
    ClientError,
    SelectOperationClientError,
    SelectOperationFailed,
)
from oss2.select_response import SelectResponseAdapter

from .common import *


def now():
    return int(calendar.timegm(time.gmtime()))


class SelectCsvObjectTestHelper(object):
    def __init__(self, bucket: AsyncBucket):
        self.bucket = bucket
        self.scannedSize = 0

    def select_call_back(self, consumed_bytes, total_bytes=None):
        self.scannedSize = consumed_bytes

    async def test_select_csv_object(self, sql, line_range=None):
        key = "city_sample_data.csv"
        result = await self.bucket.put_object_from_file(key, "tests/mock_data/sample_data.csv")
        result = await self.bucket.create_select_object_meta(key)
        result = await self.bucket.head_object(key)
        file_size = result.content_length
        input_format = {"CsvHeaderInfo": "Use"}
        if line_range is not None:
            input_format["LineRange"] = line_range

        SelectResponseAdapter._FRAMES_FOR_PROGRESS_UPDATE = 0
        result = await self.bucket.select_object(key, sql, self.select_call_back, input_format)
        content = b""
        async for chunk in result:
            content += chunk

        print(result.request_id)
        assert result.status == 206
        assert len(content) > 0

        if line_range is None:
            assert self.scannedSize == file_size

        return content

    async def test_select_csv_object_invalid_request(self, sql, line_range=None):
        key = "city_sample_data.csv"
        await self.bucket.put_object_from_file(key, "tests/mock_data/sample_data.csv")

        result = await self.bucket.create_select_object_meta(key)
        file_size = result.content_length

        input_format = {"CsvHeaderInfo": "Use"}
        if line_range is not None:
            input_format["Range"] = line_range

        try:
            result = await self.bucket.select_object(key, sql, None, input_format)
            assert result.status == 400
        except oss2.exceptions.ServerError as e:
            assert e.status == 400


@pytest.mark.asyncio
async def test_select_csv_object_not_empty_city(bucket):
    helper = SelectCsvObjectTestHelper(bucket)
    content = await helper.test_select_csv_object(
        "select Year, StateAbbr, CityName, PopulationCount from ossobject where CityName != ''",
    )

    with open("tests/mock_data/sample_data.csv") as csvfile:
        spamreader = csv.DictReader(csvfile, delimiter=",", quotechar='"')
        select_data = b""
        for row in spamreader:
            line = b""
            if row["CityName"] != "":
                line += row["Year"].encode("utf-8")
                line += ",".encode("utf-8")
                line += row["StateAbbr"].encode("utf-8")
                line += ",".encode("utf-8")
                line += row["CityName"].encode("utf-8")
                line += ",".encode("utf-8")
                line += row["PopulationCount"].encode("utf-8")
                line += "\n".encode("utf-8")
                select_data += line

        assert select_data == content


@pytest.mark.asyncio
async def test_select_csv_object_like(bucket):
    helper = SelectCsvObjectTestHelper(bucket)
    content = await helper.test_select_csv_object(
        "select Year, StateAbbr, CityName, Short_Question_Text from ossobject where "
        "Measure like '%blood pressure%Years'",
    )

    with open("tests/mock_data/sample_data.csv") as csvfile:
        spamreader = csv.DictReader(csvfile, delimiter=",", quotechar='"')
        select_data = b""
        matcher = re.compile("^.*blood pressure.*Years$")

        for row in spamreader:
            line = b""
            if matcher.match(row["Measure"]):
                line += row["Year"].encode("utf-8")
                line += ",".encode("utf-8")
                line += row["StateAbbr"].encode("utf-8")
                line += ",".encode("utf-8")
                line += row["CityName"].encode("utf-8")
                line += ",".encode("utf-8")
                line += row["Short_Question_Text"].encode("utf-8")
                line += "\n".encode("utf-8")
                select_data += line

        assert select_data == content


@pytest.mark.asyncio
async def test_select_csv_object_line_range(bucket):
    helper = SelectCsvObjectTestHelper(bucket)
    content = await helper.test_select_csv_object(
        "select Year,StateAbbr, CityName, Short_Question_Text from ossobject'", (0, 50)
    )

    with open("tests/mock_data/sample_data.csv") as csvfile:
        spamreader = csv.DictReader(csvfile, delimiter=",", quotechar='"')
        select_data = b""
        count = 0
        for row in spamreader:
            if count < 50:
                line = b""
                line += row["Year"].encode("utf-8")
                line += ",".encode("utf-8")
                line += row["StateAbbr"].encode("utf-8")
                line += ",".encode("utf-8")
                line += row["CityName"].encode("utf-8")
                line += ",".encode("utf-8")
                line += row["Short_Question_Text"].encode("utf-8")
                line += "\n".encode("utf-8")
                select_data += line
            else:
                break
            count += 1

        assert select_data == content


@pytest.mark.asyncio
async def test_select_csv_object_int_aggregation(bucket):
    helper = SelectCsvObjectTestHelper(bucket)
    content = await helper.test_select_csv_object(
        "select avg(cast(year as int)), max(cast(year as int)), min(cast(year as int)) "
        "from ossobject where year = 2015",
    )
    assert content == b"2015,2015,2015\n"


@pytest.mark.asyncio
async def test_select_csv_object_float_aggregation(bucket):
    helper = SelectCsvObjectTestHelper(bucket)
    content = await helper.test_select_csv_object(
        "select avg(cast(data_value as double)), max(cast(data_value as double)), "
        "sum(cast(data_value as double)) from ossobject",
    )
    # select_data = b''

    with open("tests/mock_data/sample_data.csv") as csvfile:
        spamreader = csv.DictReader(csvfile, delimiter=",", quotechar='"')
        sum = 0.0
        avg = 0.0
        line_count = 0
        max = 0.0
        for row in spamreader:
            if len(row["Data_Value"]) > 0:
                val = float(row["Data_Value"])
                if val > max:
                    max = val

                sum += val
                line_count += 1

        avg = sum / line_count
        aggre_results = content.split(b",")
        avg_result = float(aggre_results[0])
        max_result = float(aggre_results[1])
        sum_result = float(aggre_results[2])
        assert avg == avg_result
        assert max == max_result
        assert sum == sum_result


@pytest.mark.asyncio
async def test_select_csv_object_concat(bucket):
    helper = SelectCsvObjectTestHelper(bucket)
    content = await helper.test_select_csv_object(
        "select Year,StateAbbr, CityName, Short_Question_Text from ossobject where "
        "(data_value || data_value_unit) = '14.8%'",
    )
    select_data = b""

    with open("tests/mock_data/sample_data.csv") as csvfile:
        spamreader = csv.DictReader(csvfile, delimiter=",", quotechar='"')
        for row in spamreader:
            line = b""
            if row["Data_Value_Unit"] == "%" and row["Data_Value"] == "14.8":
                line += row["Year"].encode("utf-8")
                line += ",".encode("utf-8")
                line += row["StateAbbr"].encode("utf-8")
                line += ",".encode("utf-8")
                line += row["CityName"].encode("utf-8")
                line += ",".encode("utf-8")
                line += row["Short_Question_Text"].encode("utf-8")
                line += "\n".encode("utf-8")
                select_data += line

        assert select_data == content


@pytest.mark.asyncio
async def test_select_csv_object_complicate_condition(bucket):
    helper = SelectCsvObjectTestHelper(bucket)
    content = await helper.test_select_csv_object(
        "select Year,StateAbbr, CityName, Short_Question_Text, data_value, data_value_unit,"
        " category, high_confidence_limit from ossobject where data_value > 14.8 and "
        "data_value_unit = '%' or Measure like '%18 Years' and Category = 'Unhealthy Behaviors'"
        " or high_confidence_limit > 70.0 ",
    )
    select_data = b""

    matcher = re.compile("^.*18 Years$")
    with open("tests/mock_data/sample_data.csv") as csvfile:
        spamreader = csv.DictReader(csvfile, delimiter=",", quotechar='"')
        for row in spamreader:
            line = b""
            if (
                len(row["Data_Value"]) > 0
                and float(row["Data_Value"]) > 14.8
                and row["Data_Value_Unit"] == "%"
                or matcher.match(row["Measure"])
                and row["Category"] == "Unhealthy Behaviors"
                or len(row["High_Confidence_Limit"]) > 0
                and float(row["High_Confidence_Limit"]) > 70.0
            ):
                line += row["Year"].encode("utf-8")
                line += ",".encode("utf-8")
                line += row["StateAbbr"].encode("utf-8")
                line += ",".encode("utf-8")
                line += row["CityName"].encode("utf-8")
                line += ",".encode("utf-8")
                line += row["Short_Question_Text"].encode("utf-8")
                line += ",".encode("utf-8")
                line += row["Data_Value"].encode("utf-8")
                line += ",".encode("utf-8")
                line += row["Data_Value_Unit"].encode("utf-8")
                line += ",".encode("utf-8")
                line += row["Category"].encode("utf-8")
                line += ",".encode("utf-8")
                line += row["High_Confidence_Limit"].encode("utf-8")
                line += "\n".encode("utf-8")
                select_data += line
        assert select_data == content


@pytest.mark.asyncio
async def test_select_csv_object_invalid_sql(bucket):
    helper = SelectCsvObjectTestHelper(bucket)

    await helper.test_select_csv_object_invalid_request(
        "select * from ossobject where avg(cast(year as int)) > 2016"
    )
    await helper.test_select_csv_object_invalid_request("")
    await helper.test_select_csv_object_invalid_request("select year || CityName from ossobject")
    await helper.test_select_csv_object_invalid_request("select * from ossobject group by CityName")
    await helper.test_select_csv_object_invalid_request("select * from ossobject order by _1")
    await helper.test_select_csv_object_invalid_request(
        "select * from ossobject oss join s3object s3 on oss.CityName = s3.CityName"
    )


@pytest.mark.asyncio
async def test_select_csv_object_with_invalid_data(bucket):
    key = f"{OSS_PREFIX}/invalid_city_sample_data.csv"
    await bucket.put_object_from_file(key, "tests/mock_data/invalid_sample_data.csv")
    input_format = {"CsvHeaderInfo": "Use"}
    result = await bucket.select_object(key, "select _1 from ossobject", None, input_format)
    content = b""

    try:
        async for chunk in result:
            content += chunk
        assert False
    except SelectOperationFailed:
        print("Got the exception. Ok.")


@pytest.mark.asyncio
async def test_select_csv_object_into_file(bucket):
    key = f"{OSS_PREFIX}/city_sample_data.csv"
    await bucket.put_object_from_file(key, "tests/mock_data/sample_data.csv")
    input_format = {
        "CsvHeaderInfo": "None",
        "CommentCharacter": "#",
        "RecordDelimiter": "\n",
        "FieldDelimiter": ",",
        "QuoteCharacter": '"',
        "SplitRange": (0, None),
    }
    output_file = "tests/mock_data/sample_data_out.csv"

    head_csv_params = {
        "RecordDelimiter": "\n",
        "FieldDelimiter": ",",
        "QuoteCharacter": '"',
        "OverwriteIfExists": True,
    }

    await bucket.create_select_object_meta(key, head_csv_params)

    await bucket.select_object_to_file(
        key, output_file, "select * from ossobject", None, input_format
    )
    f1 = open("tests/mock_data/sample_data.csv")
    content1 = f1.read()
    f1.close()

    f2 = open(output_file)
    content2 = f2.read()
    f2.close()

    os.remove(output_file)
    assert content1.rstrip() == content2.rstrip()  # \n


@pytest.mark.asyncio
async def test_select_gzip_csv_object_into_file(bucket):
    key = f"{OSS_PREFIX}/city_sample_data.csv.gz"
    await bucket.put_object_from_file(key, "tests/mock_data/sample_data.csv.gz")
    input_format = {
        "CsvHeaderInfo": "None",
        "CommentCharacter": "#",
        "RecordDelimiter": "\n",
        "FieldDelimiter": ",",
        "QuoteCharacter": '"',
        "CompressionType": "GZIP",
    }
    output_file = "tests/mock_data/sample_data_out.csv"

    await bucket.select_object_to_file(
        key, output_file, "select * from ossobject", None, input_format
    )
    f1 = open("tests/mock_data/sample_data.csv")
    content1 = f1.read()
    f1.close()

    f2 = open(output_file)
    content2 = f2.read()
    f2.close()

    os.remove(output_file)
    assert content1.rstrip() == content2.rstrip()  # \n


@pytest.mark.asyncio
async def test_select_csv_object_none_range(bucket):
    key = f"{OSS_PREFIX}/city_sample_data.csv"
    await bucket.put_object_from_file(key, "tests/mock_data/sample_data.csv")
    await bucket.create_select_object_meta(key)

    input_formats = [
        {"SplitRange": (None, None)},
        {"LineRange": (None, None)},
        {"SplitRange": None},
        {"LineRange": None},
    ]

    for input_format in input_formats:
        result = await bucket.select_object(key, "select * from ossobject", None, input_format)
        content = b""
        async for chunk in result:
            content += chunk

        assert len(content) > 0


@pytest.mark.asyncio
async def test_with_select_result(bucket):
    key = f"{OSS_PREFIX}/city_sample_data.csv"
    await bucket.put_object_from_file(key, "tests/mock_data/sample_data.csv")
    await bucket.create_select_object_meta(key)

    input_formats = [
        {"SplitRange": (None, None)},
        {"LineRange": (None, None)},
        {"SplitRange": None},
        {"LineRange": None},
    ]

    for input_format in input_formats:
        result1 = await bucket.select_object(key, "select * from ossobject", None, input_format)
        content1 = b""
        async for chunk in result1:
            content1 += chunk
        assert len(content1) > 0

        result2 = await bucket.select_object(key, "select * from ossobject", None, input_format)
        content2 = b""
        async with result2 as f:
            async for chunk in f:
                content2 += chunk
        assert len(content2) > 0

        result3 = await bucket.select_object(key, "select * from ossobject", None, input_format)
        async with result3 as f:
            pass
        content3 = await result3.read()
        assert len(content3) == 0


@pytest.mark.asyncio
async def test_create_csv_object_meta_invalid_request(bucket):
    key = f"{OSS_PREFIX}/city_sample_data.csv"
    await bucket.put_object_from_file(key, "tests/mock_data/sample_data.csv")
    format = {"CompressionType": "GZIP"}
    try:
        await bucket.create_select_object_meta(key, format)
        assert False
    except oss2.exceptions.ServerError:
        print("expected error occured")


@pytest.mark.asyncio
async def test_create_csv_object_meta_invalid_request2(bucket):
    key = f"{OSS_PREFIX}/city_sample_data.csv"
    await bucket.put_object_from_file(key, "tests/mock_data/sample_data.csv")
    format = {"invalid_type": "value", "CompressionType": "GZIP"}
    try:
        await bucket.create_select_object_meta(key, format)
        assert False
    except SelectOperationClientError:
        print("expected error occured")


@pytest.mark.asyncio
async def test_select_csv_object_with_output_delimiters(bucket):
    key = "test_select_csv_object_with_output_delimiters"
    content = "abc,def\n"
    await bucket.put_object(key, content.encode("utf_8"))
    select_params = {"OutputRecordDelimiter": "\r\n", "OutputFieldDelimiter": "|"}
    result = await bucket.select_object(key, "select _1, _2 from ossobject", None, select_params)
    content = b""
    async for chunk in result:
        content += chunk

    assert content == "abc|def\r\n".encode("utf-8")


@pytest.mark.asyncio
async def test_select_csv_object_with_crc(bucket):
    key = "test_select_csv_object_with_crc"
    content = "abc,def\n"
    await bucket.put_object(key, content.encode("utf_8"))
    select_params = {"EnablePayloadCrc": True}
    result = await bucket.select_object(
        key, "select * from ossobject where true", None, select_params
    )
    content = await result.read()

    assert content == "abc,def\n".encode("utf-8")


@pytest.mark.asyncio
async def test_select_csv_object_with_skip_partial_data(bucket):
    key = "test_select_csv_object_with_skip_partial_data"
    content = "abc,def\nefg\n"
    await bucket.put_object(key, content.encode("utf_8"))
    select_params = {"SkipPartialDataRecord": "true"}
    result = await bucket.select_object(key, "select _1, _2 from ossobject", None, select_params)
    content = b""
    try:
        async for chunk in result:
            content += chunk
        assert False
    except SelectOperationFailed:
        print("expected error occurs")

    assert content == "abc,def\n".encode("utf-8")


@pytest.mark.asyncio
async def test_select_csv_object_with_max_partial_data(bucket):
    key = "test_select_csv_object_with_skip_partial_data"
    content = "abc,def\nefg\n"
    await bucket.put_object(key, content.encode("utf_8"))
    select_params = {"SkipPartialDataRecord": "true", "MaxSkippedRecordsAllowed": 100}
    result = await bucket.select_object(key, "select _1, _2 from ossobject", None, select_params)
    content = b""
    async for chunk in result:
        content += chunk

    assert content == "abc,def\n".encode("utf-8")


@pytest.mark.asyncio
async def test_select_csv_object_with_output_raw(bucket):
    key = "test_select_csv_object_with_output_raw"
    content = "abc,def\n"
    await bucket.put_object(key, content.encode("utf_8"))
    select_params = {"OutputRawData": "true"}
    result = await bucket.select_object(key, "select _1 from ossobject", None, select_params)
    content = b""
    async for chunk in result:
        content += chunk

    assert content == "abc\n".encode("utf-8")


@pytest.mark.asyncio
async def test_select_csv_object_with_keep_columns(bucket):
    key = "test_select_csv_object_with_keep_columns"
    content = "abc,def\n"
    await bucket.put_object(key, content.encode("utf_8"))
    select_params = {"KeepAllColumns": "true"}
    result = await bucket.select_object(key, "select _1 from ossobject", None, select_params)
    content = b""
    async for chunk in result:
        content += chunk

    assert content == "abc,\n".encode("utf-8")


@pytest.mark.asyncio
async def test_select_csv_object_with_output_header(bucket):
    key = "test_select_csv_object_with_output_header"
    content = "name,job\nabc,def\n"
    await bucket.put_object(key, content.encode("utf_8"))
    select_params = {"OutputHeader": "true", "CsvHeaderInfo": "Use"}
    result = await bucket.select_object(key, "select name from ossobject", None, select_params)
    content = b""
    async for chunk in result:
        content += chunk

    assert content == "name\nabc\n".encode("utf-8")


@pytest.mark.asyncio
async def test_select_csv_object_with_invalid_parameters(bucket):
    key = "test_select_csv_object_with_invalid_parameters"
    content = "name,job\nabc,def\n"
    await bucket.put_object(key, content.encode("utf_8"))
    select_params = {"OutputHeader123": "true", "CsvHeaderInfo": "Use"}
    try:
        result = await bucket.select_object(key, "select name from ossobject", None, select_params)
        assert False
    except SelectOperationClientError:
        print("expected error")


@pytest.mark.asyncio
async def test_select_csv_object_with_bytes_range(bucket):
    key = "test_select_csv_object_with_bytes_range"
    content = "test\nabc\n"
    await bucket.put_object(key, content.encode("utf-8"))
    select_params = {"AllowQuotedRecordDelimiter": False}
    byte_range = [0, 1]
    result = await bucket.select_object(
        key, "select * from ossobject", None, select_params, byte_range
    )
    content = b""
    async for chunk in result:
        content += chunk
    assert content == "test\n".encode("utf-8")


@pytest.mark.asyncio
async def test_select_csv_object_with_bytes_range_invalid(bucket):
    key = "test_select_csv_object_with_bytes_range"
    content = "test\nabc\n"
    await bucket.put_object(key, content.encode("utf-8"))
    byte_range = [0, 1]
    try:
        await bucket.select_object(key, "select * from ossobject", None, None, byte_range)
        assert False
    except ClientError:
        print("expected error")

    select_params = {"AllowQuotedRecordDelimiter": True}

    try:
        await bucket.select_object(key, "select * from ossobject", None, select_params, byte_range)
        assert False
    except ClientError:
        print("expected error")

    select_params = {"AllowQuotedRecordDelimiter": False, "Json_Type": "LINES"}

    try:
        await bucket.select_object(key, "select * from ossobject", None, select_params, byte_range)
        assert False
    except SelectOperationClientError:
        print("expected error")
