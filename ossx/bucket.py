import types
from pathlib import Path
from typing import (
    IO,
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import aiofiles
from oss2 import Bucket, Service, compat, exceptions, models, utils, xml_utils
from oss2.api import _Base, _make_range_string, logger
from oss2.exceptions import ClientError
from oss2.select_params import SelectParameters

from . import _http as http
from .models import SelectObjectResult, GetSelectObjectMetaResult
from .utils import warp_async_data

T = TypeVar("T")
ObjectPermission = Literal["default", "private", "public-read", "public-read-write"]
BucketPermission = Literal["private", "public-read", "public-read-write"]


async def _async_parse_result(
    co: http.AwaitResponse,
    parse_func: Callable,
    klass: Type[T],
) -> T:
    resp = await co
    result = klass(resp)
    data = await resp.read()
    parse_func(result, data)
    return result


class _AsyncBase(_Base):
    def _async_do(self, method, bucket_name, key, **kwargs):
        key = compat.to_string(key)
        req = http.Request(
            method,
            self._make_url(bucket_name, key),
            app_name=self.app_name,
            proxies=self.proxies,
            region=self.region,
            product=self.product,
            cloudbox_id=self.cloudbox_id,
            **kwargs,
        )
        self.auth._sign_request(req, bucket_name, key)
        return self._async_do_request(req)

    def _async_do_request(self, req: http.Request):
        resp = self.session.do_request(req, timeout=self.timeout)
        return resp

    def _async_do_url(self, method, sign_url, **kwargs):
        req = http.Request(method, sign_url, app_name=self.app_name, proxies=self.proxies, **kwargs)
        return self._async_do_request(req)


class AsyncService(Service, _AsyncBase):

    _do = _AsyncBase._async_do
    _do_url = _AsyncBase._async_do_url
    _parse_result = staticmethod(_async_parse_result)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session = http.Session(timeout=self.timeout)

    async def list_buckets(
        self,
        prefix: str = "",
        marker: str = "",
        max_keys: int = 100,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
    ):
        return await super().list_buckets(prefix, marker, max_keys, params, headers)

    async def get_user_qos_info(self) -> models.GetUserQosInfoResult:
        return await super().get_user_qos_info()

    async def describe_regions(self, regions: str = "") -> models.DescribeRegionsResult:
        return await super().describe_regions(regions)

    async def write_get_object_response(
        self,
        route: str,
        token: str,
        fwd_status: str,
        data: Union[str, bytes, IO],
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
    ) -> models.RequestResult:
        data = warp_async_data(data)
        result = super().write_get_object_response(route, token, fwd_status, data, headers)
        resp = await result.resp
        return models.RequestResult(resp)


class AsyncBucket(Bucket, _AsyncBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session = http.Session(timeout=self.timeout)

    _do = _AsyncBase._async_do
    _do_url = _AsyncBase._async_do_url
    _parse_result = staticmethod(_async_parse_result)

    async def list_objects(
        self,
        prefix: str = "",
        delimiter: str = "",
        marker: str = "",
        max_keys: int = 100,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
    ) -> models.ListObjectsResult:
        return await super().list_objects(prefix, delimiter, marker, max_keys, headers)

    async def list_objects_v2(
        self,
        prefix: str = "",
        delimiter: str = "",
        continuation_token: str = "",
        start_after: str = "",
        fetch_owner: bool = False,
        encoding_type: str = "url",
        max_keys: int = 100,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
    ) -> models.ListObjectsV2Result:
        return await super().list_objects_v2(
            prefix,
            delimiter,
            continuation_token,
            start_after,
            fetch_owner,
            encoding_type,
            max_keys,
            headers,
        )

    async def put_object(
        self,
        key: str,
        data: Union[str, bytes, IO],
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
        progress_callback: Optional[Callable[[int, Optional[int]], Any]] = None,
    ) -> models.PutObjectResult:
        data = warp_async_data(data)
        result = super().put_object(key, data, headers, progress_callback)
        resp = await result.resp
        return models.PutObjectResult(resp)

    async def put_object_from_file(
        self,
        key: str,
        filename: Union[str, Path],
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
        progress_callback: Optional[Callable[[int, Optional[int]], Any]] = None,
    ) -> models.PutObjectResult:
        headers = utils.set_content_type(http.CaseInsensitiveDict(headers), filename)
        logger.debug(
            "Put object from file, bucket: {0}, key: {1}, file path: {2}".format(
                self.bucket_name, compat.to_string(key), filename
            )
        )
        async with aiofiles.open(filename, "rb") as f:
            return await self.put_object(
                key, f, headers=headers, progress_callback=progress_callback
            )

    async def put_object_with_url(
        self,
        sign_url: str,
        data: Union[bytes, str, IO],
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
        progress_callback: Optional[Callable[[int, Optional[int]], Any]] = None,
    ) -> models.PutObjectResult:
        data = warp_async_data(data)
        result = super().put_object_with_url(sign_url, data, headers, progress_callback)
        resp = await result.resp
        return models.PutObjectResult(resp)

    async def put_object_with_url_from_file(
        self,
        sign_url: str,
        filename: Union[str, Path],
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
        progress_callback: Optional[Callable[[int, Optional[int]], Any]] = None,
    ) -> models.PutObjectResult:
        logger.debug(
            "Put object from file with signed url, bucket: {0}, sign_url: {1}, file path: {2}".format(
                self.bucket_name, sign_url, filename
            )
        )
        async with aiofiles.open(compat.to_unicode(filename), "rb") as f:
            return await self.put_object_with_url(
                sign_url, f, headers=headers, progress_callback=progress_callback
            )

    async def append_object(
        self,
        key: str,
        position: int,
        data: Union[str, bytes, IO],
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
        progress_callback: Optional[Callable[[int, Optional[int]], Any]] = None,
        init_crc: Optional[int] = None,
    ) -> models.AppendObjectResult:
        data = warp_async_data(data)
        result = super().append_object(key, position, data, headers, progress_callback, init_crc)
        resp = await result.resp
        return models.AppendObjectResult(resp)

    async def get_object(
        self,
        key: str,
        byte_range: Optional[Tuple[int, int]] = None,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
        progress_callback: Optional[Callable[[int, Optional[int]], Any]] = None,
        process: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
    ):
        enable_crc = self.enable_crc
        self.enable_crc = False
        result = super().get_object(key, byte_range, headers, None, process, params)
        self.enable_crc = enable_crc
        resp = await result.stream

        return models.GetObjectResult(resp, progress_callback, self.enable_crc)

    async def select_object(
        self,
        key: str,
        sql: str,
        progress_callback: Optional[Callable[[int, Optional[int]], Any]] = None,
        select_params: Optional[Dict[str, str]] = None,
        byte_range: Optional[Tuple[int, int]] = None,
        headers: Optional[Dict[str, str]] = None,
    ):
        range_select = False
        headers = http.CaseInsensitiveDict(headers)
        range_string = _make_range_string(byte_range)
        if range_string:
            headers["range"] = range_string
            range_select = True

        if range_select is True and (
            select_params is None
            or (
                SelectParameters.AllowQuotedRecordDelimiter not in select_params
                or str(select_params[SelectParameters.AllowQuotedRecordDelimiter]).lower()
                != "false"
            )
        ):
            raise ClientError(
                '"AllowQuotedRecordDelimiter" must be specified in select_params as False'
                ' when "Range" is specified in header.'
            )

        body = xml_utils.to_select_object(sql, select_params)
        params = {"x-oss-process": "csv/select"}
        if select_params is not None and SelectParameters.Json_Type in select_params:
            params["x-oss-process"] = "json/select"

        self.timeout = 3600
        resp = await self.__do_object("POST", key, data=body, headers=headers, params=params)
        crc_enabled = False
        if select_params is not None and SelectParameters.EnablePayloadCrc in select_params:
            if str(select_params[SelectParameters.EnablePayloadCrc]).lower() == "true":
                crc_enabled = True
        return SelectObjectResult(resp, progress_callback, crc_enabled)

    async def get_object_to_file(
        self,
        key,
        filename,
        byte_range=None,
        headers=None,
        progress_callback=None,
        process=None,
        params=None,
    ):
        raise NotImplementedError

    async def get_object_with_url(
        self, sign_url, byte_range=None, headers=None, progress_callback=None
    ):
        raise NotImplementedError

    async def get_object_with_url_to_file(
        self, sign_url, filename, byte_range=None, headers=None, progress_callback=None
    ):
        raise NotImplementedError

    async def select_object_to_file(
        self,
        key: str,
        filename: str,
        sql: str,
        progress_callback: Optional[Callable[[int, Optional[int]], Any]] = None,
        select_params: Optional[Dict[str, str]] = None,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
    ):
        async with aiofiles.open(filename, "wb") as f:
            result = await self.select_object(
                key,
                sql,
                progress_callback=progress_callback,
                select_params=select_params,
                headers=headers,
            )

            async for chunk in result:
                await f.write(chunk)

        return result

    async def head_object(
        self,
        key: str,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> models.HeadObjectResult:
        return await super().head_object(key, headers)

    async def create_select_object_meta(self, key, select_meta_params=None, headers=None):
        headers = http.CaseInsensitiveDict(headers)

        body = xml_utils.to_get_select_object_meta(select_meta_params)
        params = {"x-oss-process": "csv/meta"}
        if select_meta_params is not None and "Json_Type" in select_meta_params:
            params["x-oss-process"] = "json/meta"

        self.timeout = 3600
        resp = await self.__do_object("POST", key, data=body, headers=headers, params=params)
        return GetSelectObjectMetaResult(resp)

    async def get_object_meta(
        self,
        key: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
    ) -> models.GetObjectMetaResult:
        result = super().get_object_meta(key, params, headers)
        resp = await result.resp
        return models.GetObjectMetaResult(resp)

    async def object_exists(
        self,
        key: str,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
    ) -> bool:
        try:
            await self.get_object_meta(key, headers=headers)
        except exceptions.NoSuchKey:
            return False
        except exceptions.NoSuchBucket:
            raise
        except exceptions.NotFound:
            return False

        return True

    async def copy_object(
        self,
        source_bucket_name: str,
        source_key: str,
        target_key: str,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> models.PutObjectResult:
        result = super().copy_object(source_bucket_name, source_key, target_key, headers, params)
        resp = await result.resp
        return models.PutObjectResult(resp)

    async def update_object_meta(
        self,
        key: str,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]],
    ) -> models.PutObjectResult:
        return await super().update_object_meta(key, headers)

    async def delete_object(
        self,
        key: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
    ) -> models.RequestResult:
        headers = http.CaseInsensitiveDict(headers)

        logger.info(
            "Start to delete object, bucket: {0}, key: {1}".format(
                self.bucket_name, compat.to_string(key)
            )
        )
        resp = await self.__do_object("DELETE", key, params=params, headers=headers)
        logger.debug(
            "Delete object done, req_id: {0}, status_code: {1}".format(resp.request_id, resp.status)
        )
        return models.RequestResult(resp)

    async def restore_object(
        self,
        key: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
        input: Optional[models.RestoreConfiguration] = None,
    ) -> models.RequestResult:
        result = super().restore_object(key, params, headers, input)
        resp = await result.resp
        logger.debug(
            "Restore object done, req_id: {0}, status_code: {1}".format(
                resp.request_id, resp.status
            )
        )
        return models.RequestResult(resp)

    async def put_object_acl(
        self,
        key: str,
        permission: Literal["default", "private", "public-read", "public-read-write"],
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
    ) -> models.RequestResult:
        result = super().put_object_acl(key, permission, params, headers)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_object_acl(
        self,
        key: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
    ) -> models.GetObjectAclResult:
        return await super().get_object_acl(key, params, headers)

    async def batch_delete_objects(
        self,
        key_list: List[str],
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
    ) -> models.BatchDeleteObjectsResult:
        return await super().batch_delete_objects(key_list, headers)

    async def delete_object_versions(
        self,
        keylist_versions: models.BatchDeleteObjectVersionList,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
    ) -> models.BatchDeleteObjectsResult:
        return await super().delete_object_versions(keylist_versions, headers)

    async def init_multipart_upload(
        self,
        key: str,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> models.InitMultipartUploadResult:
        return await super().init_multipart_upload(key, headers, params)

    async def upload_part(
        self,
        key: str,
        upload_id: str,
        part_number: int,
        data: Union[bytes, str, IO],
        progress_callback: Optional[Callable[[int, Optional[int]], Any]] = None,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
    ) -> models.PutObjectResult:
        data = warp_async_data(data)
        result = super().upload_part(key, upload_id, part_number, data, progress_callback, headers)
        resp = await result.resp
        return models.PutObjectResult(resp)

    async def complete_multipart_upload(
        self,
        key: str,
        upload_id: str,
        parts: List[models.PartInfo],
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
    ) -> models.PutObjectResult:
        enable_crc = self.enable_crc
        self.enable_crc = False
        result = super().complete_multipart_upload(key, upload_id, parts, headers)
        self.enable_crc = enable_crc
        resp = await result.resp
        return models.PutObjectResult(resp)

    async def abort_multipart_upload(
        self,
        key: str,
        upload_id: str,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
    ) -> models.RequestResult:
        result = super().abort_multipart_upload(key, upload_id, headers)
        resp = await result.resp
        return models.RequestResult(resp)

    async def list_multipart_uploads(
        self,
        prefix: str = "",
        delimiter: str = "",
        key_marker: str = "",
        upload_id_marker: str = "",
        max_uploads: int = 1000,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
    ) -> models.ListMultipartUploadsResult:
        return await super().list_multipart_uploads(
            prefix, delimiter, key_marker, upload_id_marker, max_uploads, headers
        )

    async def upload_part_copy(
        self,
        source_bucket_name: str,
        source_key: str,
        byte_range: Tuple[int, int],
        target_key: str,
        target_upload_id: str,
        target_part_number: int,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> models.PutObjectResult:
        result = super().upload_part_copy(
            source_bucket_name,
            source_key,
            byte_range,
            target_key,
            target_upload_id,
            target_part_number,
            headers,
            params,
        )
        resp = await result.resp
        return models.PutObjectResult(resp)

    async def list_parts(
        self,
        key: str,
        upload_id: str,
        marker: str = "",
        max_parts: int = 1000,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
    ) -> models.ListPartsResult:
        return await super().list_parts(key, upload_id, marker, max_parts, headers)

    async def put_symlink(
        self,
        target_key: str,
        symlink_key: str,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
    ) -> models.RequestResult:
        result = super().put_symlink(target_key, symlink_key, headers)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_symlink(
        self,
        symlink_key: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
    ) -> models.RequestResult:
        result = super().get_symlink(symlink_key, params, headers)
        resp = await result.resp
        return models.RequestResult(resp)

    async def create_bucket(
        self,
        permission: BucketPermission = "private",
        input: Optional[models.BucketCreateConfig] = None,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
    ) -> models.RequestResult:
        result = super().create_bucket(permission, input, headers)
        resp = await result.resp
        return models.RequestResult(resp)

    async def delete_bucket(self) -> models.RequestResult:
        result = super().delete_bucket()
        resp = await result.resp
        return models.RequestResult(resp)

    async def put_bucket_acl(self, permission: BucketPermission) -> models.RequestResult:
        result = super().put_bucket_acl(permission)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_bucket_acl(self) -> models.GetBucketAclResult:
        return await super().get_bucket_acl()

    async def put_bucket_cors(self, input: models.BucketCors) -> models.RequestResult:
        """设置Bucket的CORS。

        :param input: :class:`BucketCors <oss2.models.BucketCors>` 对象或其他
        """
        result = super().put_bucket_cors(input)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_bucket_cors(self) -> models.GetBucketCorsResult:
        return await super().get_bucket_cors()

    async def delete_bucket_cors(self):
        """删除Bucket的CORS配置。"""
        result = super().delete_bucket_cors()
        resp = await result.resp
        return models.RequestResult(resp)

    async def put_bucket_lifecycle(
        self,
        input: models.BucketLifecycle,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
    ) -> models.RequestResult:
        result = super().put_bucket_lifecycle(input, headers)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_bucket_lifecycle(self) -> models.GetBucketLifecycleResult:
        return await super().get_bucket_lifecycle()

    async def delete_bucket_lifecycle(self) -> models.RequestResult:
        result = super().delete_bucket_lifecycle()
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_bucket_location(self) -> models.GetBucketLocationResult:
        return await super().get_bucket_location()

    async def put_bucket_logging(self, input: models.BucketLogging) -> models.RequestResult:
        result = super().put_bucket_logging(input)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_bucket_logging(self) -> models.GetBucketLoggingResult:
        return await super().get_bucket_logging()

    async def delete_bucket_logging(self) -> models.RequestResult:
        result = super().delete_bucket_logging()
        resp = await result.resp
        return models.RequestResult(resp)

    async def put_bucket_referer(self, input: models.BucketReferer) -> models.RequestResult:
        result = super().put_bucket_referer(input)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_bucket_referer(self) -> models.GetBucketRefererResult:
        return await super().get_bucket_referer()

    async def get_bucket_stat(self) -> models.GetBucketStatResult:
        return await super().get_bucket_stat()

    async def get_bucket_info(self) -> models.GetBucketInfoResult:
        return await super().get_bucket_info()

    async def put_bucket_website(self, input: models.BucketWebsite) -> models.RequestResult:
        result = super().put_bucket_website(input)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_bucket_website(self) -> models.GetBucketWebsiteResult:
        return await super().get_bucket_website()

    async def delete_bucket_website(self) -> models.RequestResult:
        """关闭Bucket的静态网站托管功能。"""
        result = super().delete_bucket_website()
        resp = await result.resp
        return models.RequestResult(resp)

    async def create_live_channel(
        self,
        channel_name: str,
        input: models.LiveChannelInfo,
    ) -> models.CreateLiveChannelResult:
        return await super().create_live_channel(channel_name, input)

    async def delete_live_channel(self, channel_name: str) -> models.RequestResult:
        """删除推流直播频道

        :param str channel_name: 要删除的live channel的名称
        """
        result = super().delete_live_channel(channel_name)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_live_channel(self, channel_name: str) -> models.GetLiveChannelResult:
        return await super().get_live_channel(channel_name)

    async def list_live_channel(
        self,
        prefix: str = "",
        marker: str = "",
        max_keys: int = 100,
    ) -> models.ListLiveChannelResult:
        return await super().list_live_channel(prefix, marker, max_keys)

    async def get_live_channel_stat(self, channel_name: str) -> models.GetLiveChannelStatResult:
        return await super().get_live_channel_stat(channel_name)

    async def put_live_channel_status(self, channel_name: str, status: str) -> models.RequestResult:
        result = super().put_live_channel_status(channel_name, status)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_live_channel_history(
        self,
        channel_name: str,
    ) -> models.GetLiveChannelHistoryResult:
        return await super().get_live_channel_history(channel_name)

    async def post_vod_playlist(
        self,
        channel_name: str,
        playlist_name: str,
        start_time: int = 0,
        end_time: int = 0,
    ) -> models.RequestResult:
        result = super().post_vod_playlist(channel_name, playlist_name, start_time, end_time)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_vod_playlist(
        self,
        channel_name: str,
        start_time: int,
        end_time: int,
    ) -> models.GetVodPlaylistResult:
        result = super().get_vod_playlist(channel_name, start_time, end_time)
        resp = await result.resp
        return models.GetVodPlaylistResult(resp)

    async def process_object(
        self,
        key: str,
        process: str,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
    ) -> models.ProcessObjectResult:
        headers = http.CaseInsensitiveDict(headers)

        logger.debug(
            "Start to process object, bucket: {0}, key: {1}, process: {2}".format(
                self.bucket_name, compat.to_string(key), process
            )
        )
        process_data = "%s=%s" % (Bucket.PROCESS, process)
        resp = await self.__do_object(
            "POST", key, params={Bucket.PROCESS: ""}, headers=headers, data=process_data
        )
        logger.debug(
            "Process object done, req_id: {0}, status_code: {1}".format(
                resp.request_id, resp.status
            )
        )

        data = await resp.read()
        resp.read = types.MethodType(lambda: data, resp)
        return models.ProcessObjectResult(resp)

    async def put_object_tagging(
        self,
        key: str,
        tagging: models.Tagging,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> models.RequestResult:
        result = super().put_object_tagging(key, tagging, headers, params)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_object_tagging(
        self,
        key: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
    ) -> models.GetTaggingResult:
        return await super().get_object_tagging(key, params, headers)

    async def delete_object_tagging(
        self,
        key: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
    ) -> models.RequestResult:
        result = super().delete_object_tagging(key, params, headers)
        resp = await result.resp
        return models.RequestResult(resp)

    async def put_bucket_encryption(
        self, rule: models.ServerSideEncryptionRule
    ) -> models.RequestResult:
        result = super().put_bucket_encryption(rule)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_bucket_encryption(self) -> models.GetServerSideEncryptionResult:
        return await super().get_bucket_encryption()

    async def delete_bucket_encryption(self) -> models.RequestResult:
        result = super().delete_bucket_encryption()
        resp = await result.resp
        return models.RequestResult(resp)

    async def put_bucket_tagging(
        self,
        tagging: models.Tagging,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
    ) -> models.RequestResult:
        result = super().put_bucket_tagging(tagging, headers)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_bucket_tagging(self) -> models.GetTaggingResult:
        return await super().get_bucket_tagging()

    async def delete_bucket_tagging(
        self,
        params: Optional[Dict[str, Any]] = None,
    ) -> models.RequestResult:
        result = super().delete_bucket_tagging(params)
        resp = await result.resp
        return models.RequestResult(resp)

    async def list_object_versions(
        self,
        prefix: str = "",
        delimiter: str = "",
        key_marker: str = "",
        max_keys: int = 100,
        versionid_marker: str = "",
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
    ) -> models.ListObjectVersionsResult:
        return await super().list_object_versions(
            prefix, delimiter, key_marker, max_keys, versionid_marker, headers
        )

    async def put_bucket_versioning(self, config, headers=None) -> models.RequestResult:
        result = super().put_bucket_versioning(config, headers)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_bucket_versioning(self) -> models.GetBucketVersioningResult:
        return await super().get_bucket_versioning()

    async def put_bucket_policy(self, policy: str) -> models.RequestResult:
        result = super().put_bucket_policy(policy)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_bucket_policy(self) -> models.GetBucketPolicyResult:
        result = super().get_bucket_policy()
        resp = await result.resp
        return models.GetBucketPolicyResult(resp)

    async def delete_bucket_policy(self) -> models.RequestResult:
        result = super().delete_bucket_policy()
        resp = await result.resp
        return models.RequestResult(resp)

    # TODO add more methods

    def __do_object(self, method, key, **kwargs):
        if not self.bucket_name:
            raise exceptions.ClientError("Bucket name should not be null or empty.")
        if not key:
            raise exceptions.ClientError("key should not be null or empty.")
        return self._do(method, self.bucket_name, key, **kwargs)
