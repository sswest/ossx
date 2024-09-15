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
from oss2 import Auth, Bucket, Service, compat, exceptions, utils, xml_utils
from oss2.api import _Base, _make_range_string, logger
from oss2.exceptions import ClientError
from oss2.select_params import SelectParameters

from . import _http as http
from . import models
from .utils import async_copyfileobj, warp_async_data

T = TypeVar("T")
ObjectPermission = Literal["default", "private", "public-read", "public-read-write"]
BucketPermission = Literal["private", "public-read", "public-read-write"]
URLTypes = Union["URL", str]
ProxyTypes = Union[URLTypes, "Proxy"]
ProxiesTypes = Union[ProxyTypes, Dict[URLTypes, Union[None, ProxyTypes]]]


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

    def __init__(
        self,
        auth: Auth,
        endpoint: str,
        session: Optional[http.Session] = None,
        connect_timeout=None,
        app_name: str = "",
        proxies: Optional[ProxiesTypes] = None,
        region: Optional[str] = None,
        cloudbox_id: Optional[str] = None,
        is_path_style: bool = False,
    ):
        super().__init__(
            auth,
            endpoint,
            session,
            connect_timeout,
            app_name,
            proxies,
            region,
            cloudbox_id,
            is_path_style,
        )
        if session is None:
            self.session = http.Session(timeout=self.timeout, proxies=proxies)

    async def list_buckets(
        self,
        prefix: str = "",
        marker: str = "",
        max_keys: int = 100,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
    ) -> models.ListBucketsResult:
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

    async def list_user_data_redundancy_transition(
        self,
        continuation_token: str = "",
        max_keys: int = 100,
    ) -> models.ListUserDataRedundancyTransitionResult:
        return await super().list_user_data_redundancy_transition(continuation_token, max_keys)

    async def list_access_points(
        self,
        max_keys: int = 100,
        continuation_token: str = "",
    ) -> models.ListAccessPointResult:
        return await super().list_access_points(max_keys, continuation_token)

    async def put_public_access_block(
        self,
        block_public_access: bool = False,
    ) -> models.RequestResult:
        result = super().put_public_access_block(block_public_access)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_public_access_block(self) -> models.GetPublicAccessBlockResult:
        return await super().get_public_access_block()

    async def delete_public_access_block(self) -> models.RequestResult:
        result = super().delete_public_access_block()
        resp = await result.resp
        return models.RequestResult(resp)

    async def list_resource_pools(
        self,
        continuation_token: str = "",
        max_keys: int = 100,
    ) -> models.ListResourcePoolsResult:
        return await super().list_resource_pools(continuation_token, max_keys)

    async def get_resource_pool_info(
        self,
        resource_pool_name: str,
    ) -> models.ResourcePoolInfoResult:
        return await super().get_resource_pool_info(resource_pool_name)

    async def list_resource_pool_buckets(
        self,
        resource_pool_name: str,
        continuation_token: str = "",
        max_keys: int = 100,
    ) -> models.ListResourcePoolBucketsResult:
        return await super().list_resource_pool_buckets(
            resource_pool_name, continuation_token, max_keys
        )

    async def put_resource_pool_requester_qos_info(
        self,
        uid: str,
        resource_pool_name: str,
        qos_configuration: models.QoSConfiguration,
    ) -> models.RequestResult:
        result = super().put_resource_pool_requester_qos_info(
            uid, resource_pool_name, qos_configuration
        )
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_resource_pool_requester_qos_info(
        self,
        uid: str,
        resource_pool_name: str,
    ) -> models.RequesterQoSInfoResult:
        return await super().get_resource_pool_requester_qos_info(uid, resource_pool_name)

    async def list_resource_pool_requester_qos_infos(
        self,
        resource_pool_name: str,
        continuation_token: str = "",
        max_keys: int = 100,
    ) -> models.ListResourcePoolRequesterQoSInfosResult:
        return await super().list_resource_pool_requester_qos_infos(
            resource_pool_name, continuation_token, max_keys
        )

    async def delete_resource_pool_requester_qos_info(
        self,
        uid: str,
        resource_pool_name: str,
    ) -> models.RequestResult:
        result = super().delete_resource_pool_requester_qos_info(uid, resource_pool_name)
        resp = await result.resp
        return models.RequestResult(resp)


class AsyncBucket(Bucket, _AsyncBase):
    def __init__(
        self,
        auth: Auth,
        endpoint: str,
        bucket_name: str,
        is_cname: bool = False,
        session: Optional[http.Session] = None,
        connect_timeout: Optional[Union[float, Tuple[float, float]]] = None,
        app_name: str = "",
        enable_crc: bool = True,
        proxies: Optional[ProxiesTypes] = None,
        region: Optional[str] = None,
        cloudbox_id: Optional[str] = None,
        is_path_style: bool = False,
        is_verify_object_strict: bool = True,
    ):
        super().__init__(
            auth,
            endpoint,
            bucket_name,
            is_cname,
            session,
            connect_timeout,
            app_name,
            enable_crc,
            proxies,
            region,
            cloudbox_id,
            is_path_style,
            is_verify_object_strict,
        )
        if session is None:
            self.session = http.Session(timeout=self.timeout, proxies=proxies)

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
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
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
        return models.SelectObjectResult(resp, progress_callback, crc_enabled)

    async def get_object_to_file(
        self,
        key: str,
        filename: str,
        byte_range: Optional[Tuple[int, int]] = None,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
        progress_callback: Optional[Callable[[int, Optional[int]], Any]] = None,
        process: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> models.GetObjectResult:
        logger.debug(
            f"Start to get object to file, bucket: {self.bucket_name}"
            f", key: {key}, file path: {filename}"
        )
        async with aiofiles.open(compat.to_unicode(filename), "wb") as f:
            result = await self.get_object(
                key,
                byte_range=byte_range,
                headers=headers,
                progress_callback=progress_callback,
                process=process,
                params=params,
            )
            await async_copyfileobj(result, f, result.content_length, request_id=result.request_id)

            if self.enable_crc and byte_range is None:
                if (
                    (headers is None)
                    or ("Accept-Encoding" not in headers)
                    or (headers["Accept-Encoding"] != "gzip")
                ):
                    utils.check_crc("get", result.client_crc, result.server_crc, result.request_id)

        return result

    async def get_object_with_url(
        self,
        sign_url: str,
        byte_range: Optional[Tuple[int, int]] = None,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
        progress_callback: Optional[Callable[[int, Optional[int]], Any]] = None,
    ) -> models.GetObjectResult:
        headers = http.CaseInsensitiveDict(headers)

        range_string = _make_range_string(byte_range)
        if range_string:
            headers["range"] = range_string

        logger.debug(
            f"Start to get object with url, bucket: {self.bucket_name}, sign_url: {sign_url},"
            f" range: {range_string}, headers: {headers}"
        )
        resp = await self._do_url("GET", sign_url, headers=headers)
        return models.GetObjectResult(resp, progress_callback, self.enable_crc)

    async def get_object_with_url_to_file(
        self,
        sign_url: str,
        filename: str,
        byte_range: Optional[Tuple[int, int]] = None,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
        progress_callback: Optional[Callable[[int, Optional[int]], Any]] = None,
    ) -> models.GetObjectResult:
        logger.debug(
            f"Start to get object with url, bucket: {self.bucket_name}, sign_url: {sign_url}"
            f", file path: {filename}, range: {byte_range}, headers: {headers}"
        )

        async with aiofiles.open(compat.to_unicode(filename), "wb") as f:
            result = await self.get_object_with_url(
                sign_url,
                byte_range=byte_range,
                headers=headers,
                progress_callback=progress_callback,
            )
            await async_copyfileobj(result, f, result.content_length, request_id=result.request_id)

        return result

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
        return models.GetSelectObjectMetaResult(resp)

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

    async def put_bucket_request_payment(self, payer: str) -> models.RequestResult:
        result = super().put_bucket_request_payment(payer)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_bucket_request_payment(self) -> models.GetBucketRequestPaymentResult:
        return await super().get_bucket_request_payment()

    async def put_bucket_qos_info(
        self, bucket_qos_info: models.BucketQosInfo
    ) -> models.RequestResult:
        result = super().put_bucket_qos_info(bucket_qos_info)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_bucket_qos_info(self) -> models.GetBucketQosInfoResult:
        return await super().get_bucket_qos_info()

    async def delete_bucket_qos_info(self) -> models.RequestResult:
        result = super().delete_bucket_qos_info()
        resp = await result.resp
        return models.RequestResult(resp)

    async def set_bucket_storage_capacity(
        self, user_qos: models.BucketUserQos
    ) -> models.RequestResult:
        result = super().set_bucket_storage_capacity(user_qos)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_bucket_storage_capacity(self) -> models.GetBucketUserQosResult:
        return await super().get_bucket_storage_capacity()

    async def put_async_fetch_task(
        self, task_config: models.AsyncFetchTaskConfiguration
    ) -> models.PutAsyncFetchTaskResult:
        result = super().put_async_fetch_task(task_config)
        resp = await result.resp
        return models.PutAsyncFetchTaskResult(resp)

    async def get_async_fetch_task(self, task_id: str) -> models.GetAsyncFetchTaskResult:
        return await super().get_async_fetch_task(task_id)

    async def put_bucket_inventory_configuration(
        self, inventory_configuration: models.InventoryConfiguration
    ) -> models.RequestResult:
        result = super().put_bucket_inventory_configuration(inventory_configuration)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_bucket_inventory_configuration(
        self, inventory_id: str
    ) -> models.GetInventoryConfigurationResult:
        return await super().get_bucket_inventory_configuration(inventory_id)

    async def list_bucket_inventory_configurations(
        self, continuation_token: Optional[str] = None
    ) -> models.ListInventoryConfigurationsResult:
        return await super().list_bucket_inventory_configurations(continuation_token)

    async def delete_bucket_inventory_configuration(
        self, inventory_id: str
    ) -> models.RequestResult:
        result = super().delete_bucket_inventory_configuration(inventory_id)
        resp = await result.resp
        return models.RequestResult(resp)

    async def init_bucket_worm(
        self, retention_period_days: Optional[int] = None
    ) -> models.InitBucketWormResult:
        logger.debug(
            "Start to init bucket worm, bucket: {0}, retention_period_days: {1}.".format(
                self.bucket_name, retention_period_days
            )
        )
        data = xml_utils.to_put_init_bucket_worm(retention_period_days)
        headers = http.CaseInsensitiveDict()
        headers["Content-MD5"] = utils.content_md5(data)
        resp = await self.__do_bucket("POST", data=data, params={Bucket.WORM: ""}, headers=headers)
        logger.debug(
            "init bucket worm done, req_id: {0}, status_code: {1}".format(
                resp.request_id, resp.status
            )
        )

        result = models.InitBucketWormResult(resp)
        result.worm_id = resp.headers.get("x-oss-worm-id")
        return result

    async def abort_bucket_worm(self) -> models.RequestResult:
        result = super().abort_bucket_worm()
        resp = await result.resp
        return models.RequestResult(resp)

    async def complete_bucket_worm(self, worm_id: Optional[str] = None) -> models.RequestResult:
        result = super().complete_bucket_worm(worm_id)
        resp = await result.resp
        return models.RequestResult(resp)

    async def extend_bucket_worm(
        self, worm_id: Optional[str] = None, retention_period_days: Optional[int] = None
    ) -> models.RequestResult:
        result = super().extend_bucket_worm(worm_id, retention_period_days)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_bucket_worm(self) -> models.GetBucketWormResult:
        return await super().get_bucket_worm()

    async def put_bucket_replication(self, rule: models.ReplicationRule) -> models.RequestResult:
        result = super().put_bucket_replication(rule)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_bucket_replication(self) -> models.GetBucketReplicationResult:
        return await super().get_bucket_replication()

    async def delete_bucket_replication(self, rule_id: str) -> models.RequestResult:
        result = super().delete_bucket_replication(rule_id)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_bucket_replication_location(self) -> models.GetBucketReplicationLocationResult:
        return await super().get_bucket_replication_location()

    async def get_bucket_replication_progress(
        self, rule_id: str
    ) -> models.GetBucketReplicationProgressResult:
        return await super().get_bucket_replication_progress(rule_id)

    async def put_bucket_transfer_acceleration(self, enabled: str) -> models.RequestResult:
        result = super().put_bucket_transfer_acceleration(enabled)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_bucket_transfer_acceleration(self) -> models.GetBucketTransferAccelerationResult:
        return await super().get_bucket_transfer_acceleration()

    async def create_bucket_cname_token(self, domain: str) -> models.CreateBucketCnameTokenResult:
        return await super().create_bucket_cname_token(domain)

    async def get_bucket_cname_token(self, domain: str) -> models.GetBucketCnameTokenResult:
        return await super().get_bucket_cname_token(domain)

    async def put_bucket_cname(self, input: models.PutBucketCnameRequest) -> models.RequestResult:
        result = super().put_bucket_cname(input)
        resp = await result.resp
        return models.RequestResult(resp)

    async def list_bucket_cname(self) -> models.ListBucketCnameResult:
        return await super().list_bucket_cname()

    async def delete_bucket_cname(self, domain: str) -> models.RequestResult:
        result = super().delete_bucket_cname(domain)
        resp = await result.resp
        return models.RequestResult(resp)

    async def open_bucket_meta_query(self) -> models.RequestResult:
        result = super().open_bucket_meta_query()
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_bucket_meta_query_status(self) -> models.GetBucketMetaQueryResult:
        return await super().get_bucket_meta_query_status()

    async def do_bucket_meta_query(
        self, do_meta_query_request: models.MetaQuery
    ) -> models.DoBucketMetaQueryResult:
        return await super().do_bucket_meta_query(do_meta_query_request)

    async def close_bucket_meta_query(self) -> models.RequestResult:
        result = super().close_bucket_meta_query()
        resp = await result.resp
        return models.RequestResult(resp)

    async def put_bucket_access_monitor(self, status: str) -> models.RequestResult:
        result = super().put_bucket_access_monitor(status)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_bucket_access_monitor(self) -> models.GetBucketAccessMonitorResult:
        return await super().get_bucket_access_monitor()

    async def get_bucket_resource_group(self) -> models.GetBucketResourceGroupResult:
        return await super().get_bucket_resource_group()

    async def put_bucket_resource_group(self, resourceGroupId: str) -> models.RequestResult:
        result = super().put_bucket_resource_group(resourceGroupId)
        resp = await result.resp
        return models.RequestResult(resp)

    async def put_bucket_style(self, styleName: str, content: str) -> models.RequestResult:
        result = super().put_bucket_style(styleName, content)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_bucket_style(self, styleName: str) -> models.GetBucketStyleResult:
        return await super().get_bucket_style(styleName)

    async def list_bucket_style(self) -> models.ListBucketStyleResult:
        return await super().list_bucket_style()

    async def delete_bucket_style(self, styleName: str) -> models.RequestResult:
        result = super().delete_bucket_style(styleName)
        resp = await result.resp
        return models.RequestResult(resp)

    async def async_process_object(
        self,
        key: str,
        process: str,
        headers: Optional[Union[dict, http.CaseInsensitiveDict]] = None,
    ) -> models.AsyncProcessObject:
        return await super().async_process_object(key, process, headers)

    async def put_bucket_callback_policy(self, callbackPolicy: str) -> models.RequestResult:
        result = super().put_bucket_callback_policy(callbackPolicy)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_bucket_callback_policy(self) -> models.GetBucketPolicyResult:
        return await super().get_bucket_callback_policy()

    async def delete_bucket_callback_policy(self) -> models.RequestResult:
        result = super().delete_bucket_callback_policy()
        resp = await result.resp
        return models.RequestResult(resp)

    async def put_bucket_archive_direct_read(self, enabled: bool = False) -> models.RequestResult:
        result = super().put_bucket_archive_direct_read(enabled)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_bucket_archive_direct_read(self) -> models.GetBucketArchiveDirectReadResult:
        return await super().get_bucket_archive_direct_read()

    async def put_bucket_https_config(
        self, httpsConfig: models.BucketTlsVersion
    ) -> models.RequestResult:
        result = super().put_bucket_https_config(httpsConfig)
        resp = await result.resp
        return models.RequestResult(resp)

    async def create_bucket_data_redundancy_transition(
        self,
        targetType: str,
    ) -> models.CreateDataRedundancyTransitionResult:
        return await super().create_bucket_data_redundancy_transition(targetType)

    async def get_bucket_data_redundancy_transition(
        self, taskId: str
    ) -> models.DataRedundancyTransitionInfoResult:
        return await super().get_bucket_data_redundancy_transition(taskId)

    async def delete_bucket_data_redundancy_transition(self, taskId: str) -> models.RequestResult:
        result = super().delete_bucket_data_redundancy_transition(taskId)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_bucket_https_config(self) -> models.HttpsConfigResult:
        return await super().get_bucket_https_config()

    async def list_bucket_data_redundancy_transition(
        self,
    ) -> models.ListBucketDataRedundancyTransitionResult:
        return await super().list_bucket_data_redundancy_transition()

    async def create_access_point(
        self,
        accessPoint: models.CreateAccessPointRequest,
    ) -> models.CreateAccessPointResult:
        return await super().create_access_point(accessPoint)

    async def get_access_point(self, accessPointName: str) -> models.GetAccessPointResult:
        return await super().get_access_point(accessPointName)

    async def delete_access_point(self, accessPointName: str) -> models.RequestResult:
        result = super().delete_access_point(accessPointName)
        resp = await result.resp
        return models.RequestResult(resp)

    async def list_bucket_access_points(
        self,
        max_keys: int = 100,
        continuation_token: str = "",
    ) -> models.ListAccessPointResult:
        return await super().list_bucket_access_points(max_keys, continuation_token)

    async def put_access_point_policy(
        self,
        accessPointName: str,
        accessPointPolicy: str,
    ) -> models.RequestResult:
        result = super().put_access_point_policy(accessPointName, accessPointPolicy)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_access_point_policy(
        self,
        accessPointName: str,
    ) -> models.GetAccessPointPolicyResult:
        """获取接入点策略
        :param str accessPointName: 接入点名称
        :return: :class:`GetAccessPointPolicyResult <oss2.models.GetAccessPointPolicyResult>`
        """
        result = super().get_access_point_policy(accessPointName)
        resp = await result.resp
        return models.GetAccessPointPolicyResult(resp)

    async def delete_access_point_policy(self, accessPointName: str) -> models.RequestResult:
        result = super().delete_access_point_policy(accessPointName)
        resp = await result.resp
        return models.RequestResult(resp)

    async def put_bucket_public_access_block(
        self, block_public_access: bool = False
    ) -> models.RequestResult:
        result = super().put_bucket_public_access_block(block_public_access)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_bucket_public_access_block(self) -> models.GetBucketPublicAccessBlockResult:
        return await super().get_bucket_public_access_block()

    async def delete_bucket_public_access_block(self) -> models.RequestResult:
        result = super().delete_bucket_public_access_block()
        resp = await result.resp
        return models.RequestResult(resp)

    async def put_access_point_public_access_block(
        self,
        access_point_name: str,
        block_public_access: bool = False,
    ) -> models.RequestResult:
        result = super().put_access_point_public_access_block(
            access_point_name, block_public_access
        )
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_access_point_public_access_block(
        self,
        access_point_name: str,
    ) -> models.GetBucketPublicAccessBlockResult:
        return await super().get_access_point_public_access_block(access_point_name)

    async def delete_access_point_public_access_block(
        self,
        access_point_name: str,
    ) -> models.RequestResult:
        result = super().delete_access_point_public_access_block(access_point_name)
        resp = await result.resp
        return models.RequestResult(resp)

    async def put_bucket_requester_qos_info(
        self,
        uid: str,
        qos_configuration: models.QoSConfiguration,
    ) -> models.RequestResult:
        result = super().put_bucket_requester_qos_info(uid, qos_configuration)
        resp = await result.resp
        return models.RequestResult(resp)

    async def get_bucket_requester_qos_info(self, uid: str) -> models.RequesterQoSInfoResult:
        return await super().get_bucket_requester_qos_info(uid)

    async def list_bucket_requester_qos_infos(
        self,
        continuation_token: str = "",
        max_keys: int = 100,
    ) -> models.ListBucketRequesterQoSInfosResult:
        return await super().list_bucket_requester_qos_infos(continuation_token, max_keys)

    async def delete_bucket_requester_qos_info(self, uid: str) -> models.RequestResult:
        result = super().delete_bucket_requester_qos_info(uid)
        resp = await result.resp
        return models.RequestResult(resp)

    def __do_object(self, method, key, **kwargs):
        if not self.bucket_name:
            raise exceptions.ClientError("Bucket name should not be null or empty.")
        if not key:
            raise exceptions.ClientError("key should not be null or empty.")
        return self._do(method, self.bucket_name, key, **kwargs)
