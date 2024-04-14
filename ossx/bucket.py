from typing import Callable, Type, TypeVar

import aiofiles
from oss2 import Bucket, compat, exceptions, utils
from oss2.api import _Base, logger
from oss2.models import (
    GetObjectMetaResult,
    GetObjectResult,
    PutObjectResult,
    RequestResult,
)

from . import _http as http

T = TypeVar("T")


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
        co = self._async_do_request(req)
        return co

    def _async_do_request(self, req: http.Request):
        resp = self.session.do_request(req, timeout=self.timeout)
        return resp

    def _async_do_url(self, method, sign_url, **kwargs):
        req = http.Request(method, sign_url, app_name=self.app_name, proxies=self.proxies, **kwargs)
        return self._async_do_request(req)


class AsyncBucket(Bucket, _AsyncBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session = http.Session(timeout=self.timeout)

    _do = _AsyncBase._async_do
    _do_url = _AsyncBase._async_do_url
    _parse_result = staticmethod(_async_parse_result)

    async def list_objects(self, prefix="", delimiter="", marker="", max_keys=100, headers=None):
        return await super().list_objects(prefix, delimiter, marker, max_keys, headers)

    async def list_objects_v2(
        self,
        prefix="",
        delimiter="",
        continuation_token="",
        start_after="",
        fetch_owner=False,
        encoding_type="url",
        max_keys=100,
        headers=None,
    ):
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

    async def put_object(self, key, data, headers=None, progress_callback=None):
        enable_crc = self.enable_crc
        self.enable_crc = False
        result: PutObjectResult = super().put_object(key, data, headers, progress_callback)
        self.enable_crc = enable_crc
        resp = await result.resp

        return PutObjectResult(resp)

    async def put_object_from_file(self, key, filename, headers=None, progress_callback=None):
        headers = utils.set_content_type(http.CaseInsensitiveDict(headers), filename)
        logger.debug(
            "Put object from file, bucket: {0}, key: {1}, file path: {2}".format(
                self.bucket_name, compat.to_string(key), filename
            )
        )
        async with aiofiles.open(compat.to_unicode(filename), "rb") as f:
            return await self.put_object(
                key, f, headers=headers, progress_callback=progress_callback
            )

    async def put_object_with_url(self, sign_url, data, headers=None, progress_callback=None):
        raise NotImplementedError

    async def put_object_with_url_from_file(
        self, sign_url, filename, headers=None, progress_callback=None
    ):
        raise NotImplementedError

    async def append_object(
        self, key, position, data, headers=None, progress_callback=None, init_crc=None
    ):
        raise NotImplementedError

    async def get_object(
        self,
        key,
        byte_range=None,
        headers=None,
        progress_callback=None,
        process=None,
        params=None,
    ):
        enable_crc = self.enable_crc
        self.enable_crc = False
        result = super().get_object(key, byte_range, headers, None, process, params)
        self.enable_crc = enable_crc
        resp = await result.stream

        return GetObjectResult(resp, progress_callback, self.enable_crc)

    async def select_object(
        self, key, sql, progress_callback=None, select_params=None, byte_range=None, headers=None
    ):
        raise NotImplementedError

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
        self, key, filename, sql, progress_callback=None, select_params=None, headers=None
    ):
        raise NotImplementedError

    async def head_object(self, key, headers=None):
        return await super().head_object(key, headers)

    async def create_select_object_meta(self, key, select_meta_params=None, headers=None):
        raise NotImplementedError

    async def get_object_meta(self, key, params=None, headers=None):
        result = super().get_object_meta(key, params, headers)
        resp = await result.resp
        return GetObjectMetaResult(resp)

    async def object_exists(self, key, headers=None):
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
        self, source_bucket_name, source_key, target_key, headers=None, params=None
    ):
        result = super().copy_object(source_bucket_name, source_key, target_key, headers, params)
        resp = await result.resp
        return PutObjectResult(resp)

    async def update_object_meta(self, key, headers):
        return await super().update_object_meta(key, headers)

    async def delete_object(self, key, params=None, headers=None):
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
        return RequestResult(resp)

    async def restore_object(self, key, params=None, headers=None, input=None):
        result = super().restore_object(key, params, headers, input)
        resp = await result.resp
        logger.debug(
            "Restore object done, req_id: {0}, status_code: {1}".format(
                resp.request_id, resp.status
            )
        )
        return RequestResult(resp)

    async def put_object_acl(self, key, permission, params=None, headers=None):
        result = super().put_object_acl(key, permission, params, headers)
        resp = await result.resp
        logger.debug(
            "Put object acl done, req_id: {0}, status_code: {1}".format(
                resp.request_id, resp.status
            )
        )
        return RequestResult(resp)

    async def get_object_acl(self, key, params=None, headers=None):
        return await super().get_object_acl(key, params, headers)

    async def batch_delete_objects(self, key_list, headers=None):
        return await super().batch_delete_objects(key_list, headers)

    async def delete_object_versions(self, keylist_versions, headers=None):
        return await super().delete_object_versions(keylist_versions, headers)

    async def init_multipart_upload(self, key, headers=None, params=None):
        return await super().init_multipart_upload(key, headers, params)

    async def upload_part(
        self, key, upload_id, part_number, data, progress_callback=None, headers=None
    ):
        result = super().upload_part(key, upload_id, part_number, data, progress_callback, headers)
        resp = await result.resp
        return PutObjectResult(resp)

    # TODO add more methods

    def __do_object(self, method, key, **kwargs):
        if not self.bucket_name:
            raise exceptions.ClientError("Bucket name should not be null or empty.")
        if not key:
            raise exceptions.ClientError("key should not be null or empty.")
        return self._do(method, self.bucket_name, key, **kwargs)
