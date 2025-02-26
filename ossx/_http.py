import asyncio
import base64
import logging
from typing import Any, AsyncIterator, Callable, Coroutine, Optional

import httpx
from aiofiles.threadpool.binary import AsyncBufferedReader
from httpx import AsyncBaseTransport
from oss2 import compat, defaults, exceptions, models, utils
from oss2.http import USER_AGENT

CaseInsensitiveDict = httpx.Headers

logger = logging.getLogger(__name__)


class Session:
    def __init__(
        self,
        pool_size: Optional[int] = None,
        adapter: Optional[AsyncBaseTransport] = None,
        timeout: float = defaults.connect_timeout,
        proxies: Optional[dict] = None,
    ):
        psize = pool_size or defaults.connection_pool_size
        limits = httpx.Limits(max_connections=psize, max_keepalive_connections=psize)
        if adapter is None:
            adapter = httpx.AsyncHTTPTransport(limits=limits)
        mounts = {"http://": adapter, "https://": adapter}
        if httpx.__version__ >= "0.26.0":
            self.session = httpx.AsyncClient(mounts=mounts, timeout=timeout, proxy=proxies)
        else:
            self.session = httpx.AsyncClient(mounts=mounts, timeout=timeout, proxies=proxies)

    def do_request(self, req: "Request", timeout: float):
        try:

            logger.debug(
                "Send request, method: {0}, url: {1}, params: {2}, headers: {3}, timeout: {4}".format(
                    req.method, req.url, req.params, req.headers, timeout
                )
            )
            request = self.session.build_request(
                method=req.method,
                url=req.url,
                data=req.data,
                params=req.params,
                headers=req.headers,
                timeout=timeout,
            )

            async def co():
                return await self.session.send(request=request, stream=True)

            return AwaitResponse(co)
        except httpx.HTTPError as err:
            raise exceptions.RequestError(err)


class Request(object):
    def __init__(
        self,
        method,
        url,
        data=None,
        params=None,
        headers=None,
        app_name="",
        proxies=None,
        region=None,
        product=None,
        cloudbox_id=None,
    ):
        self.method = method
        self.url = url
        self.data = _convert_request_body(data)
        self.params = params or {}
        self.proxies = proxies
        self.region = region
        self.product = product
        self.cloudbox_id = cloudbox_id

        if not isinstance(headers, httpx.Headers):
            self.headers = httpx.Headers(headers)
        else:
            self.headers = headers

        # not to add 'Accept-Encoding: gzip, deflate' by default
        if "Accept-Encoding" not in self.headers:
            self.headers["Accept-Encoding"] = "identity"

        if "User-Agent" not in self.headers:
            if app_name:
                self.headers["User-Agent"] = USER_AGENT + "/" + app_name
            else:
                self.headers["User-Agent"] = USER_AGENT

        logger.debug(
            "Init request, method: {0}, url: {1}, params: {2}, headers: {3}".format(
                method, url, params, headers
            )
        )


_CHUNK_SIZE = 8 * 1024


class AwaitResponse(object):
    def __init__(self, response: Callable[..., Coroutine[Any, Any, httpx.Response]]):
        self._co = response
        self.response = None
        self.status = 0
        self.headers = {}
        self.request_id = ""

        self.__all_read = False
        self.__iter = None
        self.chunker = bytearray()

    def __iter__(self):
        return self

    def __await__(self):
        return self._await().__await__()

    async def _await(self):
        if self.response is not None:
            return self

        self.response = await self._co()

        self.status = self.response.status_code
        self.headers = self.response.headers
        self.request_id = self.response.headers.get("x-oss-request-id", "")

        if self.status // 100 != 2:
            e = await make_exception(self)
            logger.info("Exception: {0}".format(e))
            raise e

        # FIXME: check here
        content_length = models._hget(self.headers, "content-length", int)
        if content_length is not None and content_length == 0:
            await self.read()

        logger.debug(
            "Get response headers, req-id:{0}, status: {1}, headers: {2}".format(
                self.request_id, self.status, self.headers
            )
        )
        return self

    async def read(self, amt: Optional[int] = None):
        if self.__all_read:
            return b""

        if amt is None:
            content_list = [bytes(self.chunker)]
            async for chunk in self._iter:
                content_list.append(chunk)
            content = b"".join(content_list)
            self.chunker = bytearray()
            self.__all_read = True
            return content
        else:
            while len(self.chunker) <= amt:
                try:
                    self.chunker.extend(await self._iter.__anext__())
                except StopAsyncIteration:
                    break

            chunk = bytes(self.chunker[:amt])
            del self.chunker[:amt]

            if not self.chunker:
                self.__all_read = True
            return chunk

    async def close(self):
        await self.response.aclose()

    @property
    def _iter(self):
        if self.__iter is None:
            self.__iter = self.__aiter__()
        return self.__iter

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self.response.aiter_bytes(_CHUNK_SIZE)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.response.aclose()

    def __del__(self):
        if self.response is not None:
            try:
                loop = asyncio.get_event_loop()
                loop.create_task(self.response.aclose())
            except RuntimeError:
                pass


def _convert_request_body(data):
    data = compat.to_bytes(data)

    if hasattr(data, "__len__"):
        return data

    if isinstance(data, AsyncBufferedReader):
        return data

    if hasattr(data, "seek") and hasattr(data, "tell"):
        return utils.SizedFileAdapter(data, utils.file_object_remaining_bytes(data))

    return data


async def make_exception(resp: AwaitResponse):
    status = resp.status
    headers = resp.headers
    body = await resp.read(4096)
    if not body and headers.get("x-oss-err") is not None:
        try:
            value = base64.b64decode(compat.to_string(headers.get("x-oss-err")))
        except:
            value = body
        details = exceptions._parse_error_body(value)
    else:
        details = exceptions._parse_error_body(body)
    code = details.get("Code", "")

    try:
        klass = exceptions._OSS_ERROR_TO_EXCEPTION[(status, code)]
        return klass(status, headers, body, details)
    except KeyError:
        return exceptions.ServerError(status, headers, body, details)
