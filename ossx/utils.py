import os
from inspect import isawaitable, iscoroutinefunction
from io import BytesIO
from typing import IO, AsyncIterable, Union

from oss2.exceptions import InconsistentError
from oss2.utils import (
    _CHUNK_SIZE,
    ClientError,
    Crc64,
    _BytesAndFileAdapter,
    _FileLikeAdapter,
    _get_data_size,
    _has_data_size_attr,
    _invoke_cipher_callback,
    _invoke_crc_callback,
    _IterableAdapter,
    to_bytes,
)

_WINDOWS = os.name == "nt"
COPY_BUFSIZE = 1024 * 1024 if _WINDOWS else 64 * 1024


async def async_copyfileobj(
    fsrc,
    fdst,
    expected_len=None,
    request_id="",
    length=COPY_BUFSIZE,
):
    fsrc_read = fsrc.read
    fdst_write = fdst.write
    num_read = 0
    while True:
        buf = await fsrc_read(length)
        if not buf:
            break

        num_read += len(buf)
        await fdst_write(buf)

    if expected_len and num_read != expected_len:
        raise InconsistentError("IncompleteRead from source", request_id)


def make_crc_adapter(data, init_crc=0, discard=0):
    """返回一个适配器，从而在读取 `data` ，即调用read或者对其进行迭代的时候，能够计算CRC。

    :param discard:
    :return:
    :param data: 可以是bytes、file object或iterable
    :param init_crc: 初始CRC值，可选

    :return: 能够调用计算CRC函数的适配器
    """
    if isinstance(data, AwaitReadAdapter):
        data.crc_callback = Crc64(init_crc)
        data.discard = discard
        return data

    data = to_bytes(data)

    # bytes or file object
    if _has_data_size_attr(data):
        if discard:
            raise ClientError("Bytes of file object adapter does not support discard bytes")
        return _BytesAndFileAdapter(data, size=_get_data_size(data), crc_callback=Crc64(init_crc))
    elif hasattr(data, "read") and iscoroutinefunction(data.read):
        return AwaitReadAdapter(data, crc_callback=Crc64(init_crc), discard=discard)
    # file-like object
    elif hasattr(data, "read"):
        return _FileLikeAdapter(data, crc_callback=Crc64(init_crc), discard=discard)
    # iterator
    elif hasattr(data, "__iter__"):
        if discard:
            raise ClientError("Iterator adapter does not support discard bytes")
        return _IterableAdapter(data, crc_callback=Crc64(init_crc))
    else:
        raise ClientError(
            "{0} is not a file object, nor an iterator".format(data.__class__.__name__)
        )


def make_progress_adapter(data, progress_callback, size=None):
    """返回一个适配器，从而在读取 `data` ，即调用read或者对其进行迭代的时候，能够
     调用进度回调函数。当 `size` 没有指定，且无法确定时，上传回调函数返回的总字节数为None。

    :param data: 可以是bytes、file object或iterable
    :param progress_callback: 进度回调函数，参见 :ref:`progress_callback`
    :param size: 指定 `data` 的大小，可选

    :return: 能够调用进度回调函数的适配器
    """
    if isinstance(data, AwaitReadAdapter):
        data.progress_callback = progress_callback
        return data

    data = to_bytes(data)

    if size is None:
        size = _get_data_size(data)

    if size is None:
        if hasattr(data, "read") and iscoroutinefunction(data.read):
            return AwaitReadAdapter(data, progress_callback)
        if hasattr(data, "read"):
            return _FileLikeAdapter(data, progress_callback)
        elif hasattr(data, "__iter__"):
            return _IterableAdapter(data, progress_callback)
        else:
            raise ClientError(
                "{0} is not a file object, nor an iterator".format(data.__class__.__name__)
            )
    else:
        return _BytesAndFileAdapter(data, progress_callback, size)


def warp_async_data(data: Union[str, bytes, IO, AsyncIterable]):
    if isinstance(data, bytes):
        data = BytesIO(data)
    elif isinstance(data, str):
        data = BytesIO(data.encode("utf-8"))
    return AwaitReadAdapter(data)


async def _invoke_progress_callback(progress_callback, consumed_bytes, total_bytes):
    if progress_callback:
        if iscoroutinefunction(progress_callback):
            await progress_callback(consumed_bytes, total_bytes)
        else:
            progress_callback(consumed_bytes, total_bytes)


class AwaitReadAdapter(object):
    """通过这个适配器，可以给AwaitResponse加上进度监控。

    :param f: file-like object，只要支持async read / read即可
    :param progress_callback: 进度回调函数
    """

    def __init__(
        self,
        f,
        progress_callback=None,
        crc_callback=None,
        cipher_callback=None,
        discard=0,
    ):
        self.fileobj = f
        self.progress_callback = progress_callback
        self.offset = 0

        self.crc_callback = crc_callback
        self.cipher_callback = cipher_callback
        self.discard = discard
        self.read_all = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.next()

    async def next(self):
        if self.read_all:
            raise StopAsyncIteration

        content = await self.read(_CHUNK_SIZE)

        if content:
            return content
        else:
            raise StopAsyncIteration

    async def read(self, amt=None):
        offset_start = self.offset
        if offset_start < self.discard and amt and self.cipher_callback:
            amt += self.discard

        content = self.fileobj.read(amt)
        if isawaitable(content):
            content = await content
        if not content:
            self.read_all = True
            await _invoke_progress_callback(self.progress_callback, self.offset, None)
        else:
            await _invoke_progress_callback(self.progress_callback, self.offset, None)

            self.offset += len(content)

            real_discard = 0
            if offset_start < self.discard:
                if len(content) <= self.discard:
                    real_discard = len(content)
                else:
                    real_discard = self.discard

            _invoke_crc_callback(self.crc_callback, content, real_discard)
            content = _invoke_cipher_callback(self.cipher_callback, content, real_discard)

            self.discard -= real_discard
        return content

    @property
    def crc(self):
        if self.crc_callback:
            return self.crc_callback.crc
        elif self.fileobj:
            return self.fileobj.crc
        else:
            return None
