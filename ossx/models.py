from oss2 import __version__
from oss2 import models as _models
from oss2.models import *

from .select_response import AsyncSelectResponseAdapter


class SelectObjectResult(HeadObjectResult):
    def __init__(self, resp, progress_callback=None, crc_enabled=False):
        super(SelectObjectResult, self).__init__(resp)
        self.__crc_enabled = crc_enabled
        self.select_resp = AsyncSelectResponseAdapter(
            resp, progress_callback, None, enable_crc=self.__crc_enabled
        )

    def read(self):
        return self.select_resp.read()

    def close(self):
        return self.resp.close()

    def __aiter__(self):
        return self.select_resp.__aiter__()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


class GetSelectObjectMetaResult(HeadObjectResult):
    def __init__(self, resp):
        super(GetSelectObjectMetaResult, self).__init__(resp)
        self.select_resp = AsyncSelectResponseAdapter(resp, None, None, False)

    def __await__(self):
        async def await_result():
            async for _ in self.select_resp:
                pass

            self.csv_rows = self.select_resp.rows  # to be compatible with previous version.
            self.csv_splits = self.select_resp.splits  # to be compatible with previous version.
            self.rows = self.csv_rows
            self.splits = self.csv_splits

        return await_result().__await__()


def since(version):
    def decorator(cls):
        if __version__ >= version:
            return _models.__dict__[cls.__name__]
        return cls

    return decorator


@since("2.18.5")
class GetBucketArchiveDirectReadResult(RequestResult):
    """获取归档直读。
    :param bool enabled: Bucket是否开启归档直读
    """

    def __init__(self, resp):
        super(GetBucketArchiveDirectReadResult, self).__init__(resp)
        self.enabled = None


@since("2.18.5")
class BucketTlsVersion(object):
    """BucketTLS版本设置。
    :param bool tls_enabled: 是否为Bucket开启TLS版本设置。
    :param tls_version: TLS版本。
    """

    def __init__(self, tls_enabled=False, tls_version=None):
        self.tls_enabled = tls_enabled
        self.tls_version = tls_version


@since("2.18.5")
class HttpsConfigResult(RequestResult):
    """返回Bucket TLS版本信息。
    :param bool tls_enabled: bucket是否开启TLS版本设置。
    :param tls_version: TLS版本。
    """

    def __init__(self, resp):
        super(HttpsConfigResult, self).__init__(resp)
        self.tls_enabled = None
        self.tls_version = []


@since("2.18.5")
class DataRedundancyTransitionInfo(RequestResult):
    """冗余转换任务信息。
    :param str bucket: 存储桶名称。
    :param str task_id: 存储冗余转换任务的ID。
    :param str create_time: 存储冗余转换任务的创建时间。
    :param str start_time: 存储冗余转换任务的开始时间。任务处于Processing、Finished状态时，有该字段。
    :param str end_time: 存储冗余转换任务的结束时间。任务处于Finished状态时，有该字段。
    :param str transition_status: 存储冗余转换任务的状态。
    :param int estimated_remaining_time: 存储冗余转换任务的预计剩余耗时。单位为小时。任务处于Processing、Finished状态时，有该字段。
    :param int process_percentage: 存储冗余转换任务的进度百分比。取值范围：0-100。任务处于Processing、Finished状态时，有该字段。
    """

    def __init__(
        self,
        bucket=None,
        task_id=None,
        create_time=None,
        start_time=None,
        end_time=None,
        transition_status=None,
        estimated_remaining_time=None,
        process_percentage=None,
    ):
        self.bucket = bucket
        self.task_id = task_id
        self.create_time = create_time
        self.start_time = start_time
        self.end_time = end_time
        self.transition_status = transition_status
        self.estimated_remaining_time = estimated_remaining_time
        self.process_percentage = process_percentage


@since("2.18.5")
class DataRedundancyTransitionInfoResult(RequestResult):
    """冗余转换任务信息。
    :param str bucket: 存储桶名称。
    :param str task_id: 存储冗余转换任务的ID。
    :param str create_time: 存储冗余转换任务的创建时间。
    :param str start_time: 存储冗余转换任务的开始时间。任务处于Processing、Finished状态时，有该字段。
    :param str end_time: 存储冗余转换任务的结束时间。任务处于Finished状态时，有该字段。
    :param str transition_status: 存储冗余转换任务的状态。
    :param int estimated_remaining_time: 存储冗余转换任务的预计剩余耗时。单位为小时。任务处于Processing、Finished状态时，有该字段。
    :param int process_percentage: 存储冗余转换任务的进度百分比。取值范围：0-100。任务处于Processing、Finished状态时，有该字段。
    """

    def __init__(self, resp):
        super(DataRedundancyTransitionInfoResult, self).__init__(resp)
        self.bucket = None
        self.task_id = None
        self.create_time = None
        self.start_time = None
        self.end_time = None
        self.transition_status = None
        self.estimated_remaining_time = None
        self.process_percentage = None


@since("2.18.5")
class CreateDataRedundancyTransitionResult(RequestResult):
    """存储冗余转换任务的容器。
    :param str task_id: 存储冗余转换任务的ID
    """

    def __init__(self, resp):
        super(CreateDataRedundancyTransitionResult, self).__init__(resp)
        self.task_id = None


@since("2.18.5")
class ListUserDataRedundancyTransitionResult(RequestResult):
    """某个Bucket下所有的存储冗余转换任务。
    :param bool is_truncated: 罗列结果是否是截断的， true: 本地罗列结果并不完整, False: 所有清单配置项已经罗列完毕。
    :param str next_continuation_token: 下一个罗列操作携带的token
    :param list data_redundancy_transition: 冗余转换任务信息列表。元素类型为:class:`DataRedundancyTransitionInfo <oss2.models.DataRedundancyTransitionInfo>`。
    """

    def __init__(self, resp):
        super(ListUserDataRedundancyTransitionResult, self).__init__(resp)
        self.is_truncated = None
        self.next_continuation_token = None
        self.data_redundancy_transitions = []


@since("2.18.5")
class ListBucketDataRedundancyTransitionResult(RequestResult):
    """某个Bucket下所有的存储冗余转换任务。
    :param list data_redundancy_transition: 冗余转换任务信息列表。元素类型为:class:`DataRedundancyTransitionInfo <oss2.models.DataRedundancyTransitionInfo>`。
    """

    def __init__(self, resp):
        super(ListBucketDataRedundancyTransitionResult, self).__init__(resp)
        self.data_redundancy_transitions = []
