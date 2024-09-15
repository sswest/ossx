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


@since("2.19.0")
class CreateAccessPointRequest(RequestResult):
    """创建接入点请求信息。

    :param str access_point_name: 接入点名称。
    :param str network_origin: 网络类型。
    :param class vpc: vpc信息。元素类型为:class:`<oss2.models.AccessPointVpcConfiguration>`。
    """

    def __init__(self, access_point_name=None, network_origin=None, vpc=None):
        self.access_point_name = access_point_name
        self.network_origin = network_origin
        self.vpc = vpc


@since("2.19.0")
class CreateAccessPointResult(RequestResult):
    """创建接入点返回信息。

    :param str access_point_arn: 接入点arn。
    :param str alias: 别名。
    """

    def __init__(self, resp):
        super(CreateAccessPointResult, self).__init__(resp)
        self.access_point_arn = None
        self.alias = None


@since("2.19.0")
class AccessPointVpcConfiguration(RequestResult):
    """vpc信息。

    :param str vpc_id: vpc网络id。
    """

    def __init__(self, vpc_id=None):
        self.vpc_id = vpc_id


@since("2.19.0")
class PublicAccessBlockConfiguration(RequestResult):
    """保存阻止公共访问信息。

    :param bool block_public_access: 获取接入点的阻止公共访问配置信息。
    """

    def __init__(self, block_public_access=None):
        self.block_public_access = block_public_access


@since("2.19.0")
class GetAccessPointResult(RequestResult):
    """获取接入点返回信息。

    :param str access_point_name: 接入点名称。
    :param str bucket: bucket名称。
    :param str account_id: 账户id。
    :param str network_origin: 网络类型。
    :param class vpc: vpc信息。元素类型为:class:`<oss2.models.AccessPointVpcConfiguration>`。
    :param str access_point_arn: 接入点arn。
    :param str creation_date: 创建日期。
    :param str alias: 别名。
    :param str access_point_status: 状态。
    :param class endpoints: 接入点endpoint。元素类型为:class:`<oss2.models.AccessPointEndpoints>`。
    :param class public_access_block_configuration: 获取接入点的阻止公共访问配置。元素类型为:class:`<oss2.models.PublicAccessBlockConfiguration>`。
    """

    def __init__(self, resp):
        super(GetAccessPointResult, self).__init__(resp)
        self.access_point_name = None
        self.bucket = None
        self.account_id = None
        self.network_origin = None
        self.vpc = None
        self.access_point_arn = None
        self.creation_date = None
        self.alias = None
        self.access_point_status = None
        self.endpoints = None
        self.public_access_block_configuration = None


@since("2.19.0")
class AccessPointEndpoints(RequestResult):
    """接入点endpoint信息。

    :param str public_endpoint: 公共endpoint。
    :param str internal_endpoint: 内部endpoint。
    """

    def __init__(self, public_endpoint=None, internal_endpoint=None):
        self.public_endpoint = public_endpoint
        self.internal_endpoint = internal_endpoint


@since("2.19.0")
class GetAccessPointPolicyResult(RequestResult):
    """获取接入点策略信息。

    :param str access_point_policy: 接入点策略。
    """

    def __init__(self, resp):
        super(GetAccessPointPolicyResult, self).__init__(resp)
        self.access_point_policy = None


@since("2.19.0")
class GetAccessPointPolicyResult(RequestResult):
    def __init__(self, resp):
        RequestResult.__init__(self, resp)
        self.policy = to_string(resp.read())


@since("2.19.0")
class AccessPointInfo(RequestResult):
    """接入点信息。

    :param str access_point_name: 接入点名称。
    :param str bucket: bucket名称。
    :param str network_origin: 网络类型。
    :param class vpc: vpc信息。元素类型为:class:`<oss2.models.AccessPointVpcConfiguration>`。
    :param str alias: 别名。
    :param str status: 状态。
    """

    def __init__(
        self,
        access_point_name=None,
        bucket=None,
        network_origin=None,
        vpc=None,
        alias=None,
        status=None,
    ):
        self.access_point_name = access_point_name
        self.bucket = bucket
        self.network_origin = network_origin
        self.vpc = vpc
        self.alias = alias
        self.status = status


@since("2.19.0")
class ListAccessPointResult(RequestResult):
    """返回所有接入点信息。

    :param str account_id: 账户id。
    :param int max_keys: List时返回的最多的接入点的条数。
    :param bool is_truncated: 是否列举完所有的接入点。
    :param str next_continuation_token: 下一个罗列操作携带的token。
    :param str marker: 标记位。
    :param str access_points: 接入点集合。元素类型为:class:`<oss2.models.AccessPointInfo>`
    """

    def __init__(self, resp):
        super(ListAccessPointResult, self).__init__(resp)
        self.account_id = None
        self.max_keys = None
        self.is_truncated = None
        self.next_continuation_token = None
        self.marker = None
        self.access_points = []


@since("2.19.0")
class GetPublicAccessBlockResult(RequestResult):
    """保存阻止公共访问信息的容器。

    :param block_public_access: OSS全局阻止公共访问的配置信息。
    """
    def __init__(self, resp):
        super(GetPublicAccessBlockResult, self).__init__(resp)
        self.block_public_access = None


@since("2.19.0")
class GetBucketPublicAccessBlockResult(RequestResult):
    """保存bucket阻止公共访问信息的容器。

    :param block_public_access: Bucket的阻止公共访问配置信息。
    """
    def __init__(self, resp):
        super(GetBucketPublicAccessBlockResult, self).__init__(resp)
        self.block_public_access = None


@since("2.19.0")
class GetAccessPointPublicAccessBlockResult(RequestResult):
    """保存access point阻止公共访问信息的容器。

    :param block_public_access: 接入点的阻止公共访问配置信息。
    """
    def __init__(self, resp):
        super(GetAccessPointPublicAccessBlockResult, self).__init__(resp)
        self.block_public_access = None


@since("2.19.0")
class QoSConfiguration(object):
    """Qos信息

    :param total_upload_bw: 总上传带宽, 单位Gbps
    :type total_upload_bw: int

    :param intranet_upload_bw: 内网上传带宽, 单位Gbps
    :type intranet_upload_bw: int

    :param extranet_upload_bw: 外网上传带宽, 单位Gbps
    :type extranet_upload_bw: int

    :param total_download_bw: 总下载带宽, 单位Gbps
    :type total_download_bw: int

    :param intranet_download_bw: 内外下载带宽, 单位Gbps
    :type intranet_download_bw: int

    :param extranet_download_bw: 外网下载带宽, 单位Gbps
    :type extranet_download_bw: int

    :param total_qps: 总qps, 单位请求数/s
    :type total_qps: int

    :param intranet_qps: 内网访问qps, 单位请求数/s
    :type intranet_qps: int

    :param extranet_qps: 外网访问qps, 单位请求数/s
    :type extranet_qps: int
    """
    def __init__(self,
                 total_upload_bw = None,
                 intranet_upload_bw = None,
                 extranet_upload_bw = None,
                 total_download_bw = None,
                 intranet_download_bw = None,
                 extranet_download_bw = None,
                 total_qps = None,
                 intranet_qps = None,
                 extranet_qps = None):

        self.total_upload_bw = total_upload_bw
        self.intranet_upload_bw = intranet_upload_bw
        self.extranet_upload_bw = extranet_upload_bw
        self.total_download_bw = total_download_bw
        self.intranet_download_bw = intranet_download_bw
        self.extranet_download_bw = extranet_download_bw
        self.total_qps = total_qps
        self.intranet_qps = intranet_qps
        self.extranet_qps = extranet_qps


@since("2.19.0")
class RequesterQoSInfo(object):
    """流控配置信息。

    :param str requester: 请求者UID
    :param list qos_configuration: 流控配置信息。元素类型为:class:`QoSConfiguration <oss2.models.QoSConfiguration>`。
    """

    def __init__(self, requester=None, qos_configuration=None):

        self.requester = requester
        self.qos_configuration = qos_configuration


@since("2.19.0")
class RequesterQoSInfoResult(RequestResult):
    """流控配置。

    :param str requester: 请求者UID
    :param list qos_configuration: 流控配置信息。元素类型为:class:`QoSConfiguration <oss2.models.QoSConfiguration>`。
    """

    def __init__(self, resp):
        super(RequesterQoSInfoResult, self).__init__(resp)
        self.requester = None
        self.qos_configuration = None


@since("2.19.0")
class ResourcePoolInfoResult(RequestResult):
    """资源池的基本信息。

    :param str region: 资源池所属的地域
    :param str name: 资源池的名称
    :param str owner: 资源池所属的用户
    :param str create_time: 资源池创建的时间
    :param list qos_configuration: 流控配置信息。元素类型为:class:`QoSConfiguration <oss2.models.QoSConfiguration>`。
    """

    def __init__(self, resp):
        super(ResourcePoolInfoResult, self).__init__(resp)
        self.region = None
        self.name = None
        self.owner = None
        self.create_time = None
        self.qos_configuration = None


@since("2.19.0")
class ResourcePoolInfo(RequestResult):
    """资源池的简单信息。

    :param str name: 资源池的名称
    :param str create_time: 资源池创建的时间
    """

    def __init__(self, name=None, create_time=None):
        self.name = name
        self.create_time = create_time


@since("2.19.0")
class ListResourcePoolsResult(RequestResult):
    """资源池集合。

    :param str region: 资源池所属的地域
    :param str owner: 资源池所属的用户
    :param str continuation_token: 本次列举使用的ContinuationToken
    :param str next_continuation_token: 下次列举请求的ContinuationToken
    :param bool is_truncated: 本次返回结果是否截断
    :param list resource_pool: 资源池信息。元素类型为:class:`ResourcePoolInfo <oss2.models.ResourcePoolInfo>`。
    """

    def __init__(self, resp):
        super(ListResourcePoolsResult, self).__init__(resp)
        self.region = None
        self.owner = None
        self.continuation_token = ''
        self.next_continuation_token = ''
        self.is_truncated = False
        self.resource_pool = []


@since("2.19.0")
class ResourcePoolBucketInfo(RequestResult):
    """资源池的基本信息。

    :param str name: Bucket的名称
    :param str join_time: Bucket加入资源池的时间，ISO8601格式
    """

    def __init__(self, name=None, join_time=None):
        self.name = name
        self.join_time = join_time


@since("2.19.0")
class ListResourcePoolBucketsResult(RequestResult):
    """资源池中的Bucket列表。

    :param str resource_pool: 目标资源池名称
    :param str continuation_token: 本次列举使用的ContinuationToken
    :param str next_continuation_token: 下次列举请求的ContinuationToken
    :param bool is_truncated: 本次返回结果是否截断
    :param list resource_pool_buckets: 资源池中Bucket的信息。元素类型为:class:`ResourcePoolBucketInfo <oss2.models.ResourcePoolBucketInfo>`。
    """

    def __init__(self, resp):
        super(ListResourcePoolBucketsResult, self).__init__(resp)
        self.resource_pool = None
        self.continuation_token = ''
        self.next_continuation_token = ''
        self.is_truncated = False
        self.resource_pool_buckets = []


@since("2.19.0")
class ListResourcePoolRequesterQoSInfosResult(RequestResult):
    """资源池的请求者流控配置信息。

    :param str resource_pool: 目标资源池名称
    :param str continuation_token: 本次列举使用的ContinuationToken
    :param str next_continuation_token: 下次列举请求的ContinuationToken
    :param bool is_truncated: 本次返回结果是否截断
    :param list requester_qos_info: 请求者流控配置信息。元素类型为:class:`RequesterQoSInfo <oss2.models.RequesterQoSInfo>`。
    """

    def __init__(self, resp):
        super(ListResourcePoolRequesterQoSInfosResult, self).__init__(resp)
        self.resource_pool = None
        self.continuation_token = ''
        self.next_continuation_token = ''
        self.is_truncated = False
        self.requester_qos_info = []


@since("2.19.0")
class ListBucketRequesterQoSInfosResult(RequestResult):
    """bucket的请求者流控配置信息。

    :param str bucket: 请求者流控对应的Bucket名称
    :param str continuation_token: 本次列举使用的ContinuationTokenRequesterQoSInfo
    :param str next_continuation_token: 下次列举请求的ContinuationToken
    :param bool is_truncated: 本次返回结果是否截断
    :param list requester_qos_info: 请求者流控配置信息。元素类型为:class:`RequesterQoSInfo <oss2.models.RequesterQoSInfo>`。
    """

    def __init__(self, resp):
        super(ListBucketRequesterQoSInfosResult, self).__init__(resp)
        self.bucket = None
        self.continuation_token = ''
        self.next_continuation_token = ''
        self.is_truncated = False
        self.requester_qos_info = []