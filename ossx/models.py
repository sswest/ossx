from oss2.models import HeadObjectResult

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
