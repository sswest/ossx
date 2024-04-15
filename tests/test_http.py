import httpx
import pytest
from oss2 import CaseInsensitiveDict

from ossx import _http as http


def handler(request):
    return httpx.Response(200, content=b"test" * 10244)


class TestSession:
    @pytest.fixture
    def session(self):
        adapter = httpx.MockTransport(handler)

        return http.Session(adapter=adapter)

    @pytest.mark.asyncio
    async def test_do_request(self, session):
        req = http.Request("GET", "http://example.com")
        timeout = 5.0

        response = await session.do_request(req, timeout)

        assert response.status == 200
        assert await response.read(2) == b"te"
        assert await response.read(2) == b"st"
        assert await response.read(4) == b"test"
        assert await response.read(8) == b"test" * 2
        count = 10244 - 4
        assert len(await response.read()) == len(b"test") * count
        assert await response.read() == b""


def test_http_header():
    headers = CaseInsensitiveDict({"Oss-Test": "test"})
    headers2 = http.CaseInsensitiveDict(headers)
    assert headers2["oss-test"] == "test"
