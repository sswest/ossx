# ossx

[![Coverage Status](https://coveralls.io/repos/github/sswest/ossx/badge.svg?branch=main)](https://coveralls.io/github/sswest/ossx?branch=main)
[![PyPI version](https://badge.fury.io/py/ossx.svg)](https://badge.fury.io/py/ossx)

ossx is an Aliyun OSS SDK for Python Asyncio.

You are free to extend this project, but it's recommended to always run the unit tests to ensure the existing functionality works as expected.

## Implementation Details

ossx uses `httpx` as the asynchronous http client, and makes the greatest effort to reuse `oss2` code. Therefore, the code of ossx itself is very small, and in addition, ossx also adds type annotations for public methods.

From the user's perspective, the use of ossx and oss2 should be very similar, in most cases you only need to add an `await`. Only when streaming reading Object, you need to call `await obj.read()` or asynchronous iteration `async for chunk in obj`.

**Please note, this library will monkey patch a small number of functions in the oss2 library, usually we believe this will not affect the independent use of the oss2 library, for details please see `ossx/patch.py`.**

## Compatibility

Starting from version 2.18.4, ossx will synchronize with the version numbers of the official Aliyun SDK in subsequent releases. When releasing new versions, OSSX will strive to achieve forward compatibility as much as possible. Currently, we guarantee that a minor version of OSSX can be compatible with lower versions of oss2.

It is recommended to keep the version numbers of ossx and oss2 consistent to reduce compatibility issues.

## Getting Started

### Installation

```bash
pip install ossx
```

### Usage

```python
import asyncio
from oss2 import Auth, models
from ossx import AsyncBucket

async def main():
    bucket = AsyncBucket(
        auth=Auth('your-access-key-id', 'your-access-key-secret'),
        endpoint='oss-cn-beijing.aliyuncs.com',
        bucket_name='your-bucket-name'
    )
    content = b'Hello, ossx!'
    await bucket.put_object('your-object-key', content)
    obj = await bucket.get_object('your-object-key')
    assert isinstance(obj, models.GetObjectResult)
    assert await obj.read() == content
    assert await obj.read() == b''

if __name__ == '__main__':
    asyncio.run(main())
```

You can find the API use cases currently supported in the `tests` directory.
