import hashlib
import logging
import os
from collections.abc import Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Annotated, Any

import aioboto3
import uvicorn
import valkey.asyncio as valkey
from botocore.config import Config
from fastapi import Depends, FastAPI, HTTPException, Request, UploadFile

logger = logging.getLogger(__name__)


def get_stream_name(name: str) -> str:
    return f"stream:{name}"


ENQUEUE_JOB = """
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end
redis.call('XADD', KEYS[1], '*', 'status', 'pending')
redis.call('XADD', KEYS[2], '*', 'key', ARGV[1])
return 1
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with AsyncExitStack() as stack:
        client = await stack.enter_async_context(
            valkey.Valkey(
                host=os.environ.get("VALKEY_HOST", "cache.default"),
                port=int(os.environ.get("VALKEY_PORT", "6379")),
                socket_timeout=None,
                decode_responses=True,
            )
        )
        app.state.valkey = client

        _enqueue = client.register_script(ENQUEUE_JOB)

        async def enqueue_job(name: str) -> bool:
            """Return True if we created the stream, False if it already existed."""
            return bool(await _enqueue(keys=[get_stream_name(name), "jobs"], args=[name]))

        app.state.enqueue_job = enqueue_job

        session = aioboto3.Session()
        app.state.s3 = await stack.enter_async_context(
            session.client("s3", config=Config(s3={"addressing_style": "path"}))
        )
        app.state.bucket = os.environ.get("S3_BUCKET", "pdfs")

        yield


def get_valkey(request: Request) -> valkey.Valkey:
    return request.app.state.valkey


Valkey = Annotated[valkey.Valkey, Depends(get_valkey)]


def get_enqueue_job(request: Request) -> Callable[[str], Awaitable[bool]]:
    return request.app.state.enqueue_job


EnqueueJob = Annotated[Callable[[str], Awaitable[bool]], Depends(get_enqueue_job)]


def get_s3(request: Request) -> Any:
    return request.app.state.s3


S3 = Annotated[Any, Depends(get_s3)]


def get_bucket(request: Request) -> str:
    return request.app.state.bucket


Bucket = Annotated[str, Depends(get_bucket)]

app = FastAPI(lifespan=lifespan)


@app.post("/queue-extraction-job")
async def queue_extraction_job(
    file: UploadFile,
    client: Valkey,
    enqueue_job: EnqueueJob,
    s3: S3,
    bucket: Bucket,
):
    digest = hashlib.sha256()
    while chunk := await file.read(1024 * 1024):
        digest.update(chunk)

    key = digest.hexdigest()

    logger.debug("starting to upload PDF")
    await file.seek(0)
    await s3.upload_fileobj(file.file, bucket, key)
    logger.debug("finished uploading PDF")

    await enqueue_job(name=key)

    crt_id = "0-0"
    while True:
        response = await client.xread(
            streams={get_stream_name(key): crt_id},
            block=5000,
        )
        entries = response[0][1] if response else []
        for entry_id, fields in entries:
            if fields["status"] == "done":
                return {"result": fields["result"]}
            if fields["status"] == "aborted":
                raise HTTPException(status_code=409, detail=fields["reason"])
            crt_id = entry_id


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper())
    uvicorn.run(
        app,
        host=os.environ.get("HOST", "0.0.0.0"),
        port=int(os.environ.get("PORT", "8000")),
        proxy_headers=True,
        forwarded_allow_ips="*",
        timeout_graceful_shutdown=30,
    )


if __name__ == "__main__":
    main()
