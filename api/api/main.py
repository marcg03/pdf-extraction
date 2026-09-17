import hashlib
import logging
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from typing import Annotated, Any

import aioboto3
import uvicorn
import valkey.asyncio as valkey
from botocore.config import Config
from fastapi import Depends, FastAPI, HTTPException, Request, UploadFile

from api.utils import get_stream_name
from api.valkey_scripts import register_enqueue_job

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class AppState:
    valkey: valkey.Valkey
    enqueue_job: Callable[[str], Awaitable[bool]]
    s3: Any
    bucket: str


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    async with AsyncExitStack() as stack:
        client = await stack.enter_async_context(
            valkey.Valkey(
                host=os.environ.get("VALKEY_HOST", "cache.default"),
                port=int(os.environ.get("VALKEY_PORT", "6379")),
                socket_timeout=None,
                decode_responses=True,
            )
        )
        session = aioboto3.Session()
        s3 = await stack.enter_async_context(
            session.client("s3", config=Config(s3={"addressing_style": "path"}))
        )

        app.state.deps = AppState(
            valkey=client,
            enqueue_job=register_enqueue_job(client),
            s3=s3,
            bucket=os.environ.get("S3_BUCKET", "pdfs"),
        )
        yield


def get_state(request: Request) -> AppState:
    return request.app.state.deps


State = Annotated[AppState, Depends(get_state)]

app = FastAPI(lifespan=lifespan)


@app.post("/queue-extraction-job")
async def queue_extraction_job(file: UploadFile, state: State):
    digest = hashlib.sha256()
    while chunk := await file.read(1024 * 1024):
        digest.update(chunk)
    key = digest.hexdigest()

    logger.debug("starting to upload PDF")
    await file.seek(0)
    await state.s3.upload_fileobj(file.file, state.bucket, key)
    logger.debug("finished uploading PDF")

    await state.enqueue_job(key)

    crt_id = "0-0"
    while True:
        response = await state.valkey.xread(
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
