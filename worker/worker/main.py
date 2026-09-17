import logging
import os
import shutil
import tempfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import boto3
import pytesseract
import valkey
from botocore.config import Config
from pdf2image import convert_from_path, pdfinfo_from_path
from valkey.exceptions import ResponseError
from valkey.typing import EncodableT, StreamIdT

from worker.heartbeat import start_heartbeat
from worker.utils import get_stream_name
from worker.valkey_scripts import (
    register_finish_job,
    register_refresh_claims,
    register_start_job,
)

HEARTBEAT_MS = 2000
MAX_RESULT_BYTES = 1024 * 1024

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Context:
    client: valkey.Valkey
    start_job: Callable[[str, str, str], bool]
    finish_job: Callable[[str, str, str, Mapping[str, str]], bool]
    s3: Any
    bucket: str
    pod_name: str


def handle(ctx: Context, key: str) -> EncodableT:
    obj = ctx.s3.get_object(Bucket=ctx.bucket, Key=key)

    with tempfile.NamedTemporaryFile(suffix=".pdf") as tmp:
        shutil.copyfileobj(obj["Body"], tmp)
        tmp.flush()

        info = pdfinfo_from_path(tmp.name)
        page_count = info["Pages"]

        pages = []
        total = 0
        for n in range(1, page_count + 1):
            images = convert_from_path(tmp.name, dpi=200, first_page=n, last_page=n)
            text = pytesseract.image_to_string(images[0], lang="eng")
            images[0].close()

            total += len(text.encode())
            if total > MAX_RESULT_BYTES:
                logger.debug("truncating result for %s at page %d", key, n)
                break

            pages.append(text)
            logger.debug("ocr'd page %d/%d of %s", n, page_count, key)

    return "\n\n".join(pages)


def process_job(ctx: Context, key: str, job_id: StreamIdT):
    logger.debug(f"processing job ({key})")
    if not ctx.start_job(key=key, consumer=ctx.pod_name, job_id=job_id):
        logger.debug(f"lost claim on job {key}, skipping")
        return

    failure_reason = None
    try:
        result = handle(ctx, key)
    except Exception as e:
        logger.exception(f"handler failed for job ({job_id})")
        failure_reason = repr(e)

    if failure_reason is None:
        fields = {"status": "done", "result": result}
    else:
        fields = {"status": "aborted", "reason": failure_reason}

    ctx.finish_job(
        key=key,
        consumer=ctx.pod_name,
        job_id=job_id,
        fields=fields,
    )
    logger.debug(f"finished job ({key})")


def consume_jobs(ctx: Context):
    ctx.client.xautoclaim(
        name="jobs",
        groupname="job-consumers",
        consumername=ctx.pod_name,
        min_idle_time=HEARTBEAT_MS * 10,
        justid=True,
    )

    pending_jobs = ctx.client.xreadgroup(
        groupname="job-consumers",
        consumername=ctx.pod_name,
        streams={"jobs": 0},
    )
    for stream_name, entries in pending_jobs or []:
        for msg_id, fields in entries:
            process_job(ctx, key=fields["key"], job_id=msg_id)

    new_jobs = ctx.client.xreadgroup(
        groupname="job-consumers",
        consumername=ctx.pod_name,
        streams={"jobs": ">"},
        block=5000,
    )
    for stream_name, entries in new_jobs or []:
        for msg_id, fields in entries:
            key = fields["key"]
            ctx.client.xadd(name=get_stream_name(key), fields={"status": "assigned"})
            logger.debug(f"assigned job ({key})")


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper())
    client = valkey.Valkey(
        host=os.environ.get("VALKEY_HOST", "cache.default"),
        port=os.environ.get("VALKEY_PORT", "6379"),
        socket_timeout=None,
        decode_responses=True,
    )
    start_job = register_start_job(client)
    finish_job = register_finish_job(client)
    refresh_claims = register_refresh_claims(client)
    s3 = boto3.client("s3", config=Config(s3={"addressing_style": "path"}))
    bucket = os.environ.get("S3_BUCKET", "pdfs")
    pod_name = os.environ.get("POD_NAME")
    if not pod_name:
        raise RuntimeError("POD_NAME is not set")
    ctx = Context(
        client=client,
        start_job=start_job,
        finish_job=finish_job,
        s3=s3,
        bucket=bucket,
        pod_name=pod_name,
    )

    try:
        client.xgroup_create(
            name="jobs",
            groupname="job-consumers",
            id="0-0",
            mkstream=True,
        )
    except ResponseError as e:
        if not str(e).startswith("BUSYGROUP"):
            # raise an exception if the error is not related to the `xgroup`
            # already existing
            raise

    _, stop_heartbeat = start_heartbeat(
        refresh_claims,
        consumer=pod_name,
        interval_s=HEARTBEAT_MS / 1000,
    )

    try:
        while True:
            consume_jobs(ctx)
    finally:
        stop_heartbeat.set()


if __name__ == "__main__":
    main()
