from collections.abc import Callable, Mapping

import valkey

from worker.utils import get_stream_name

_START_JOB = """
local p = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[3], ARGV[3], 1)
if #p == 0 or p[1][2] ~= ARGV[2] then
  return 0
end
redis.call('XADD', KEYS[2], '*', 'status', 'in-progress')
return 1
"""

_FINISH_JOB = """
local p = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[3], ARGV[3], 1)
if #p == 0 or p[1][2] ~= ARGV[2] then
  return 0
end
redis.call('XADD', KEYS[2], '*', unpack(ARGV, 4))
redis.call('XACK', KEYS[1], ARGV[1], ARGV[3])
return 1
"""

_REFRESH_CLAIMS = """
local p = redis.call('XPENDING', KEYS[1], ARGV[1], '-', '+', 100, ARGV[2])
local ids = {}
for i = 1, #p do
  ids[#ids + 1] = p[i][1]
end
if #ids == 0 then
  return 0
end
redis.call('XCLAIM', KEYS[1], ARGV[1], ARGV[2], 0, unpack(ids), 'JUSTID')
return #ids
"""


def register_start_job(
    client: valkey.Valkey,
) -> Callable[[str, str, str], bool]:
    if not isinstance(client, valkey.Valkey):
        cls = type(client)
        raise TypeError(
            f"expected valkey.Valkey, got {cls.__module__}.{cls.__qualname__}"
        )

    _start = client.register_script(_START_JOB)

    def start_job(key: str, consumer: str, job_id: str) -> bool:
        """Return True if we own the job and marked it in-progress, False otherwise."""
        result = _start(
            keys=["jobs", get_stream_name(key)],
            args=["job-consumers", consumer, job_id],
        )
        if not isinstance(result, int):
            raise TypeError(
                f"script did not execute immediately (got {type(result).__name__}); "
            )
        return bool(result)

    return start_job


def register_finish_job(
    client: valkey.Valkey,
) -> Callable[[str, str, str, Mapping[str, str]], bool]:
    if not isinstance(client, valkey.Valkey):
        cls = type(client)
        raise TypeError(
            f"expected valkey.Valkey, got {cls.__module__}.{cls.__qualname__}"
        )

    _finish = client.register_script(_FINISH_JOB)

    def finish_job(
        key: str, consumer: str, job_id: str, fields: Mapping[str, str]
    ) -> bool:
        """Return True if we owned the job, published the status and acked it."""
        flat = [x for kv in fields.items() for x in kv]
        result = _finish(
            keys=["jobs", get_stream_name(key)],
            args=["job-consumers", consumer, job_id, *flat],
        )
        if not isinstance(result, int):
            raise TypeError(
                f"script did not execute immediately (got {type(result).__name__}); "
            )
        return bool(result)

    return finish_job


def register_refresh_claims(
    client: valkey.Valkey,
) -> Callable[[str], int]:
    if not isinstance(client, valkey.Valkey):
        cls = type(client)
        raise TypeError(
            f"expected valkey.Valkey, got {cls.__module__}.{cls.__qualname__}"
        )

    _refresh = client.register_script(_REFRESH_CLAIMS)

    def refresh_claims(consumer: str) -> int:
        """Reset idle time on our pending entries. Returns how many we refreshed."""
        result = _refresh(keys=["jobs"], args=["job-consumers", consumer])
        if not isinstance(result, int):
            raise TypeError(
                f"script did not execute immediately (got {type(result).__name__}); "
            )
        return result

    return refresh_claims
