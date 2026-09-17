from collections.abc import Awaitable, Callable

import valkey.asyncio as avalkey

from api.utils import get_stream_name

_ENQUEUE_JOB = """
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end
redis.call('XADD', KEYS[1], '*', 'status', 'pending')
redis.call('XADD', KEYS[2], '*', 'key', ARGV[1])
return 1
"""


def register_enqueue_job(client: avalkey.Valkey) -> Callable[[str], Awaitable[bool]]:
    if not isinstance(client, avalkey.Valkey):
        cls = type(client)
        raise TypeError(
            f"expected valkey.asyncio.Valkey, got {cls.__module__}.{cls.__qualname__}"
        )

    _enqueue = client.register_script(_ENQUEUE_JOB)

    async def enqueue_job(name: str) -> bool:
        """Return True if we created the stream, False if it already existed."""
        result = await _enqueue(keys=[get_stream_name(name), "jobs"], args=[name])
        if not isinstance(result, int):
            raise TypeError(
                f"script did not execute immediately (got {type(result).__name__}); "
            )
        return bool(result)

    return enqueue_job
