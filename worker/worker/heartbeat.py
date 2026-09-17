import logging
import threading
from collections.abc import Callable

logger = logging.getLogger(__name__)


def start_heartbeat(
    refresh_claims: Callable[[str], int],
    consumer: str,
    interval_s: float,
) -> tuple[threading.Thread, threading.Event]:
    stop = threading.Event()

    def beat() -> None:
        while not stop.wait(interval_s):
            try:
                n = refresh_claims(consumer)
                if n:
                    logger.debug("refreshed %d claims", n)
            except Exception:
                logger.exception("heartbeat failed")

    thread = threading.Thread(target=beat, name="heartbeat", daemon=True)
    thread.start()
    return thread, stop
