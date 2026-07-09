"""Launch Tracker entry point."""

from __future__ import annotations

import asyncio
import logging
import signal
import sys

from launch_tracker.config import Config
from launch_tracker.logging_setup import log_extra, setup_logging
from launch_tracker.service import LaunchTrackerService

logger = logging.getLogger(__name__)


async def run() -> None:
    config = Config.from_env()
    setup_logging(config.log_level)

    service = LaunchTrackerService(config)
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _signal_handler() -> None:
        log_extra(logger, logging.INFO, "Shutdown signal received", event="shutdown_signal")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    await service.start()

    try:
        await stop_event.wait()
    finally:
        await service.stop()


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
