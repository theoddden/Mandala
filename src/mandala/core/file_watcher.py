"""File watcher for directory and S3 bucket monitoring.

Supports:
- Local directory watching (polling-based to avoid watchdog dependency)
- S3 bucket watching (via boto3)
- Pattern matching (glob patterns)
- Debouncing to avoid processing the same file multiple times
"""

from __future__ import annotations

import asyncio
import fnmatch
import os
from pathlib import Path
from typing import Any, Callable

import structlog

log = structlog.get_logger(__name__)


class FileWatcher:
    """Simple file watcher for local directories and S3 buckets.

    Uses polling instead of inotify to avoid watchdog dependency.
    Debounces file events to avoid processing the same file multiple times.
    """

    def __init__(self, interval_seconds: float = 5.0) -> None:
        self._watchers: dict[str, asyncio.Task] = {}
        self._interval = interval_seconds
        self._running = False
        self._seen_files: dict[str, float] = {}  # path -> last seen timestamp

    def watch_directory(
        self,
        name: str,
        path: str,
        callback: Callable[[str], Any],
        pattern: str = "*",
    ) -> None:
        """Watch a local directory for new files matching a pattern."""

        async def _loop() -> None:
            while self._running:
                try:
                    await self._scan_directory(path, pattern, callback)
                except Exception as exc:
                    log.exception("file_watcher.error", watcher=name, error=str(exc))
                await asyncio.sleep(self._interval)

        self._watchers[name] = asyncio.create_task(_loop())
        log.info("file_watcher.started", name=name, path=path, pattern=pattern)

    async def _scan_directory(
        self,
        path: str,
        pattern: str,
        callback: Callable[[str], Any],
    ) -> None:
        """Scan directory for new files matching pattern."""
        dir_path = Path(path)
        if not dir_path.exists():
            log.warning("file_watcher.directory_not_found", path=path)
            return

        for file_path in dir_path.glob(pattern):
            if file_path.is_file():
                file_str = str(file_path)
                mtime = file_path.stat().st_mtime

                # Only process if file is new (not seen or modified)
                if file_str not in self._seen_files or mtime > self._seen_files[file_str]:
                    try:
                        await callback(file_str)
                        self._seen_files[file_str] = mtime
                        log.info("file_watcher.processed", file=file_str)
                    except Exception as exc:
                        log.exception("file_watcher.callback_error", file=file_str, error=str(exc))

    def watch_s3(
        self,
        name: str,
        bucket: str,
        callback: Callable[[str], Any],
        prefix: str = "",
        pattern: str = "*",
    ) -> None:
        """Watch an S3 bucket for new files matching a pattern.

        Note: Requires boto3 to be installed (optional dependency).
        """
        try:
            import boto3
        except ImportError:
            log.error("file_watcher.s3_not_available", watcher=name)
            return

        s3 = boto3.client("s3")

        async def _loop() -> None:
            while self._running:
                try:
                    await self._scan_s3(s3, bucket, prefix, pattern, callback)
                except Exception as exc:
                    log.exception("file_watcher.error", watcher=name, error=str(exc))
                await asyncio.sleep(self._interval)

        self._watchers[name] = asyncio.create_task(_loop())
        log.info("file_watcher.started", name=name, bucket=bucket, prefix=prefix, pattern=pattern)

    async def _scan_s3(
        self,
        s3: Any,
        bucket: str,
        prefix: str,
        pattern: str,
        callback: Callable[[str], Any],
    ) -> None:
        """Scan S3 bucket for new files matching pattern."""
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

        if "Contents" not in response:
            return

        for obj in response["Contents"]:
            key = obj["Key"]
            if fnmatch.fnmatch(key, pattern):
                last_modified = obj["LastModified"].timestamp()

                # Only process if file is new
                if key not in self._seen_files or last_modified > self._seen_files[key]:
                    try:
                        # Download to temp file for processing
                        import tempfile

                        with tempfile.NamedTemporaryFile(delete=False) as tmp:
                            s3.download_fileobj(bucket, key, tmp)
                            await callback(tmp.name)
                            os.unlink(tmp.name)
                        self._seen_files[key] = last_modified
                        log.info("file_watcher.processed", file=key)
                    except Exception as exc:
                        log.exception("file_watcher.callback_error", file=key, error=str(exc))

    async def start(self) -> None:
        """Start all file watchers."""
        self._running = True
        log.info("file_watcher.starting", count=len(self._watchers))

    async def stop(self) -> None:
        """Stop all file watchers."""
        self._running = False
        log.info("file_watcher.stopping")
        for task in self._watchers.values():
            task.cancel()
        await asyncio.gather(*self._watchers.values(), return_exceptions=True)
