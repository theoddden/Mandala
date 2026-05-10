"""CDC (Change Data Capture) infrastructure for database connectors.

Supports:
- Postgres logical replication (via psycopg3)
- MySQL binlog (via mysql-replication)
- Simple polling-based CDC as fallback
"""
from __future__ import annotations

import asyncio
from typing import Any, Callable

import structlog

log = structlog.get_logger(__name__)


class CDCConsumer:
    """Base class for CDC consumers.
    
    Watches database change logs and publishes events for changes.
    """
    
    def __init__(self, name: str, callback: Callable[[dict[str, Any]], None]) -> None:
        self.name = name
        self._callback = callback
        self._running = False
        self._task: asyncio.Task | None = None
    
    @abstractmethod
    async def _consume(self) -> None:
        """Database-specific consumption logic."""
        ...
    
    async def start(self) -> None:
        """Start CDC consumption."""
        self._running = True
        self._task = asyncio.create_task(self._consume())
        log.info("cdc.start", name=self.name)
    
    async def stop(self) -> None:
        """Stop CDC consumption."""
        self._running = False
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        log.info("cdc.stop", name=self.name)


class PostgresCDC(CDCConsumer):
    """Postgres logical replication CDC consumer.
    
    Uses psycopg3 logical replication to decode WAL changes.
    """
    
    def __init__(
        self,
        connection_string: str,
        slot_name: str,
        publication: str,
        callback: Callable[[dict[str, Any]], None],
    ) -> None:
        super().__init__("postgres_cdc", callback)
        self._connection_string = connection_string
        self._slot_name = slot_name
        self._publication = publication
    
    async def _consume(self) -> None:
        """Consume Postgres logical replication stream."""
        try:
            from psycopg import connect
            from psycopg.types import LogicalReplicationConnection
        except ImportError:
            log.error("cdc.postgres.psycopg_not_installed")
            return
        
        while self._running:
            try:
                conn = connect(
                    self._connection_string,
                    connection_factory=LogicalReplicationConnection,
                )
                cur = conn.cursor()
                
                cur.start_replication(
                    slot_name=self._slot_name,
                    decode=True,
                )
                
                while self._running:
                    msg = cur.read_message()
                    if msg:
                        await self._callback({"type": "change", "data": msg.data})
                
                cur.stop_replication()
                conn.close()
            except Exception as exc:
                log.exception("cdc.postgres.error", error=str(exc))
                await asyncio.sleep(5)


class MySQLCDC(CDCConsumer):
    """MySQL binlog CDC consumer.
    
    Uses mysql-replication to read binlog events.
    """
    
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        callback: Callable[[dict[str, Any]], None],
    ) -> None:
        super().__init__("mysql_cdc", callback)
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database
    
    async def _consume(self) -> None:
        """Consume MySQL binlog stream."""
        try:
            from pymysqlreplication import BinLogStreamReader
            from pymysqlreplication.row_event import (
                DeleteRowsEvent,
                UpdateRowsEvent,
                WriteRowsEvent,
            )
        except ImportError:
            log.error("cdc.mysql.mysql_replication_not_installed")
            return
        
        while self._running:
            try:
                stream = BinLogStreamReader(
                    connection_settings={
                        "host": self._host,
                        "port": self._port,
                        "user": self._user,
                        "passwd": self._password,
                    },
                    server_id=100,
                    blocking=True,
                    only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
                )
                
                for binlog_event in stream:
                    if not self._running:
                        break
                    
                    event_data = {
                        "type": "change",
                        "table": binlog_event.table,
                        "database": self._database,
                        "event_type": type(binlog_event).__name__,
                        "data": binlog_event.rows,
                    }
                    await self._callback(event_data)
                
            except Exception as exc:
                log.exception("cdc.mysql.error", error=str(exc))
                await asyncio.sleep(5)
