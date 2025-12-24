import asyncio
from dataclasses import dataclass
from aiohttp import ClientSession
import logging
import sys
from types import SimpleNamespace

from aiohttp import ClientSession, TraceConfig, TraceRequestStartParams
from aiohttp_retry import RetryClient, JitterRetry
from dataclasses_json import dataclass_json
import uuid

import sqlite3


handler = logging.StreamHandler(sys.stdout)
logging.basicConfig(handlers=[handler])
logger = logging.getLogger(__name__)
retry_options = JitterRetry(attempts=100,max_timeout=120)

async def on_request_start(
    session: ClientSession,
    trace_config_ctx: SimpleNamespace,
    params: TraceRequestStartParams,
) -> None:
    current_attempt = trace_config_ctx.trace_request_ctx['current_attempt']
    if(current_attempt > 1):
        logger.warning(params)
    if retry_options.attempts <= current_attempt:
        logger.warning('Wow! We are in last attempt')

@dataclass_json    
@dataclass
class PageInfo:
    url: str
    uuid: uuid.UUID

@dataclass_json    
@dataclass    
class MetadataFile:
    pages : list[PageInfo]

class PageStore:
    con : sqlite3.Connection
    def __init__(self, file: str = "rawhtml.db") -> None:
        self.con = sqlite3.connect(file)
        self.con.execute('''
            CREATE TABLE IF NOT EXISTS pages (
                url TEXT PRIMARY KEY,
                content TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            ''')
        self.con.commit()

    def page_exists(self, url: str) -> bool:
        cursor = self.con.execute("SELECT 1 FROM pages WHERE url=?", (url,))
        return cursor.fetchone() is not None
        

    
    def save_page(self, url: str, content: str) -> None:
        self.con.execute("INSERT OR REPLACE INTO pages (url, content, timestamp) VALUES (?, ?, CURRENT_TIMESTAMP)", (url, content))
        self.con.commit()                
    
    def get_page_content(self, url: str) -> str | None:
        cursor = self.con.execute("SELECT content FROM pages WHERE url=?", (url,))
        row = cursor.fetchone()
        if row:
            return row[0]
        return None
class CachedPageLoader:
    page_cache : PageStore
    client : RetryClient
    qt_session : int = 0
    lock_qt_session : asyncio.Lock = asyncio.Lock()
    def __init__(self) -> None:
        self.page_cache = PageStore()
        trace_config = TraceConfig()
        trace_config.on_request_start.append(on_request_start)
        self.client = RetryClient(retry_options=retry_options, trace_configs=[trace_config])

    async def restart_session(self) -> None:
        async with self.lock_qt_session:
            await self.client.close()
            self.client = RetryClient(raise_for_status=False, retry_options=retry_options, trace_configs=[TraceConfig(on_request_start=[on_request_start])])
            self.qt_session += 1
            logger.warning(f"Restarted session, qt_session={self.qt_session}")
            
    async def get(self, url: str) -> tuple[bool, str]:
        if self.page_cache.page_exists(url):
            return (True, self.page_cache.get_page_content(url))
        async with self.client.get(url) as response:
            page = await response.text()
        self.page_cache.save_page(url, page)
        return (False, page)