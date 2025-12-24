import aiohttp
import asyncio
from bs4 import BeautifulSoup 
from dataclasses import dataclass
from aiohttp import ClientSession
import logging
import sys
from types import SimpleNamespace

from aiohttp import ClientSession, TraceConfig, TraceRequestStartParams
from aiohttp_retry import RetryClient, JitterRetry, Tuple
import csv
import os
import time
from dataclasses_json import dataclass_json
import random
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
        self.con.execute('''CREATE TABLE IF NOT EXISTS pages
                     (url TEXT PRIMARY KEY, content TEXT)''')
        self.con.commit()

    def page_exists(self, url: str) -> bool:
        cursor = self.con.execute("SELECT 1 FROM pages WHERE url=?", (url,))
        return cursor.fetchone() is not None
        

    
    def save_page(self, url: str, content: str) -> None:
        self.con.execute("INSERT OR REPLACE INTO pages (url, content) VALUES (?, ?)", (url, content))
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
    def __init__(self, client : RetryClient) -> None:
        self.page_cache = PageStore()
        self.client = client

    async def get(self, url: str) -> tuple[bool, str]:
        if self.page_cache.page_exists(url):
            return (True, self.page_cache.get_page_content(url))
        async with self.client.get(url) as response:
            page = await response.text()
        self.page_cache.save_page(url, page)
        return (False, page)