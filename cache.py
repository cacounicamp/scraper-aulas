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
    pages : list[PageInfo]
    def __init__(self, folder: str = "rawhtml", metadata_file: str = "pagestore.json") -> None:
        self.folder = folder
        self.metadata_file = metadata_file
        self.pages = []
        os.makedirs(folder, exist_ok=True)
        self.load_metadata()
            
    def load_metadata(self) -> None:
        try: 
            with open(self.metadata_file, 'r') as f:
                value = MetadataFile.schema().loads(f.read())
                self.pages = value.pages
        except FileNotFoundError:
            self.pages = []
    
    def save_metadata(self) -> None:
        data = MetadataFile(self.pages)
        with open(self.metadata_file, 'w', newline='') as f:
            value = MetadataFile.schema().dumps(data)
            f.write(value)
  
    def _get_pageinfo(self, url: str) -> PageInfo | None:
        for page in self.pages:
            if(page.url == url):
                return page
        return None

    def page_exists(self, url: str) -> bool:
        return self._get_pageinfo(url) is not None
    
    def _path(self, pageinfo : PageInfo) -> str:
        return os.path.join(self.folder, f"{pageinfo.uuid}.html")
    
    def page_path(self, url: str) -> str | None:
        pageinfo = self._get_pageinfo(url)
        if(pageinfo is not None):
            return self._path(pageinfo)
        return None
    
    def save_page(self, url: str, content: str) -> str:
        pageinfo = self._get_pageinfo(url)
        if(pageinfo is not None):
            page_uuid = pageinfo.uuid
            
        page_uuid = uuid.uuid4()
        pageinfo = PageInfo(url, page_uuid)
        path = self._path(pageinfo)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)
        self.pages.append(pageinfo)
        self.save_metadata()
        
        return path
    
    def get_page_content(self, url: str) -> str | None:
        pageinfo = self._get_pageinfo(url)
        if(pageinfo is not None):
            path = self._path(pageinfo)
            with open(path, 'r', encoding='utf-8') as f:
                return f.read()
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
            print("Carregando do cache:", url)
            return (True, self.page_cache.get_page_content(url))
        async with self.client.get(url) as response:
            page = await response.text()
        self.page_cache.save_page(url, page)
        return (False, page)