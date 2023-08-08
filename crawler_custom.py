import asyncio
import html.parser
import pathlib
import time
import urllib.parse
import httpx
import re
import os

from typing import Callable, Iterable
from urllib.parse import urlparse

# CONFIG
WORKERS = os.cpu_count()  # depending on available computing power, from 1 to os.cpu_count()
LIMIT = 10000000  # maximum number of URLs to crawl
CRAWLER_LIMIT = 5000000  # maximum number of URLs cralwer can process
FILE = 'custom.txt'  # source file with domain/set of domains


###


class UrlFilterer:
    def __init__(
            self,
            allowed_domains: set[str] | None = None,
            allowed_schemes: set[str] | None = None,
            allowed_filetypes: set[str] | None = None,
            restricted_urls: set[str] | None = None
    ):
        self.allowed_domains = allowed_domains
        self.allowed_schemes = allowed_schemes
        self.allowed_filetypes = allowed_filetypes
        self.restricted_urls = [
            "web.archive.org", "plugins", ":8080", "moodle", "kalendarz",
            "password", "mobile", "query", "calendar",
            "ajax", "Zaloguj", "reddit.", "source=", "rozmiar=",
            "ssid=", "f_ov", "Facebook=", "cookies", "add", "cart", "comment",
            "reply", "en_US", "/login", "/logowanie", "producer_", "register",
            "orderby", "tumblr.", "redirect", "linkedin.", "facebook.",
            "instagram.", "youtube.", "twitter.", "whatsapp.", "pinterest.",
            "login.", "google.", "wykop.", "drukuj/", "pliki/"
        ]

    def filter_url(self, base: str, url: str) -> str | None:
        # base_count = url.count(base)
        # If the base URL is present more than once, remove it
        # if base_count > 1:
        # 	url = url.replace(base, '', 1)

        url = urllib.parse.urljoin(base, url)
        url, _ = urllib.parse.urldefrag(url)
        parsed = urllib.parse.urlparse(url)
        # segments = parsed.path.split('/')

        # # Remove duplicates
        # unique_segments = []
        # seen_segments = set()
        # for segment in segments:
        # 	if segment not in seen_segments:
        # 		unique_segments.append(segment)
        # 		seen_segments.add(segment)

        # # Reconstruct the URL
        # parsed = parsed._replace(path='/'.join(unique_segments))
        # url = urllib.parse.urlunparse(parsed)

        if (self.allowed_schemes is not None
                and parsed.scheme not in self.allowed_schemes):
            return None

        if any(substring in url for substring in self.restricted_urls):
            return None

        if self.allowed_domains is not None and self.allowed_domains not in url:
            return None

        login_patterns = ["login", "signin", "auth", "logon", "signon", "logowanie", "rejestracja"]

        if any(pattern in url.lower() for pattern in login_patterns):
            return None

        # Check for repeating path segments
        # path_segments = parsed.path.strip("/").split("/")
        # for i, segment in enumerate(path_segments[:-1]):
        # 	if segment == path_segments[i + 1]:
        # 		return None

        # Check for too many unique path segments
        # unique_segments = len(set(path_segments))
        # if unique_segments > 10:  # Adjust the threshold as needed
        # 	return None

        # if sum([parsed.path.count(char) + parsed.query.count(char) for char in "':;,.'"]) > 2:
        # 	return None

        # Check for long random strings (hashes)
        # if re.search(r'[a-zA-Z0-9_-]{20,}', url):
        # 	return None

        ext = pathlib.Path(parsed.path).suffix
        if (self.allowed_filetypes is not None
                and ext not in self.allowed_filetypes):
            return None
        return url


class UrlParser(html.parser.HTMLParser):
    def __init__(
            self,
            base: str,
            filter_url: Callable[[str, str], str | None]
    ):
        super().__init__()
        self.base = base
        self.filter_url = filter_url
        self.found_links = set()

    def handle_starttag(self, tag: str, attrs):

        if tag != "a":
            return

        for attr, url in attrs:
            if attr != "href":
                continue

            if (url := self.filter_url(self.base, url)) is not None:
                self.found_links.add(url)


class Crawler:
    def __init__(
            self,
            client: httpx.AsyncClient(),
            urls: Iterable[str],
            filter_url: Callable[[str, str], str | None],
            workers: int = WORKERS,
            limit: int = CRAWLER_LIMIT,
    ):
        self.client = client
        self.start_urls = set(urls)
        self.todo = asyncio.Queue()
        self.seen = set()
        self.done = set()

        self.filter_url = filter_url
        self.num_workers = workers
        self.limit = limit
        self.total = 0

    async def run(self):
        await self.on_found_links(self.start_urls)
        workers = [
            asyncio.create_task(self.worker())
            for _ in range(self.num_workers)
        ]
        await self.todo.join()

        for worker in workers:
            worker.cancel()

    async def worker(self):
        while True:
            try:
                await self.process_one()
            except asyncio.CancelledError as e:
                return

    async def process_one(self):
        url = await self.todo.get()
        try:
            await self.crawl(url)
        except Exception as exc:
            # retry handling here...
            pass
        finally:
            self.todo.task_done()

    async def crawl(self, url: str):

        # rate limit here...
        await asyncio.sleep(.1)

        response = await self.client.get(url, follow_redirects=True)
        found_links = await self.parse_links(
            base=str(response.url),
            text=response.text,
        )

        await self.on_found_links(found_links)

        self.done.add(url.lower())

    async def parse_links(self, base: str, text: str) -> set[str]:
        parser = UrlParser(base, self.filter_url)
        parser.feed(text)
        return parser.found_links

    async def on_found_links(self, urls: set[str]):
        new = urls - self.seen
        self.seen.update(new)

        # await save to database or file here...

        for i, url in enumerate(new):
            if len(url) > 256 or url.count(" "):
                pass
            else:
                print(i, url)
                await self.put_todo(url)

    async def put_todo(self, url: str):
        if self.total >= self.limit:
            return
        self.total += 1
        print("total: ", self.total)
        await self.todo.put(url)


async def main(url):
    url_domain = urllib.parse.urlparse(url).netloc
    url_domain = url_domain.replace("www.", "")
    print(url_domain)
    filterer = UrlFilterer(
        allowed_domains=url_domain,
        allowed_schemes={"http", "https"},
        allowed_filetypes={".html", ".htm", ".php", ".asp", ".aspx", ".jsp", ".cgi", ""}
    )

    start = time.perf_counter()

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/77.0.3865.120 Safari/537.36',
        "Accept-Encoding": "gzip, deflate",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive"}
    async with httpx.AsyncClient(headers=headers) as client:
        crawler = Crawler(
            client=client,
            urls=[url],
            filter_url=filterer.filter_url,
            workers=WORKERS,
            limit=LIMIT,
        )
        await crawler.run()

    end = time.perf_counter()
    seen = sorted(crawler.seen)
    print("Results:")

    # for url in seen:
    with open(url_domain + ".txt", 'a', encoding="utf-8") as f:
        # with open("custom/"+url_domain+".txt", 'a', encoding="utf-8") as f:
        f.write("\n".join(seen))

    # Print summary of crawling
    print(f"Crawled: {len(crawler.done)} URLs")
    print(f"Found: {len(seen)} URLs")
    print(f"Done in {end - start:.2f}s")


if __name__ == '__main__':
    with open(FILE, "r", encoding="utf-8") as f:
        urls = f.read().split("\n")
    for url in urls:
        asyncio.run(main(url), debug=True)
