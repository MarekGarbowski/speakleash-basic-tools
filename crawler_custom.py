import asyncio
import html.parser
import pathlib
import time
import urllib.parse
import httpx
import os
from typing import Callable, Iterable

# CONFIG
WORKERS = os.cpu_count()
LIMIT = 1000000
CRAWLER_LIMIT = 50000
FILE = 'custom.txt'


class UrlFilterer:
    def __init__(
            self,
            allowed_domains: set[str] | None = None,
            allowed_schemes: set[str] | None = None,
            allowed_filetypes: set[str] | None = None
    ):
        self.allowed_domains = allowed_domains
        self.allowed_schemes = allowed_schemes
        self.allowed_filetypes = allowed_filetypes
        self.restricted_urls = [
            "web.archive.org", "plugins", ":8080", "moodle", "kalendarz",
            "password", "mobile", "query", "calendar", "ajax", "Zaloguj",
            "reddit.", "source=", "rozmiar=", "ssid=", "f_ov", "Facebook=",
            "cookies", "add", "cart", "comment", "reply", "en_US", "/login",
            "/logowanie", "producer_", "register", "orderby", "tumblr.",
            "redirect", "linkedin.", "facebook.", "instagram.", "youtube.",
            "twitter.", "whatsapp.", "pinterest.", "login.", "google.",
            "wykop.", "drukuj/", "pliki/"
        ]

    def filter_url(self, base: str, filtered_link: str) -> str | None:
        filtered_link = urllib.parse.urljoin(base, filtered_link)
        filtered_link, _ = urllib.parse.urldefrag(filtered_link)
        parsed = urllib.parse.urlparse(filtered_link)

        if ((self.allowed_schemes is not None and parsed.scheme not in self.allowed_schemes)
                or any(substring in filtered_link for substring in self.restricted_urls)
                or any(pattern in filtered_link.lower() for pattern in
                       ["login", "signin", "auth", "logon", "signon", "logowanie", "rejestracja"])
                or (self.allowed_filetypes is not None and pathlib.Path(
                    parsed.path).suffix not in self.allowed_filetypes)):
            return None

        return filtered_link


class UrlParser(html.parser.HTMLParser):
    def __init__(self, base: str, filter_url: Callable[[str, str], str | None]):
        super().__init__()
        self.base = base
        self.filter_url = filter_url
        self.found_links = set()

    def handle_starttag(self, tag: str, attrs):
        if tag != "a":
            return
        for attr, link in attrs:
            if attr == "href" and (link := self.filter_url(self.base, link)) is not None:
                self.found_links.add(link)


class Crawler:
    def __init__(
            self,
            client: httpx.AsyncClient(),
            entry_urls: Iterable[str],
            filter_url: Callable[[str, str], str | None],
            workers: int = WORKERS,
            limit: int = LIMIT
    ):
        self.client = client
        self.start_urls = set(entry_urls)
        self.todo = asyncio.Queue()
        self.seen = set()
        self.done = set()
        self.filter_url = filter_url
        self.num_workers = workers
        self.limit = limit
        self.total = 0

    async def run(self):
        await self.on_found_links(self.start_urls)
        workers = [asyncio.create_task(self.worker()) for _ in range(self.num_workers)]
        await self.todo.join()
        for worker in workers:
            worker.cancel()

    async def worker(self):
        while True:
            try:
                await self.process_one()
            except asyncio.CancelledError:
                return

    async def process_one(self):
        page = await self.todo.get()
        try:
            await self.crawl(page)
        finally:
            self.todo.task_done()

    async def crawl(self, website: str):
        await asyncio.sleep(.1)
        response = await self.client.get(website, follow_redirects=True)
        found_links = await self.parse_links(base=str(response.url), text=response.text, )
        await self.on_found_links(found_links)
        self.done.add(website.lower())

    async def parse_links(self, base: str, text: str) -> set[str]:
        parser = UrlParser(base, self.filter_url)
        parser.feed(text)
        return parser.found_links

    async def on_found_links(self, found_links: set[str]):
        new = found_links - self.seen
        self.seen.update(new)
        for i, found_link in enumerate(new):
            if len(found_link) <= 256 and not found_link.count(" "):
                await self.put_todo(found_link)

    async def put_todo(self, item: str):
        if self.total < self.limit:
            self.total += 1
            await self.todo.put(item)


async def main(input_url):
    url_domain = urllib.parse.urlparse(input_url).netloc.replace("www.", "")
    filterer = UrlFilterer(
        allowed_domains=url_domain,
        allowed_schemes={"http", "https"},
        allowed_filetypes={".html", ".htm", ".php", ".asp", ".aspx", ".jsp", ".cgi", ""}
    )

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/77.0.3865.120 Safari/537.36',
        "Accept-Encoding": "gzip, deflate",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive"
    }
    async with httpx.AsyncClient(headers=headers) as client:
        crawler = Crawler(client=client, entry_urls=[input_url], filter_url=filterer.filter_url)
        await crawler.run()
        seen = sorted(crawler.seen)
        with open(f"{url_domain}.txt", 'a', encoding="utf-8") as data_file:
            data_file.write("\n".join(seen))

    print(f"Crawled: {len(crawler.done)} URLs")
    print(f"Found: {len(seen)} URLs")
    print(f"Done in {time.perf_counter():.2f}s")


if __name__ == '__main__':
    with open(FILE, "r", encoding="utf-8") as file:
        urls = file.read().split("\n")
    for url in urls:
        asyncio.run(main(url), debug=True)
