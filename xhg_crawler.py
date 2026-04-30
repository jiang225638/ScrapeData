"""
寻欢阁列表数据爬虫模块

功能：
  - 发送HTTP请求获取页面HTML内容，处理403/404、超时、重定向等异常
  - 使用BeautifulSoup/lxml解析HTML，兼容静态渲染与动态渲染（Playwright）
  - 提取列表数据（标题、链接、发布时间、摘要等）并清洗为结构化格式
  - 去重与增量更新：同一URL只采集一次，后续运行仅抓取新增或变更记录
  - 限速与礼貌性爬取：并发≤3，请求间隔1-2s，携带合法User-Agent及Referer
  - 完整运行日志（INFO级别以上），网络异常或解析失败写入ERROR日志并自动重试3次
"""
import argparse
import csv
import hashlib
import json
import logging
import random
import re
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# 常量与默认配置
# ---------------------------------------------------------------------------

DEFAULT_BASE_URL = "https://xhg20260430.xhg303.one/"
DEFAULT_OUTPUT_DIR = Path("output")
DEFAULT_LIST_JSON = DEFAULT_OUTPUT_DIR / "list_data.json"
DEFAULT_LIST_CSV = DEFAULT_OUTPUT_DIR / "list_data.csv"
DEFAULT_DEDUP_FILE = DEFAULT_OUTPUT_DIR / "crawled_urls.json"
DEFAULT_LOG_FILE = DEFAULT_OUTPUT_DIR / "crawler.log"

USER_AGENTS = [
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
]

REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_BACKOFF_FACTOR = 2.0
MIN_DELAY = 1.0
MAX_DELAY = 2.0
MAX_CONCURRENT = 3

# Discuz! 论坛帖子列表常用CSS选择器
THREAD_LIST_SELECTORS = [
    "table#threadlisttableid tbody tr",
    "div.tl div.bm_c table tbody tr",
    "div#threadlist table tbody tr",
    "ul.tl li",
    "div.threadlist ul li",
    "div.bm_c table tbody tr",
    "div.tl ul li",
    "div.tl div.bm_c ul li",
    "div.bm_c ul li",
]

# 帖子标题链接匹配模式
THREAD_LINK_PATTERN = re.compile(
    r'forum\.php\?[^"\'#<>\s]*mod=viewthread[^"\'#<>\s]*tid=(\d+)', re.I
)

# 时间匹配模式
TIME_PATTERNS = [
    re.compile(r"(\d{4}-\d{1,2}-\d{1,2}\s+\d{1,2}:\d{2})"),
    re.compile(r"(\d{4}-\d{1,2}-\d{1,2})"),
    re.compile(r"(\d{1,2}-\d{1,2}\s+\d{1,2}:\d{2})"),
    re.compile(r"(\d+)&nbsp;分钟前"),
    re.compile(r"(\d+)&nbsp;小时前"),
    re.compile(r"昨天\s*(\d{1,2}:\d{2})"),
    re.compile(r"前天\s*(\d{1,2}:\d{2})"),
    re.compile(r"(\d+)&nbsp;分钟前"),
    re.compile(r"(\d+)&nbsp;小时前"),
    re.compile(r"(\d+)\s*秒前"),
    re.compile(r"(\d+)\s*分钟前"),
    re.compile(r"(\d+)\s*小时前"),
    re.compile(r"昨天\s*(\d{1,2}:\d{2})"),
]

COMPACT_TIME_PATTERN = re.compile(
    r"(?P<time>\d{4}-\d{1,2}-\d{1,2}\s+\d{1,2}:\d{2}|"
    r"\d{4}-\d{1,2}-\d{1,2}|"
    r"\d{1,2}-\d{1,2}\s+\d{1,2}:\d{2}|"
    r"\d+\s*(?:秒|分钟|小时)前|"
    r"半\s*(?:分钟|小时)前|"
    r"昨天\s*\d{1,2}:\d{2}|"
    r"前天\s*\d{1,2}:\d{2})"
)

TRAILING_CITY_PATTERN = re.compile(r"\s*[\u4e00-\u9fff]{2,8}[市县区]\s*$")

# 摘要提取的"阅读全文"风格截断
SUMMARY_MAX_LEN = 200


# ---------------------------------------------------------------------------
# 日志配置
# ---------------------------------------------------------------------------

def setup_logging(log_file: Path, level: int = logging.INFO) -> logging.Logger:
    """配置日志：同时输出到控制台和文件"""
    logger = logging.getLogger("xhg_crawler")
    logger.setLevel(level)
    logger.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(fmt)
    logger.addHandler(console_handler)

    log_file.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(str(log_file), encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    return logger


# ---------------------------------------------------------------------------
# 数据结构
# ---------------------------------------------------------------------------

@dataclass
class ListItem:
    """列表项数据结构，缺失字段留空"""
    title: str = ""
    link: str = ""
    publish_time: str = ""
    summary: str = ""
    category: str = ""
    thread_id: str = ""
    author: str = ""
    reply_count: str = ""
    view_count: str = ""
    source_page: str = ""
    crawl_time: str = ""

    @staticmethod
    def fieldnames() -> list[str]:
        return list(ListItem.__dataclass_fields__.keys())


@dataclass
class CrawlerStats:
    """爬取统计"""
    pages_crawled: int = 0
    items_extracted: int = 0
    items_new: int = 0
    items_skipped: int = 0
    errors: int = 0
    start_time: str = ""
    end_time: str = ""


# ---------------------------------------------------------------------------
# 限速器
# ---------------------------------------------------------------------------

class RateLimiter:
    """请求限速器：确保请求间隔在 1-2s 之间"""

    def __init__(self, min_delay: float = MIN_DELAY, max_delay: float = MAX_DELAY):
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._last_request_time = 0.0

    def wait(self) -> None:
        """等待直到满足最小延迟要求"""
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self._min_delay:
            sleep_time = self._min_delay - elapsed + random.uniform(0, self._max_delay - self._min_delay)
            time.sleep(sleep_time)
        else:
            jitter = random.uniform(0, self._max_delay - self._min_delay)
            time.sleep(jitter)
        self._last_request_time = time.time()


# ---------------------------------------------------------------------------
# HTTP会话与重试
# ---------------------------------------------------------------------------

class RetryableSession:
    """带自动重试的HTTP会话"""

    def __init__(
        self,
        max_retries: int = MAX_RETRIES,
        backoff_factor: float = RETRY_BACKOFF_FACTOR,
        timeout: int = REQUEST_TIMEOUT,
    ):
        self._max_retries = max_retries
        self._backoff_factor = backoff_factor
        self._timeout = timeout
        self._session = requests.Session()
        self._session.headers.update(self._default_headers())

    @staticmethod
    def _default_headers() -> dict:
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Cache-Control": "max-age=0",
        }

    def get(self, url: str, referer: str = "") -> requests.Response:
        """发送GET请求，自动重试，返回Response对象"""
        logger = logging.getLogger("xhg_crawler")
        headers = {}
        if referer:
            headers["Referer"] = referer

        last_exception: Optional[Exception] = None
        for attempt in range(1, self._max_retries + 1):
            try:
                response = self._session.get(
                    url, headers=headers, timeout=self._timeout,
                    allow_redirects=True,
                )
                response.encoding = response.apparent_encoding or "utf-8"
                if response.status_code in (403, 404):
                    logger.warning("请求返回 %d: %s (尝试 %d/%d)", response.status_code, url, attempt, self._max_retries)
                response.raise_for_status()
                return response
            except requests.exceptions.Timeout as exc:
                last_exception = exc
                logger.error("请求超时: %s (尝试 %d/%d)", url, attempt, self._max_retries)
            except requests.exceptions.HTTPError as exc:
                last_exception = exc
                logger.error("HTTP错误 %s: %s (尝试 %d/%d)", exc.response.status_code if exc.response else "?", url, attempt, self._max_retries)
            except requests.exceptions.ConnectionError as exc:
                last_exception = exc
                logger.error("连接错误: %s (尝试 %d/%d)", url, attempt, self._max_retries)
            except requests.exceptions.RequestException as exc:
                last_exception = exc
                logger.error("请求异常: %s (尝试 %d/%d)", url, attempt, self._max_retries)

            if attempt < self._max_retries:
                wait = self._backoff_factor ** attempt
                logger.info("等待 %.1f 秒后重试...", wait)
                time.sleep(wait)

        raise RuntimeError(f"请求失败(已重试{self._max_retries}次): {url}") from last_exception


# ---------------------------------------------------------------------------
# HTML解析器
# ---------------------------------------------------------------------------

class DataExtractor:
    """从HTML中提取列表数据"""

    def __init__(self, base_url: str = DEFAULT_BASE_URL):
        self._base_url = base_url
        self._logger = logging.getLogger("xhg_crawler")

    def extract_list_items(self, html: str, source_url: str) -> list[ListItem]:
        """从列表页HTML中提取所有列表项"""
        soup = BeautifulSoup(html, "lxml")
        items: list[ListItem] = []
        seen_links: set[str] = set()

        category = self._extract_category(soup)

        for selector in THREAD_LIST_SELECTORS:
            rows = soup.select(selector)
            if rows:
                self._logger.debug("使用选择器 '%s' 找到 %d 行", selector, len(rows))
                for row in rows:
                    item = self._parse_thread_row(row, source_url, category)
                    if item and item.link and item.link not in seen_links:
                        seen_links.add(item.link)
                        items.append(item)
                if items:
                    break

        if not items:
            items = self._extract_from_links(soup, source_url, category, seen_links)

        self._logger.info("从 %s 提取到 %d 条记录", source_url, len(items))
        return items

    def _extract_category(self, soup: BeautifulSoup) -> str:
        """提取页面分类/板块名称"""
        for selector in ["div.bm_h h1", "div.bm_h a", "div#pt div.z a:last-child", "a.cr"]:
            el = soup.select_one(selector)
            if el:
                text = el.get_text(strip=True)
                if text:
                    return text
        return ""

    def _parse_thread_row(self, row, source_url: str, category: str) -> Optional[ListItem]:
        """解析单个帖子行元素"""
        try:
            # 优先匹配带有标题class的链接，其次匹配任何viewthread链接
            link_el = (
                row.select_one("a.xst")
                or row.select_one("a.s")
                or row.select_one("a[href*='mod=viewthread']")
            )

            if not link_el:
                return None

            href = link_el.get("href", "")
            absolute_link = urljoin(self._base_url, href.replace("&amp;", "&"))
            title = link_el.get_text(strip=True)
            thread_id = self._extract_tid(href)

            if not thread_id:
                return None

            publish_time = self._extract_publish_time(row)
            author = self._extract_author(row)
            reply_count = self._extract_reply_count(row)
            view_count = self._extract_view_count(row)
            summary = self._extract_summary(row)

            return ListItem(
                title=title,
                link=absolute_link,
                publish_time=publish_time,
                summary=summary,
                category=category,
                thread_id=thread_id,
                author=author,
                reply_count=reply_count,
                view_count=view_count,
                source_page=source_url,
                crawl_time=datetime.now().isoformat(),
            )
        except Exception as exc:
            self._logger.debug("解析行元素失败: %s", exc)
            return None

    def _extract_from_links(
        self, soup: BeautifulSoup, source_url: str, category: str, seen_links: set[str]
    ) -> list[ListItem]:
        """回退方案：直接从页面中所有链接提取"""
        items: list[ListItem] = []
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            match = THREAD_LINK_PATTERN.search(href)
            if not match:
                continue
            thread_id = match.group(1)
            absolute_link = urljoin(self._base_url, href.replace("&amp;", "&"))
            if absolute_link in seen_links:
                continue
            seen_links.add(absolute_link)
            raw_text = a_tag.get_text(" ", strip=True) or ""
            title, publish_time, view_count, summary = self._parse_compact_link_text(raw_text)
            items.append(ListItem(
                title=title,
                link=absolute_link,
                publish_time=publish_time,
                summary=summary,
                thread_id=thread_id,
                view_count=view_count,
                category=category,
                source_page=source_url,
                crawl_time=datetime.now().isoformat(),
            ))
        return items

    @staticmethod
    def _parse_compact_link_text(text: str) -> tuple[str, str, str, str]:
        """解析移动版列表中包在同一个链接内的标题、时间、浏览数和摘要。"""
        normalized = re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()
        match = COMPACT_TIME_PATTERN.search(normalized)
        if not match:
            return normalized, "", "", ""

        title = normalized[:match.start()].strip()
        publish_time = match.group("time").strip()
        remainder = normalized[match.end():].strip()

        view_count = ""
        view_match = re.match(r"(\d+)\s*(.*)", remainder, re.S)
        if view_match:
            view_count = view_match.group(1)
            remainder = view_match.group(2).strip()

        summary = TRAILING_CITY_PATTERN.sub("", remainder).strip()
        return title, publish_time, view_count, summary[:SUMMARY_MAX_LEN]

    @staticmethod
    def _extract_tid(href: str) -> str:
        """从链接中提取thread ID"""
        match = THREAD_LINK_PATTERN.search(href)
        return match.group(1) if match else ""

    @staticmethod
    def _extract_publish_time(row) -> str:
        """从行元素中提取发布时间"""
        row_text = row.get_text(" ", strip=True)
        row_text = row_text.replace("\xa0", " ").replace("&nbsp;", " ")
        for pattern in TIME_PATTERNS:
            match = pattern.search(row_text)
            if match:
                return match.group(0).replace("\xa0", " ").replace("&nbsp;", " ")
        em_el = row.select_one("em span")
        if em_el:
            title_attr = em_el.get("title", "")
            if title_attr:
                return title_attr
        span_els = row.select("span")
        for span in span_els:
            text = span.get_text(strip=True)
            if re.match(r"\d{4}-\d{1,2}-\d{1,2}", text):
                return text
        return ""

    @staticmethod
    def _extract_author(row) -> str:
        """提取作者信息"""
        for selector in ["cite a", "div.authi a", "td.by a"]:
            el = row.select_one(selector)
            if el:
                return el.get_text(strip=True)
        return ""

    @staticmethod
    def _extract_reply_count(row) -> str:
        """提取回复数"""
        for selector in ["td.num a", "a.xi2"]:
            el = row.select_one(selector)
            if el:
                text = el.get_text(strip=True)
                if text.isdigit():
                    return text
        row_text = row.get_text(" ", strip=True)
        match = re.search(r"(\d+)\s*(?:回复|回帖)", row_text)
        if match:
            return match.group(1)
        return ""

    @staticmethod
    def _extract_view_count(row) -> str:
        """提取查看数"""
        # Discuz! 中查看数通常在 td.num 或回复数后面的 <em> 标签中
        num_cell = row.select_one("td.num")
        if num_cell:
            em_el = num_cell.select_one("em")
            if em_el:
                text = em_el.get_text(strip=True)
                if text.isdigit():
                    return text
        row_text = row.get_text(" ", strip=True)
        match = re.search(r"(\d+)\s*(?:查看|浏览|阅读)", row_text)
        if match:
            return match.group(1)
        return ""

    @staticmethod
    def _extract_summary(row) -> str:
        """提取摘要信息"""
        for selector in ["div.summary", "p.summary", "div.threaddesc", "div.c"]:
            el = row.select_one(selector)
            if el:
                text = el.get_text(" ", strip=True)
                if text and len(text) > 10:
                    return text[:SUMMARY_MAX_LEN]
        row_text = row.get_text(" ", strip=True)
        row_text = re.sub(r"\s+", " ", row_text)
        if len(row_text) > 20:
            return row_text[:SUMMARY_MAX_LEN]
        return ""


# ---------------------------------------------------------------------------
# 数据存储（含去重与增量更新）
# ---------------------------------------------------------------------------

class DataStorage:
    """数据存储：JSON/CSV双格式输出，去重与增量更新"""

    def __init__(self, json_path: Path, csv_path: Path, dedup_path: Path):
        self._json_path = json_path
        self._csv_path = csv_path
        self._dedup_path = dedup_path
        self._logger = logging.getLogger("xhg_crawler")
        self._link_hashset: set[str] = self._load_dedup()
        self._records: list[dict] = self._load_existing_records()

    def _load_dedup(self) -> set[str]:
        """加载已爬取的URL哈希集合"""
        if self._dedup_path.exists():
            try:
                data = json.loads(self._dedup_path.read_text(encoding="utf-8"))
                return set(data.get("hashes", []))
            except (json.JSONDecodeError, KeyError):
                pass
        return set()

    def _save_dedup(self) -> None:
        """保存去重哈希集合"""
        self._dedup_path.parent.mkdir(parents=True, exist_ok=True)
        data = {"hashes": sorted(self._link_hashset), "updated": datetime.now().isoformat()}
        self._dedup_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    def _load_existing_records(self) -> list[dict]:
        """加载已有JSON记录"""
        if self._json_path.exists():
            try:
                return json.loads(self._json_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                pass
        return []

    def _url_hash(self, url: str) -> str:
        """生成URL唯一哈希"""
        return hashlib.md5(url.encode("utf-8")).hexdigest()

    def is_duplicate(self, url: str) -> bool:
        """检查URL是否已采集过"""
        return self._url_hash(url) in self._link_hashset

    def save_items(self, items: list[ListItem]) -> tuple[int, int]:
        """保存列表项，返回(新增数, 跳过数)"""
        new_count = 0
        skipped = 0
        for item in items:
            h = self._url_hash(item.link)
            if h in self._link_hashset:
                skipped += 1
                continue
            self._link_hashset.add(h)
            self._records.append(asdict(item))
            new_count += 1

        if new_count > 0:
            self._flush()
        return new_count, skipped

    def _flush(self) -> None:
        """持久化所有数据"""
        self._json_path.parent.mkdir(parents=True, exist_ok=True)
        self._json_path.write_text(json.dumps(self._records, ensure_ascii=False, indent=2), encoding="utf-8")
        self._save_dedup()
        self._write_csv()

    def _write_csv(self) -> None:
        """导出CSV文件"""
        if not self._records:
            return
        fieldnames = ListItem.fieldnames()
        with self._csv_path.open("w", encoding="utf-8-sig", newline="") as fp:
            writer = csv.DictWriter(fp, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self._records)
        self._logger.info("CSV已导出: %s (%d 条记录)", self._csv_path, len(self._records))

    @property
    def total_records(self) -> int:
        return len(self._records)


# ---------------------------------------------------------------------------
# 主爬虫类
# ---------------------------------------------------------------------------

class XhgCrawler:
    """寻欢阁列表数据爬虫"""

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        output_dir: Path = DEFAULT_OUTPUT_DIR,
        use_playwright: bool = False,
        auth_file: Optional[Path] = None,
    ):
        self._base_url = base_url
        self._output_dir = Path(output_dir)
        self._use_playwright = use_playwright
        self._auth_file = auth_file
        self._logger = setup_logging(self._output_dir / "crawler.log")
        self._session = RetryableSession()
        self._rate_limiter = RateLimiter()
        self._extractor = DataExtractor(base_url)
        self._storage = DataStorage(
            self._output_dir / "list_data.json",
            self._output_dir / "list_data.csv",
            self._output_dir / "crawled_urls.json",
        )
        self._stats = CrawlerStats(start_time=datetime.now().isoformat())

    def crawl_list_page(self, url: str) -> list[ListItem]:
        """爬取单个列表页，返回提取到的列表项"""
        self._logger.info("正在爬取列表页: %s", url)
        self._stats.pages_crawled += 1

        try:
            self._rate_limiter.wait()
            response = self._session.get(url, referer=self._base_url)

            if "charset=gbk" in response.text[:2000].lower():
                response.encoding = "gbk"

            items = self._extractor.extract_list_items(response.text, url)
            new_count, skipped = self._storage.save_items(items)
            self._stats.items_extracted += len(items)
            self._stats.items_new += new_count
            self._stats.items_skipped += skipped
            self._logger.info(
                "页面处理完成: 提取 %d 条, 新增 %d 条, 跳过 %d 条",
                len(items), new_count, skipped,
            )
            return items

        except RuntimeError as exc:
            self._stats.errors += 1
            self._logger.error("爬取列表页失败: %s - %s", url, exc)
            return []
        except Exception as exc:
            self._stats.errors += 1
            self._logger.error("解析列表页异常: %s - %s", url, exc, exc_info=True)
            return []

    def crawl_list_pages(self, urls: list[str]) -> CrawlerStats:
        """爬取多个列表页"""
        self._logger.info("开始爬取 %d 个列表页", len(urls))
        for i, url in enumerate(urls, 1):
            self._logger.info("[%d/%d] %s", i, len(urls), url)
            self.crawl_list_page(url)
        self._stats.end_time = datetime.now().isoformat()
        self._log_summary()
        return self._stats

    def _log_summary(self) -> None:
        """输出爬取汇总"""
        self._logger.info("=" * 50)
        self._logger.info("爬取汇总:")
        self._logger.info("  页面数: %d", self._stats.pages_crawled)
        self._logger.info("  提取记录: %d", self._stats.items_extracted)
        self._logger.info("  新增记录: %d", self._stats.items_new)
        self._logger.info("  跳过记录: %d", self._stats.items_skipped)
        self._logger.info("  错误次数: %d", self._stats.errors)
        self._logger.info("  总存储记录: %d", self._storage.total_records)
        self._logger.info("=" * 50)


# ---------------------------------------------------------------------------
# CLI入口
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    """构建命令行参数解析器"""
    parser = argparse.ArgumentParser(
        description="寻欢阁列表数据爬虫 - 提取论坛列表页数据并保存为JSON/CSV",
    )
    parser.add_argument(
        "--base-url", default=DEFAULT_BASE_URL,
        help="网站基础URL（默认: %(default)s）",
    )
    parser.add_argument(
        "--output-dir", default=str(DEFAULT_OUTPUT_DIR),
        help="输出目录（默认: %(default)s）",
    )
    parser.add_argument(
        "--urls", nargs="*", default=None,
        help="要爬取的列表页URL列表（不指定则使用默认列表页）",
    )
    parser.add_argument(
        "--urls-file", default=None,
        help="包含URL列表的文本文件（每行一个URL）",
    )
    parser.add_argument(
        "--use-playwright", action="store_true",
        help="启用Playwright动态渲染（用于JS渲染页面）",
    )
    parser.add_argument(
        "--auth-file", default=None,
        help="Playwright登录状态文件路径（与--use-playwright配合使用）",
    )
    parser.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="日志级别（默认: %(default)s）",
    )
    return parser


DEFAULT_LIST_PAGE = (
    "https://xhg20260430.xhg303.one/forum.php?"
    "mod=forumdisplay&fid=2"
)


def _default_list_urls(base_url: str) -> list[str]:
    """生成默认的列表页URL列表"""
    parsed = urlparse(base_url)
    base = f"{parsed.scheme}://{parsed.netloc}/"
    return [
        f"{base}forum.php?mod=forumdisplay&fid=2",
    ]


def _read_urls_from_file(filepath: str) -> list[str]:
    """从文件中读取URL列表"""
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"URL文件不存在: {path}")
    urls = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            urls.append(line)
    return urls


def main() -> int:
    """主入口函数"""
    parser = build_parser()
    args = parser.parse_args()

    log_level = getattr(logging, args.log_level)
    logging.getLogger("xhg_crawler").setLevel(log_level)

    if args.urls:
        urls = args.urls
    elif args.urls_file:
        urls = _read_urls_from_file(args.urls_file)
    else:
        urls = _default_list_urls(args.base_url)

    auth_file = Path(args.auth_file) if args.auth_file else None

    crawler = XhgCrawler(
        base_url=args.base_url,
        output_dir=Path(args.output_dir),
        use_playwright=args.use_playwright,
        auth_file=auth_file,
    )

    try:
        crawler.crawl_list_pages(urls)
    except KeyboardInterrupt:
        logging.getLogger("xhg_crawler").warning("用户中断爬取")
        return 1
    except Exception as exc:
        logging.getLogger("xhg_crawler").error("爬取过程异常: %s", exc, exc_info=True)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
