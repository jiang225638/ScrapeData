"""
寻欢阁详情页数据采集模块（Playwright版）

功能：
  - 使用 Playwright 加载登录状态，渲染 JavaScript 页面
  - 支持从全局配置文件 config.yaml 加载地区/板块/参数
  - 支持按地区筛选列表页（点击省份标签过滤）
  - 提取详情页结构化数据：年龄、消费水平、服务项目、联系方式、详细地址等
  - 支持三种子命令：login / fetch / crawl
"""
import argparse
import csv
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, quote, unquote, urlencode, urljoin, urlparse, urlunparse

import yaml
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from detail_parser import DetailItem, DetailParser, load_config, get_region_by_name

# ---------------------------------------------------------------------------
# 默认常量
# ---------------------------------------------------------------------------

DEFAULT_URL = (
    "https://xhg20260430.xhg303.one/forum.php?"
    "mod=viewthread&tid=811063&fromguid=hot&extra=page%3D1&mobile=2"
)
DEFAULT_START_URL = "https://xhg20260430.xhg303.one/"
DEFAULT_CONFIG_PATH = Path(__file__).parent / "config.yaml"
AUTH_FILE = Path("auth_state.json")
OUTPUT_DIR = Path("output")
THREAD_LINK_RE = re.compile(r"forum\.php\?[^\"'#<>\s]*mod=viewthread[^\"'#<>\s]*tid=\d+", re.I)
PERMISSION_HINTS = (
    "\u60a8\u6ca1\u6709\u8db3\u591f\u7684\u6743\u9650\u67e5\u770b\u6b64\u9690\u85cf\u5185\u5bb9",
    "\u6ca1\u6709\u8db3\u591f\u7684\u6743\u9650",
    "\u8bf7\u3010\u767b\u5f55\u3011",
    "\u8bf7 [\u767b\u5f55]",
    "\u767b\u5f55\u540e\u67e5\u770b",
)

# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

def normalize_filename(value: str, fallback: str) -> str:
    """将字符串标准化为安全的文件名"""
    value = re.sub(r"[^\w.-]+", "_", value, flags=re.UNICODE).strip("_")
    return value or fallback


def tid_from_url(url: str) -> str:
    """从URL中提取帖子ID"""
    parsed = urlparse(url)
    tid = parse_qs(parsed.query).get("tid", [""])[0]
    return normalize_filename(tid, "page")


def read_urls(args: argparse.Namespace) -> list[str]:
    """从命令行参数或文件中读取URL列表"""
    urls: list[str] = []
    if args.url:
        urls.append(args.url)
    if args.urls_file:
        path = Path(args.urls_file)
        if not path.exists():
            raise FileNotFoundError(f"URL file does not exist: {path}")
        urls.extend(line.strip() for line in path.read_text(encoding="utf-8").splitlines())
    urls = [url for url in urls if url and not url.startswith("#")]
    if not urls:
        urls.append(DEFAULT_URL)
    return urls


def replace_page_number(url: str, page_number: int) -> str:
    """替换URL中的页码参数"""
    parsed = urlparse(url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    changed = False

    if "page" in query:
        query["page"] = [str(page_number)]
        changed = True

    if "extra" in query:
        extra = unquote(query["extra"][0])
        if re.search(r"(^|[?&])page=\d+", extra):
            extra = re.sub(r"(^|[?&])page=\d+", lambda m: f"{m.group(1)}page={page_number}", extra)
            query["extra"] = [extra]
            changed = True

    if not changed:
        query["page"] = [str(page_number)]

    new_query = urlencode(query, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def normalize_thread_url(base_url: str, href: str) -> str | None:
    """标准化帖子链接，提取纯净的viewthread URL"""
    absolute = urljoin(base_url, href.replace("&amp;", "&"))
    parsed = urlparse(absolute)
    query = parse_qs(parsed.query, keep_blank_values=True)

    if query.get("mod", [""])[0] != "viewthread":
        return None
    tid = query.get("tid", [""])[0]
    if not tid:
        return None

    clean_query = {
        "mod": ["viewthread"],
        "tid": [tid],
        "fromguid": query.get("fromguid", ["hot"]),
        "extra": query.get("extra", ["page=1"]),
        "mobile": query.get("mobile", ["2"]),
    }
    return urlunparse(parsed._replace(query=urlencode(clean_query, doseq=True), fragment=""))


def extract_thread_urls(page, current_url: str) -> list[str]:
    """从列表页提取所有帖子链接"""
    urls: list[str] = []
    seen = set()

    hrefs = page.locator("a[href*='mod=viewthread'][href*='tid=']").evaluate_all(
        "(links) => links.map((link) => link.href || link.getAttribute('href'))"
    )
    for href in hrefs:
        normalized = normalize_thread_url(current_url, href)
        if normalized and normalized not in seen:
            seen.add(normalized)
            urls.append(normalized)

    html = page.content()
    for match in THREAD_LINK_RE.findall(html):
        normalized = normalize_thread_url(current_url, match)
        if normalized and normalized not in seen:
            seen.add(normalized)
            urls.append(normalized)

    return urls


# ---------------------------------------------------------------------------
# 地区筛选
# ---------------------------------------------------------------------------

def select_region_on_page(page, region_name: str, timeout: int = 10000) -> bool:
    """
    在列表页上点击指定省份名称以筛选结果

    Args:
        page: Playwright page对象
        region_name: 省份名称（如"北京市"、"广东省"）
        timeout: 等待超时时间（毫秒）

    Returns:
        bool: 是否成功点击
    """
    try:
        page.wait_for_timeout(2000)
        # 首先尝试展开省份选择面板
        expand_button = page.locator("#psheng_button")
        if expand_button.count() > 0:
            expand_button.click()
            page.wait_for_timeout(500)

        # 查找并点击目标省份
        province_locator = page.get_by_text(region_name, exact=True).first
        if province_locator.count() == 0:
            province_locator = page.locator(f"text={region_name}").first

        if province_locator.count() > 0:
            province_locator.click()
            page.wait_for_timeout(1500)
            # 等待列表刷新
            page.wait_for_load_state("networkidle", timeout=timeout)
            return True

        print(f"  Warning: Could not find province '{region_name}' on page.")
        return False
    except Exception as exc:
        print(f"  Warning: Failed to select region '{region_name}': {exc}")
        return False


# ---------------------------------------------------------------------------
# 页面采集
# ---------------------------------------------------------------------------

def collect_page(page, url: str, timeout: int) -> dict:
    """
    采集单个详情页内容

    Args:
        page: Playwright page对象
        url: 目标URL
        timeout: 超时时间

    Returns:
        dict: 包含页面文本、HTML、元数据的结果字典
    """
    started = time.time()
    response_status = None

    try:
        response = page.goto(url, wait_until="networkidle", timeout=timeout)
        response_status = response.status if response else None
    except PlaywrightTimeoutError:
        page.goto(url, wait_until="domcontentloaded", timeout=timeout)

    title = page.title()
    html = page.content()
    text = page.locator("body").inner_text(timeout=timeout)
    permission_blocked = any(hint in text or hint in html for hint in PERMISSION_HINTS)

    return {
        "url": url,
        "tid": tid_from_url(url),
        "title": title,
        "status": response_status,
        "permission_blocked": permission_blocked,
        "text": text,
        "html": html,
        "elapsed_seconds": round(time.time() - started, 2),
    }


def write_result(result: dict, output_dir: Path, detail_item: Optional[DetailItem] = None) -> dict:
    """
    将采集结果写入文件（原始文本/HTML/JSON + 结构化详情数据）

    Args:
        result: collect_page 返回的结果字典
        output_dir: 输出目录
        detail_item: 解析后的详情数据结构（可选）

    Returns:
        dict: 写入结果摘要
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    tid = result["tid"]
    base = output_dir / tid

    text_path = base.with_suffix(".txt")
    html_path = base.with_suffix(".html")
    json_path = base.with_suffix(".json")

    text_path.write_text(result["text"], encoding="utf-8")
    html_path.write_text(result["html"], encoding="utf-8")

    serializable = {key: value for key, value in result.items() if key not in {"text", "html"}}
    json_path.write_text(
        json.dumps(serializable, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    row = {
        "tid": tid,
        "title": result["title"],
        "status": result["status"],
        "permission_blocked": result["permission_blocked"],
        "text_file": str(text_path),
        "html_file": str(html_path),
        "json_file": str(json_path),
        "url": result["url"],
    }

    # 写入结构化详情数据
    if detail_item:
        detail_path = base.with_name(f"{tid}_detail.json")
        detail_dict = detail_item.to_dict(include_raw=False)
        detail_path.write_text(
            json.dumps(detail_dict, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        row["detail_file"] = str(detail_path)
        # 合并详情字段到摘要行
        for key in ("age", "beauty_score", "price_range", "services",
                     "qq", "wechat", "phone", "address",
                     "region_province", "region_city"):
            row[f"detail_{key}"] = detail_dict.get(key, "")

    return row


def parse_detail_result(result: dict, config_path: Optional[Path] = None) -> Optional[DetailItem]:
    """
    从采集结果中解析结构化详情数据

    Args:
        result: collect_page 返回的结果字典
        config_path: 配置文件路径

    Returns:
        Optional[DetailItem]: 解析后的详情数据，解析失败返回None
    """
    crawl_time = datetime.now().isoformat()
    try:
        parser = DetailParser(config_path)
        item = parser.parse(
            text=result["text"],
            url=result["url"],
            tid=result["tid"],
            title=result["title"],
            crawl_time=crawl_time,
        )
        return item
    except Exception as exc:
        print(f"  Warning: Detail parsing failed for {result['url']}: {exc}")
        return None


# ---------------------------------------------------------------------------
# 登录状态保存
# ---------------------------------------------------------------------------

def save_login_state(args: argparse.Namespace) -> None:
    """手动登录并保存浏览器状态"""
    auth_file = Path(args.auth)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()
        page.goto(args.base_url, wait_until="domcontentloaded", timeout=args.timeout)

        print("Browser opened. Please log in with your member account manually.")
        print("After confirming the page is logged in, return here and press Enter.")
        input()

        context.storage_state(path=str(auth_file))
        browser.close()

    print(f"Saved login state: {auth_file.resolve()}")


# ---------------------------------------------------------------------------
# fetch 模式：抓取指定的详情页
# ---------------------------------------------------------------------------

def fetch_pages(args: argparse.Namespace) -> None:
    """抓取指定的URL列表并解析详情数据"""
    auth_file = Path(args.auth)
    if not auth_file.exists():
        raise FileNotFoundError(
            f"Login state file not found: {auth_file}. Run first: python xhg_scraper.py login"
        )

    urls = read_urls(args)
    output_dir = Path(args.output)
    config_path = Path(args.config) if args.config else None
    summary_rows = []
    detail_rows = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=args.headless)
        context = browser.new_context(storage_state=str(auth_file))
        page = context.new_page()

        for index, url in enumerate(urls, start=1):
            print(f"[{index}/{len(urls)}] Fetching: {url}")
            result = collect_page(page, url, args.timeout)

            # 解析详情数据
            detail_item = None
            if not result["permission_blocked"]:
                detail_item = parse_detail_result(result, config_path)
                if detail_item:
                    detail_rows.append(detail_item.to_dict(include_raw=False))
                    print(f"  Parsed: age={detail_item.age}, price={detail_item.price_range}")

            row = write_result(result, output_dir, detail_item)
            summary_rows.append(row)

            if row["permission_blocked"]:
                print("  Note: the returned page still contains a permission/login warning.")
            else:
                print(f"  Done: {row['text_file']}")

            if args.delay and index < len(urls):
                time.sleep(args.delay)

        browser.close()

    # 写入汇总CSV
    _write_summary_csv(output_dir, summary_rows, "summary.csv")

    # 写入详情数据CSV
    if detail_rows:
        _write_detail_csv(output_dir, detail_rows, "detail_data.csv")
        _write_detail_json(output_dir, detail_rows, "detail_data.json")

    _print_summary(summary_rows, detail_rows)


# ---------------------------------------------------------------------------
# crawl 模式：从列表页发现帖子链接并抓取详情
# ---------------------------------------------------------------------------

def crawl_and_fetch(args: argparse.Namespace) -> None:
    """爬取列表页发现帖子链接并抓取详情页数据"""
    auth_file = Path(args.auth)
    if not auth_file.exists():
        raise FileNotFoundError(
            f"Login state file not found: {auth_file}. Run first: python xhg_scraper.py login"
        )

    # 加载配置
    config_path = Path(args.config) if args.config else None
    base_url = DEFAULT_START_URL
    region_name = getattr(args, "region", None)

    if config_path and config_path.exists():
        config = load_config(str(config_path))
        base_url = config.get("base_url", DEFAULT_START_URL)
        # 根据配置确定起始URL
        if args.forum and args.forum in config.get("forums", {}):
            forum_config = config["forums"][args.forum]
            list_url = forum_config.get("list_url", "")
            if list_url:
                base_url = urljoin(config["base_url"], list_url)
                args.start_url = base_url

    output_dir = Path(args.output)
    discovered_file = output_dir / "discovered_urls.txt"
    start_url = args.start_url or base_url
    all_urls: list[str] = []
    seen = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=args.headless)
        context = browser.new_context(storage_state=str(auth_file))
        page = context.new_page()

        # 翻页获取列表页链接
        for page_number in range(1, args.pages + 1):
            list_url = replace_page_number(start_url, page_number)
            print(f"[list {page_number}/{args.pages}] Opening: {list_url}")
            page.goto(list_url, wait_until="networkidle", timeout=args.timeout)

            # 如果指定了地区，点击筛选
            if region_name and page_number == 1:
                if select_region_on_page(page, region_name, args.timeout):
                    print(f"  Region filter applied: {region_name}")
                else:
                    print(f"  Region filter failed, continuing with unfiltered list.")

            page.wait_for_timeout(1500)
            found = extract_thread_urls(page, list_url)
            print(f"  Found {len(found)} thread links on this page.")

            new_count = 0
            for url in found:
                if url not in seen:
                    seen.add(url)
                    all_urls.append(url)
                    new_count += 1
            print(f"  Added {new_count} new links. Total: {len(all_urls)}")

            if args.max_threads and len(all_urls) >= args.max_threads:
                all_urls = all_urls[: args.max_threads]
                break

            if args.delay and page_number < args.pages:
                time.sleep(args.delay)

        # 保存发现的URL列表
        output_dir.mkdir(parents=True, exist_ok=True)
        discovered_file.write_text("\n".join(all_urls), encoding="utf-8")
        print(f"Discovered URL file: {discovered_file.resolve()}")

        if not all_urls:
            browser.close()
            print("No thread links found. Try passing a forum/list page URL with --start-url.")
            return

        # 逐个抓取详情页
        summary_rows = []
        detail_rows = []
        for index, url in enumerate(all_urls, start=1):
            print(f"[thread {index}/{len(all_urls)}] Fetching: {url}")
            result = collect_page(page, url, args.timeout)

            # 解析详情数据
            detail_item = None
            if not result["permission_blocked"]:
                detail_item = parse_detail_result(result, config_path)
                if detail_item:
                    detail_rows.append(detail_item.to_dict(include_raw=False))
                    print(f"  Parsed: age={detail_item.age}, price={detail_item.price_range}, "
                          f"address={detail_item.address[:20] if detail_item.address else 'N/A'}")

            row = write_result(result, output_dir, detail_item)
            summary_rows.append(row)

            if row["permission_blocked"]:
                print("  Note: the returned page still contains a permission/login warning.")
            else:
                print(f"  Done: {row['text_file']}")

            if args.delay and index < len(all_urls):
                time.sleep(args.delay)

        browser.close()

    # 写入汇总CSV
    _write_summary_csv(output_dir, summary_rows, "summary.csv")

    # 写入详情数据
    if detail_rows:
        _write_detail_csv(output_dir, detail_rows, "detail_data.csv")
        _write_detail_json(output_dir, detail_rows, "detail_data.json")

    _print_summary(summary_rows, detail_rows)


# ---------------------------------------------------------------------------
# 输出辅助函数
# ---------------------------------------------------------------------------

def _write_summary_csv(output_dir: Path, rows: list[dict], filename: str) -> None:
    """写入采集汇总CSV文件"""
    if not rows:
        return
    summary_file = output_dir / filename
    with summary_file.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"Summary file: {summary_file.resolve()}")


def _write_detail_csv(output_dir: Path, rows: list[dict], filename: str) -> None:
    """写入详情数据CSV文件"""
    if not rows:
        return
    detail_file = output_dir / filename
    fieldnames = [k for k in rows[0].keys() if k != "raw_text"]
    with detail_file.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Detail CSV file: {detail_file.resolve()}")


def _write_detail_json(output_dir: Path, rows: list[dict], filename: str) -> None:
    """写入详情数据JSON文件"""
    if not rows:
        return
    detail_file = output_dir / filename
    # 移除 raw_text 字段
    clean_rows = [{k: v for k, v in row.items() if k != "raw_text"} for row in rows]
    detail_file.write_text(
        json.dumps(clean_rows, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"Detail JSON file: {detail_file.resolve()}")


def _print_summary(summary_rows: list[dict], detail_rows: list[dict]) -> None:
    """打印采集结果汇总"""
    print("=" * 50)
    print(f"采集完成: {len(summary_rows)} 个详情页")
    if detail_rows:
        fields_found = {
            "年龄": sum(1 for r in detail_rows if r.get("age")),
            "消费水平": sum(1 for r in detail_rows if r.get("price_range")),
            "服务项目": sum(1 for r in detail_rows if r.get("services")),
            "QQ": sum(1 for r in detail_rows if r.get("qq")),
            "微信": sum(1 for r in detail_rows if r.get("wechat")),
            "电话": sum(1 for r in detail_rows if r.get("phone")),
            "地址": sum(1 for r in detail_rows if r.get("address")),
        }
        print(f"解析到 {len(detail_rows)} 条结构化详情，字段覆盖率:")
        for field, count in fields_found.items():
            print(f"  {field}: {count}/{len(detail_rows)}")
    print("=" * 50)


# ---------------------------------------------------------------------------
# 显示配置信息
# ---------------------------------------------------------------------------

def show_config(args: argparse.Namespace) -> None:
    """显示当前配置信息（地区列表、板块列表等）"""
    config_path = Path(args.config) if args.config else DEFAULT_CONFIG_PATH

    if not config_path.exists():
        print(f"Config file not found: {config_path}")
        return

    config = load_config(str(config_path))

    print("=" * 50)
    print("寻欢阁爬虫 - 全局配置信息")
    print(f"Base URL: {config.get('base_url', 'N/A')}")
    print()

    print("--- 论坛板块 ---")
    for name, info in config.get("forums", {}).items():
        print(f"  {name}: fid={info.get('fid')}, url={info.get('list_url')}")

    print()
    print("--- 可选地区（共 {} 个）---".format(len(config.get("regions", []))))
    regions = config.get("regions", [])
    for i, region in enumerate(regions):
        print(f"  [{region.get('code'):<12}] {region.get('name')}", end="")
        if (i + 1) % 5 == 0:
            print()
    if len(regions) % 5 != 0:
        print()

    print()
    print("--- 详情提取字段 ---")
    for field in config.get("detail_fields", []):
        print(f"  {field.get('name')} → {field.get('key')}")
    print("=" * 50)


# ---------------------------------------------------------------------------
# CLI参数构建
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    """构建命令行参数解析器"""
    parser = argparse.ArgumentParser(
        description="寻欢阁详情页数据采集（Playwright + 全局配置）"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # ---- login 子命令 ----
    login = subparsers.add_parser("login", help="手动登录并保存浏览器状态")
    login.add_argument("--base-url", default="https://xhg20260430.xhg303.one/")
    login.add_argument("--auth", default=str(AUTH_FILE))
    login.add_argument("--timeout", type=int, default=60000)
    login.set_defaults(func=save_login_state)

    # ---- fetch 子命令 ----
    fetch = subparsers.add_parser("fetch", help="抓取指定详情页URL并解析结构化数据")
    fetch.add_argument("--url", help="单个帖子URL")
    fetch.add_argument("--urls-file", help="包含URL列表的文本文件（每行一个URL）")
    fetch.add_argument("--auth", default=str(AUTH_FILE))
    fetch.add_argument("--output", default=str(OUTPUT_DIR))
    fetch.add_argument("--timeout", type=int, default=60000)
    fetch.add_argument("--delay", type=float, default=1.5, help="请求间隔秒数")
    fetch.add_argument("--headless", action="store_true", help="无头模式运行浏览器")
    fetch.add_argument("--config", default=str(DEFAULT_CONFIG_PATH),
                       help="全局配置文件路径（默认: config.yaml）")
    fetch.set_defaults(func=fetch_pages)

    # ---- crawl 子命令 ----
    crawl = subparsers.add_parser("crawl", help="从列表页发现帖子链接并抓取详情数据")
    crawl.add_argument("--start-url", default=DEFAULT_START_URL,
                       help="论坛列表页起始URL")
    crawl.add_argument("--pages", type=int, default=1, help="要扫描的列表页数")
    crawl.add_argument("--max-threads", type=int, default=0,
                       help="最大采集帖子数（0=不限制）")
    crawl.add_argument("--auth", default=str(AUTH_FILE))
    crawl.add_argument("--output", default=str(OUTPUT_DIR))
    crawl.add_argument("--timeout", type=int, default=60000)
    crawl.add_argument("--delay", type=float, default=1.5,
                       help="页面请求间隔秒数")
    crawl.add_argument("--headless", action="store_true", help="无头模式运行浏览器")
    crawl.add_argument("--config", default=str(DEFAULT_CONFIG_PATH),
                       help="全局配置文件路径（默认: config.yaml）")
    crawl.add_argument("--region", default=None,
                       help="按省份筛选（如：'北京市'、'广东省'，需与config.yaml一致）")
    crawl.add_argument("--forum", default=None,
                       help="选择论坛板块（如：'最新信息'、'自荐认证'、'包养专区'）")
    crawl.set_defaults(func=crawl_and_fetch)

    # ---- config 子命令 ----
    config_cmd = subparsers.add_parser("config", help="显示全局配置信息")
    config_cmd.add_argument("--config", default=str(DEFAULT_CONFIG_PATH),
                            help="全局配置文件路径（默认: config.yaml）")
    config_cmd.set_defaults(func=show_config)

    return parser


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------

def main() -> int:
    """主入口函数"""
    parser = build_parser()
    args = parser.parse_args()
    try:
        args.func(args)
    except FileNotFoundError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
