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

from detail_parser import DetailItem, DetailParser, load_config

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
        expand_button = page.locator("#psheng_button")
        if expand_button.count() > 0:
            expand_button.click()
            page.wait_for_timeout(500)

        province_locator = page.get_by_text(region_name, exact=True).first
        if province_locator.count() == 0:
            province_locator = page.locator(f"text={region_name}").first

        if province_locator.count() > 0:
            province_locator.click()
            page.wait_for_timeout(1500)
            page.wait_for_load_state("networkidle", timeout=timeout)
            return True

        print(f"  Warning: Could not find province '{region_name}' on page.")
        return False
    except Exception as exc:
        print(f"  Warning: Failed to select region '{region_name}': {exc}")
        return False


def select_city_on_page(page, city_name: str, timeout: int = 10000) -> bool:
    """
    选中省份后，进一步点击城市名称做二级筛选

    Args:
        page: Playwright page对象
        city_name: 城市名称（如"深圳市"、"广州市"）
        timeout: 等待超时时间（毫秒）

    Returns:
        bool: 是否成功点击
    """
    try:
        page.wait_for_timeout(1500)
        expand_button = page.locator("#pshi_button")
        if expand_button.count() > 0:
            expand_button.click()
            page.wait_for_timeout(500)

        city_locator = page.get_by_text(city_name, exact=True).first
        if city_locator.count() == 0:
            city_locator = page.locator(f"text={city_name}").first

        if city_locator.count() > 0:
            city_locator.click()
            page.wait_for_timeout(1500)
            page.wait_for_load_state("networkidle", timeout=timeout)
            return True

        print(f"  Warning: Could not find city '{city_name}' on page.")
        return False
    except Exception as exc:
        print(f"  Warning: Failed to select city '{city_name}': {exc}")
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
    构建单条结果摘要行（不再写入单独文件，仅收集结构化数据）

    Args:
        result: collect_page 返回的结果字典
        output_dir: 输出目录（未使用，保留兼容）
        detail_item: 解析后的详情数据结构（可选）

    Returns:
        dict: 结果摘要行
    """
    tid = result["tid"]
    row = {
        "tid": tid,
        "title": result["title"],
        "status": result["status"],
        "permission_blocked": result["permission_blocked"],
        "url": result["url"],
    }
    if detail_item:
        detail_dict = detail_item.to_dict(include_raw=False)
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
    output_base = Path(args.output)
    config_path = Path(args.config) if args.config else None
    summary_rows = []
    detail_rows = []
    run_started = datetime.now()

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

            row = write_result(result, output_base, detail_item)
            summary_rows.append(row)

            if row["permission_blocked"]:
                print("  Note: the returned page still contains a permission/login warning.")
            else:
                print(f"  Done: {index}/{len(urls)}")

            if args.delay and index < len(urls):
                time.sleep(args.delay)

        browser.close()

    run_ended = datetime.now()
    run_dir = _make_run_dir(output_base)
    _finalize_run(run_dir, summary_rows, detail_rows, {
        "source": "fetch",
        "url_count": len(urls),
        "started": run_started.isoformat(),
        "ended": run_ended.isoformat(),
    })
    _print_summary(summary_rows, detail_rows)
    print(f"  输出目录: {run_dir}")


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
    city_name = getattr(args, "city", None)

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

    output_base = Path(args.output)
    start_url = args.start_url or base_url
    all_urls: list[str] = []
    seen = set()
    run_started = datetime.now()

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

            if city_name and page_number == 1:
                if select_city_on_page(page, city_name, args.timeout):
                    print(f"  City filter applied: {city_name}")
                else:
                    print(f"  City filter failed, continuing...")

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

            row = write_result(result, output_base, detail_item)
            summary_rows.append(row)

            if row["permission_blocked"]:
                print("  Note: the returned page still contains a permission/login warning.")
            else:
                print(f"  Done: {row['tid']}")

            if args.delay and index < len(all_urls):
                time.sleep(args.delay)

        browser.close()

    run_ended = datetime.now()
    run_dir = _make_run_dir(output_base)
    _finalize_run(run_dir, summary_rows, detail_rows, {
        "region": region_name or "(不限)",
        "city": city_name or "(不限)",
        "forum": args.forum or "最新信息",
        "pages": args.pages,
        "max_threads": args.max_threads,
        "discovered_urls": len(all_urls),
        "started": run_started.isoformat(),
        "ended": run_ended.isoformat(),
    })
    _print_summary(summary_rows, detail_rows)
    print(f"  输出目录: {run_dir}")


# ---------------------------------------------------------------------------
# 输出辅助函数
# ---------------------------------------------------------------------------

def _make_run_dir(output_base: Path, prefix: str = "") -> Path:
    """
    在输出基础目录下创建以时间戳命名的子文件夹

    Args:
        output_base: 输出根目录（如 Path("output")）
        prefix: 可选前缀（如地区名）

    Returns:
        Path: 时间戳子目录路径
    """
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    name = f"{prefix}_{ts}" if prefix else ts
    run_dir = output_base / name
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def _write_summary_json(run_dir: Path, summary_rows: list[dict],
                        detail_rows: list[dict], meta: dict) -> None:
    """写入summary.json（采集汇总）"""
    summary = {
        "meta": meta,
        "total_pages": len(summary_rows),
        "parsed_details": len(detail_rows),
        "permission_blocked": sum(1 for r in summary_rows if r.get("permission_blocked")),
        "records": summary_rows,
    }
    path = run_dir / "summary.json"
    path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Summary: {path.resolve()}")


def _write_detail_csv(run_dir: Path, rows: list[dict]) -> None:
    """写入detail_data.csv"""
    if not rows:
        return
    path = run_dir / "detail_data.csv"
    fieldnames = [k for k in rows[0].keys() if k != "raw_text"]
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"Detail CSV: {path.resolve()}")


def _write_detail_json(run_dir: Path, rows: list[dict]) -> None:
    """写入detail_data.json（结构化详情汇总）"""
    if not rows:
        return
    path = run_dir / "detail_data.json"
    clean_rows = [{k: v for k, v in row.items() if k != "raw_text"} for row in rows]
    path.write_text(json.dumps(clean_rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Detail JSON: {path.resolve()}")


def _finalize_run(run_dir: Path, summary_rows: list[dict],
                  detail_rows: list[dict], meta: dict) -> None:
    """一次写入所有输出文件"""
    _write_summary_json(run_dir, summary_rows, detail_rows, meta)
    if detail_rows:
        _write_detail_csv(run_dir, detail_rows)
        _write_detail_json(run_dir, detail_rows)


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
# run 模式：从配置文件读取参数，一键执行全流程
# ---------------------------------------------------------------------------

def run_from_config(args: argparse.Namespace) -> None:
    """从config.yaml读取target配置，自动完成：选地区→翻列表→抓详情→解析数据"""
    auth_file = Path(args.auth)
    if not auth_file.exists():
        raise FileNotFoundError(
            f"Login state file not found: {auth_file}. Run first: python xhg_scraper.py login"
        )

    config_path = Path(args.config) if args.config else DEFAULT_CONFIG_PATH
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    config = load_config(str(config_path))
    target = config.get("target", {})
    base_url = config.get("base_url", DEFAULT_START_URL)

    # 从 target 节读取参数
    region_name = target.get("region", "").strip()
    city_name = target.get("city", "").strip()
    forum_name = target.get("forum", "").strip()
    pages = int(target.get("pages", 1))
    max_threads = int(target.get("max_threads", 0))
    # 从 crawl 节读取其他参数
    crawl_config = config.get("crawl", {})
    output_base = Path(args.output) if args.output else Path(crawl_config.get("output_dir", "output"))
    delay = float(getattr(args, "delay", None) or crawl_config.get("delay", 1.5))
    timeout = int(getattr(args, "timeout", None) or crawl_config.get("timeout", 60000))

    # 确定起始URL（根据板块）
    if forum_name in config.get("forums", {}):
        forum_cfg = config["forums"][forum_name]
        start_url = urljoin(base_url, forum_cfg.get("list_url", ""))
    else:
        start_url = urljoin(base_url, "forum.php?mod=forumdisplay&fid=2")

    # 打印任务信息
    location_parts = []
    if region_name:
        location_parts.append(region_name)
    if city_name:
        location_parts.append(city_name)
    location_str = " > ".join(location_parts) if location_parts else "(不限)"

    print("=" * 55)
    print("  寻欢阁爬虫 - 自动采集模式")
    print(f"  地区: {location_str}")
    print(f"  板块: {forum_name}")
    print(f"  翻页: {pages} 页")
    print(f"  上限: {'不限制' if max_threads == 0 else str(max_threads) + ' 条'}")
    print("=" * 55)

    # 阶段1：翻列表，收集帖子链接
    all_urls: list[str] = []
    seen = set()
    run_started = datetime.now()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=args.headless)
        context = browser.new_context(storage_state=str(auth_file))
        page = context.new_page()

        print("\n>>> 阶段1：扫描列表页，收集帖子链接...")
        for page_number in range(1, pages + 1):
            list_url = replace_page_number(start_url, page_number)
            print(f"[列表 {page_number}/{pages}] {list_url}")
            page.goto(list_url, wait_until="networkidle", timeout=timeout)

            # 省份筛选（仅第一页）
            if region_name and page_number == 1:
                if select_region_on_page(page, region_name, timeout):
                    print(f"  ✔ 已筛选省份: {region_name}")
                else:
                    print(f"  ⚠ 省份筛选失败，继续...")

            # 城市筛选（仅第一页，且省份筛选成功后）
            if city_name and page_number == 1:
                if select_city_on_page(page, city_name, timeout):
                    print(f"  ✔ 已筛选城市: {city_name}")
                else:
                    print(f"  ⚠ 城市筛选失败，继续...")

            page.wait_for_timeout(1500)
            found = extract_thread_urls(page, list_url)
            new_count = 0
            for url in found:
                if url not in seen:
                    seen.add(url)
                    all_urls.append(url)
                    new_count += 1
            print(f"  → 发现 {len(found)} 条，新增 {new_count} 条，累计 {len(all_urls)} 条")

            if max_threads and len(all_urls) >= max_threads:
                all_urls = all_urls[:max_threads]
                print(f"  → 已达上限 {max_threads} 条，停止翻页")
                break

            if delay and page_number < pages:
                time.sleep(delay)

        if not all_urls:
            browser.close()
            print("\n未发现任何帖子链接，请检查配置或网站状态。")
            return

        # 阶段2：逐个抓取详情并解析
        print(f"\n>>> 阶段2：抓取详情并解析结构化数据...")
        summary_rows = []
        detail_rows = []
        for index, url in enumerate(all_urls, start=1):
            print(f"[{index}/{len(all_urls)}] {url}")
            result = collect_page(page, url, timeout)

            detail_item = None
            if not result["permission_blocked"]:
                detail_item = parse_detail_result(result, config_path)
                if detail_item:
                    detail_rows.append(detail_item.to_dict(include_raw=False))
                    print(f"  ✔ age={detail_item.age}, price={detail_item.price_range}, "
                          f"addr={detail_item.address[:25] if detail_item.address else 'N/A'}")

            row = write_result(result, output_base, detail_item)
            summary_rows.append(row)

            if row["permission_blocked"]:
                print("  ⚠ 页面可能需登录或积分不足")

            if delay and index < len(all_urls):
                time.sleep(delay)

        browser.close()

    # 阶段3：输出结果（时间戳子文件夹）
    run_ended = datetime.now()
    region_tag = city_name or region_name or "all"
    run_dir = _make_run_dir(output_base, region_tag)
    _finalize_run(run_dir, summary_rows, detail_rows, {
        "region": region_name or "(不限)",
        "city": city_name or "(不限)",
        "forum": forum_name,
        "pages": pages,
        "max_threads": max_threads,
        "discovered_urls": len(all_urls),
        "started": run_started.isoformat(),
        "ended": run_ended.isoformat(),
    })
    _print_summary(summary_rows, detail_rows)
    print(f"  输出目录: {run_dir}")


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

    target = config.get("target", {})
    print("--- 当前采集目标 ---")
    print(f"  省份: {target.get('region', '(不限)')}")
    print(f"  城市: {target.get('city', '(不限)')}")
    print(f"  板块: {target.get('forum', '最新信息')}")
    print(f"  翻页: {target.get('pages', 1)} 页")
    print(f"  上限: {target.get('max_threads', 0) or '不限制'} 条")
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
                       help="按省份筛选（如：'北京市'、'广东省'）")
    crawl.add_argument("--city", default=None,
                       help="按城市筛选（如：'深圳市'、'广州市'，需配合--region使用）")
    crawl.add_argument("--forum", default=None,
                       help="选择论坛板块（如：'最新信息'、'自荐认证'、'包养专区'）")
    crawl.set_defaults(func=crawl_and_fetch)

    # ---- run 子命令（★ 一键运行） ----
    run_cmd = subparsers.add_parser("run", help="从config.yaml读取target配置，一键完成采集全流程")
    run_cmd.add_argument("--auth", default=str(AUTH_FILE))
    run_cmd.add_argument("--config", default=str(DEFAULT_CONFIG_PATH),
                         help="全局配置文件路径（默认: config.yaml）")
    run_cmd.add_argument("--output", default="",
                         help="输出目录（默认使用config中的output_dir）")
    run_cmd.add_argument("--delay", type=float, default=None,
                         help="请求间隔秒数（默认使用config中的delay）")
    run_cmd.add_argument("--timeout", type=int, default=None,
                         help="超时毫秒数（默认使用config中的timeout）")
    run_cmd.add_argument("--headless", action="store_true", help="无头模式运行浏览器")
    run_cmd.set_defaults(func=run_from_config)

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
