import argparse
import csv
import json
import re
import sys
import time
from pathlib import Path
from urllib.parse import parse_qs, quote, unquote, urlencode, urljoin, urlparse, urlunparse

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


DEFAULT_URL = (
    "https://xhg20260430.xhg303.one/forum.php?"
    "mod=viewthread&tid=811063&fromguid=hot&extra=page%3D1&mobile=2"
)
DEFAULT_START_URL = "https://xhg20260430.xhg303.one/"
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


def normalize_filename(value: str, fallback: str) -> str:
    value = re.sub(r"[^\w.-]+", "_", value, flags=re.UNICODE).strip("_")
    return value or fallback


def tid_from_url(url: str) -> str:
    parsed = urlparse(url)
    tid = parse_qs(parsed.query).get("tid", [""])[0]
    return normalize_filename(tid, "page")


def read_urls(args: argparse.Namespace) -> list[str]:
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


def save_login_state(args: argparse.Namespace) -> None:
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


def collect_page(page, url: str, timeout: int) -> dict:
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


def write_result(result: dict, output_dir: Path) -> dict:
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

    return {
        "tid": tid,
        "title": result["title"],
        "status": result["status"],
        "permission_blocked": result["permission_blocked"],
        "text_file": str(text_path),
        "html_file": str(html_path),
        "json_file": str(json_path),
        "url": result["url"],
    }


def fetch_pages(args: argparse.Namespace) -> None:
    auth_file = Path(args.auth)
    if not auth_file.exists():
        raise FileNotFoundError(
            f"Login state file not found: {auth_file}. Run first: python xhg_scraper.py login"
        )

    urls = read_urls(args)
    output_dir = Path(args.output)
    summary_rows = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=args.headless)
        context = browser.new_context(storage_state=str(auth_file))
        page = context.new_page()

        for index, url in enumerate(urls, start=1):
            print(f"[{index}/{len(urls)}] Fetching: {url}")
            result = collect_page(page, url, args.timeout)
            row = write_result(result, output_dir)
            summary_rows.append(row)

            if row["permission_blocked"]:
                print("  Note: the returned page still contains a permission/login warning.")
            else:
                print(f"  Done: {row['text_file']}")

            if args.delay and index < len(urls):
                time.sleep(args.delay)

        browser.close()

    summary_file = output_dir / "summary.csv"
    with summary_file.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=list(summary_rows[0].keys()))
        writer.writeheader()
        writer.writerows(summary_rows)

    print(f"Summary file: {summary_file.resolve()}")


def crawl_and_fetch(args: argparse.Namespace) -> None:
    auth_file = Path(args.auth)
    if not auth_file.exists():
        raise FileNotFoundError(
            f"Login state file not found: {auth_file}. Run first: python xhg_scraper.py login"
        )

    output_dir = Path(args.output)
    discovered_file = output_dir / "discovered_urls.txt"
    start_url = args.start_url
    all_urls: list[str] = []
    seen = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=args.headless)
        context = browser.new_context(storage_state=str(auth_file))
        page = context.new_page()

        for page_number in range(1, args.pages + 1):
            list_url = replace_page_number(start_url, page_number)
            print(f"[list {page_number}/{args.pages}] Opening: {list_url}")
            page.goto(list_url, wait_until="networkidle", timeout=args.timeout)
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

        output_dir.mkdir(parents=True, exist_ok=True)
        discovered_file.write_text("\n".join(all_urls), encoding="utf-8")
        print(f"Discovered URL file: {discovered_file.resolve()}")

        if not all_urls:
            browser.close()
            print("No thread links found. Try passing a forum/list page URL with --start-url.")
            return

        summary_rows = []
        for index, url in enumerate(all_urls, start=1):
            print(f"[thread {index}/{len(all_urls)}] Fetching: {url}")
            result = collect_page(page, url, args.timeout)
            row = write_result(result, output_dir)
            summary_rows.append(row)

            if row["permission_blocked"]:
                print("  Note: the returned page still contains a permission/login warning.")
            else:
                print(f"  Done: {row['text_file']}")

            if args.delay and index < len(all_urls):
                time.sleep(args.delay)

        browser.close()

    summary_file = output_dir / "summary.csv"
    with summary_file.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=list(summary_rows[0].keys()))
        writer.writeheader()
        writer.writerows(summary_rows)

    print(f"Summary file: {summary_file.resolve()}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch forum pages with a saved Playwright member login state."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    login = subparsers.add_parser("login", help="Log in manually and save auth_state.json")
    login.add_argument("--base-url", default="https://xhg20260430.xhg303.one/")
    login.add_argument("--auth", default=str(AUTH_FILE))
    login.add_argument("--timeout", type=int, default=60000)
    login.set_defaults(func=save_login_state)

    fetch = subparsers.add_parser("fetch", help="Fetch pages using the saved login state")
    fetch.add_argument("--url", help="Single thread URL")
    fetch.add_argument("--urls-file", help="Text file with one URL per line")
    fetch.add_argument("--auth", default=str(AUTH_FILE))
    fetch.add_argument("--output", default=str(OUTPUT_DIR))
    fetch.add_argument("--timeout", type=int, default=60000)
    fetch.add_argument("--delay", type=float, default=1.5, help="Delay between URLs in seconds")
    fetch.add_argument("--headless", action="store_true", help="Run browser in headless mode")
    fetch.set_defaults(func=fetch_pages)

    crawl = subparsers.add_parser("crawl", help="Discover thread links from list pages and fetch them")
    crawl.add_argument("--start-url", default=DEFAULT_START_URL, help="Forum/list page URL to start from")
    crawl.add_argument("--pages", type=int, default=1, help="How many list pages to scan")
    crawl.add_argument("--max-threads", type=int, default=0, help="Stop after this many unique threads")
    crawl.add_argument("--auth", default=str(AUTH_FILE))
    crawl.add_argument("--output", default=str(OUTPUT_DIR))
    crawl.add_argument("--timeout", type=int, default=60000)
    crawl.add_argument("--delay", type=float, default=1.5, help="Delay between pages/threads in seconds")
    crawl.add_argument("--headless", action="store_true", help="Run browser in headless mode")
    crawl.set_defaults(func=crawl_and_fetch)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        args.func(args)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
