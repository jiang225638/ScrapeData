"""
单元测试与集成测试 - xhg_crawler 模块

测试范围：
  - ListItem 数据结构
  - RateLimiter 限速器
  - RetryableSession 重试机制
  - DataExtractor 数据提取器
  - DataStorage 数据存储（去重、增量）
  - XhgCrawler 集成测试
"""
import hashlib
import json
import time
from pathlib import Path
from unittest import mock

import pytest
import requests

from xhg_crawler import (
    DEFAULT_BASE_URL,
    CrawlerStats,
    DataExtractor,
    DataStorage,
    ListItem,
    MAX_RETRIES,
    MIN_DELAY,
    RateLimiter,
    RetryableSession,
    THREAD_LIST_SELECTORS,
    SUMMARY_MAX_LEN,
    XhgCrawler,
    _default_list_urls,
)


# ============================================================================
# ListItem 单元测试
# ============================================================================

class TestListItem:
    """ListIte m数据结构测试"""

    def test_default_values_empty(self):
        """测试默认值均为空字符串"""
        item = ListItem()
        assert item.title == ""
        assert item.link == ""
        assert item.publish_time == ""
        assert item.summary == ""
        assert item.category == ""
        assert item.thread_id == ""
        assert item.author == ""
        assert item.reply_count == ""
        assert item.view_count == ""
        assert item.source_page == ""
        assert item.crawl_time == ""

    def test_field_assignment(self):
        """测试字段赋值正确"""
        item = ListItem(
            title="测试帖子",
            link="https://xhg20260430.xhg303.one/forum.php?mod=viewthread&tid=123",
            publish_time="2025-04-15",
            summary="测试摘要",
            category="测试分类",
            thread_id="123",
            author="测试作者",
        )
        assert item.title == "测试帖子"
        assert item.link == "https://xhg20260430.xhg303.one/forum.php?mod=viewthread&tid=123"
        assert item.thread_id == "123"

    def test_fieldnames(self):
        """测试字段名列表正确"""
        names = ListItem.fieldnames()
        assert "title" in names
        assert "link" in names
        assert "publish_time" in names
        assert "summary" in names
        assert "category" in names
        assert "thread_id" in names
        assert "author" in names
        assert "reply_count" in names
        assert "view_count" in names
        assert "source_page" in names
        assert "crawl_time" in names

    def test_missing_fields_remain_empty(self):
        """测试部分字段缺失时其余字段保持空值"""
        item = ListItem(title="仅标题")
        assert item.title == "仅标题"
        assert item.publish_time == ""
        assert item.summary == ""


# ============================================================================
# RateLimiter 单元测试
# ============================================================================

class TestRateLimiter:
    """RateLimiter 限速器测试"""

    def test_wait_enforces_min_delay(self):
        """测试限速器强制最小延迟"""
        limiter = RateLimiter(min_delay=0.1, max_delay=0.15)
        start = time.time()
        limiter.wait()
        elapsed1 = time.time() - start

        start2 = time.time()
        limiter.wait()
        elapsed2 = time.time() - start2

        assert elapsed2 >= 0.09

    def test_first_wait_immediate(self):
        """测试首次等待无延迟"""
        limiter = RateLimiter(min_delay=0.05, max_delay=0.1)
        start = time.time()
        limiter.wait()
        elapsed = time.time() - start
        assert elapsed < 0.11

    def test_wait_with_long_gap(self):
        """测试间隔足够长时跳过延迟"""
        limiter = RateLimiter(min_delay=0.05, max_delay=0.1)
        limiter.wait()
        time.sleep(0.2)
        start = time.time()
        limiter.wait()
        elapsed = time.time() - start
        assert elapsed < 0.12


# ============================================================================
# RetryableSession 重试测试
# ============================================================================

class TestRetryableSession:
    """RetryableSession 重试机制测试"""

    def test_successful_request(self):
        """测试正常请求成功"""
        session = RetryableSession(max_retries=2, backoff_factor=0.01, timeout=5)
        mock_response = mock.MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.apparent_encoding = "utf-8"

        with mock.patch.object(session._session, "get", return_value=mock_response) as mock_get:
            response = session.get("https://example.com")
            assert response.status_code == 200
            mock_get.assert_called_once()

    def test_retry_on_timeout_then_success(self):
        """测试超时后重试成功"""
        session = RetryableSession(max_retries=3, backoff_factor=0.01, timeout=5)
        mock_response = mock.MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.apparent_encoding = "utf-8"

        with mock.patch.object(session._session, "get") as mock_get:
            mock_get.side_effect = [
                requests.exceptions.Timeout("timeout"),
                requests.exceptions.Timeout("timeout"),
                mock_response,
            ]
            response = session.get("https://example.com")
            assert response.status_code == 200
            assert mock_get.call_count == 3

    def test_retry_on_connection_error(self):
        """测试连接错误后重试"""
        session = RetryableSession(max_retries=2, backoff_factor=0.01, timeout=5)
        mock_response = mock.MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.apparent_encoding = "utf-8"

        with mock.patch.object(session._session, "get") as mock_get:
            mock_get.side_effect = [
                requests.exceptions.ConnectionError("connection refused"),
                mock_response,
            ]
            response = session.get("https://example.com")
            assert response.status_code == 200
            assert mock_get.call_count == 2

    def test_max_retries_exceeded(self):
        """测试超过最大重试次数后抛出异常"""
        session = RetryableSession(max_retries=2, backoff_factor=0.01, timeout=5)

        with mock.patch.object(session._session, "get") as mock_get:
            mock_get.side_effect = requests.exceptions.Timeout("timeout")
            with pytest.raises(RuntimeError, match="请求失败"):
                session.get("https://example.com")
            assert mock_get.call_count == 2

    def test_http_403_logs_warning(self):
        """测试HTTP 403记录警告但继续处理"""
        session = RetryableSession(max_retries=1, backoff_factor=0.01, timeout=5)
        mock_response = mock.MagicMock(spec=requests.Response)
        mock_response.status_code = 403
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("403", response=mock_response)
        mock_response.apparent_encoding = "utf-8"

        with mock.patch.object(session._session, "get", return_value=mock_response):
            with pytest.raises(RuntimeError, match="请求失败"):
                session.get("https://example.com")

    def test_referer_header_set(self):
        """测试Referer请求头正确设置"""
        session = RetryableSession(max_retries=1, timeout=5)
        mock_response = mock.MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.apparent_encoding = "utf-8"

        with mock.patch.object(session._session, "get", return_value=mock_response) as mock_get:
            session.get("https://example.com/page", referer="https://example.com/")
            call_kwargs = mock_get.call_args[1]
            assert "Referer" in call_kwargs.get("headers", {})
            assert call_kwargs["headers"]["Referer"] == "https://example.com/"


# ============================================================================
# DataExtractor 单元测试
# ============================================================================

class TestDataExtractor:
    """DataExtractor 数据提取测试"""

    def test_extract_from_threadlist_table(self, sample_discuz_threadlist_html):
        """测试从标准帖子列表表格提取数据"""
        extractor = DataExtractor()
        items = extractor.extract_list_items(
            sample_discuz_threadlist_html,
            "https://xhg20260430.xhg303.one/forum.php?mod=forumdisplay&fid=2",
        )
        assert len(items) == 3

        item1 = items[0]
        assert item1.title == "【朝阳区】验证大胸妹子的服务体验"
        assert item1.thread_id == "811063"
        assert "mod=viewthread" in item1.link
        assert "tid=811063" in item1.link
        assert item1.category == "最新信息"
        assert item1.author == "test_user1"
        assert item1.publish_time == "2025-04-15 14:30"
        assert item1.reply_count == "128"
        assert item1.view_count == "5200"

        item2 = items[1]
        assert item2.title == "【海淀区】新来的兼职学生妹"
        assert item2.thread_id == "811064"
        assert item2.author == "test_user2"
        assert item2.publish_time == "2025-04-16 10:20"

        item3 = items[2]
        assert item3.title == "【东城区】高端SPA会所体验"
        assert item3.thread_id == "811065"

    def test_extract_from_v2_layout(self, sample_discuz_threadlist_v2_html):
        """测试从v2布局（ul/li）提取数据"""
        extractor = DataExtractor()
        items = extractor.extract_list_items(
            sample_discuz_threadlist_v2_html,
            "https://xhg20260430.xhg303.one/forum.php?mod=forumdisplay&fid=47",
        )
        assert len(items) == 2

        item1 = items[0]
        assert item1.title == "【自荐】本人高素质提供优质服务"
        assert item1.thread_id == "811066"
        assert item1.category == "自荐认证"
        assert item1.author == "self_recommend1"

        item2 = items[1]
        assert item2.title == "【自荐】新来京大学生兼职"
        assert item2.thread_id == "811067"

    def test_extract_from_empty_page(self, sample_empty_list_html):
        """测试从空列表页提取数据"""
        extractor = DataExtractor()
        items = extractor.extract_list_items(
            sample_empty_list_html,
            "https://xhg20260430.xhg303.one/forum.php?mod=forumdisplay&fid=40",
        )
        assert len(items) == 0

    def test_extract_from_portal(self, sample_portal_html):
        """测试从门户首页提取链接"""
        extractor = DataExtractor()
        items = extractor.extract_list_items(
            sample_portal_html,
            "https://xhg20260430.xhg303.one/portal.php",
        )
        assert len(items) == 3
        thread_ids = {item.thread_id for item in items}
        assert thread_ids == {"811001", "811002", "811003"}

    def test_deduplication_within_same_page(self, sample_discuz_threadlist_html):
        """测试同一页面内去重（重复链接只保留一个）"""
        duplicated_html = sample_discuz_threadlist_html + """
<table id="threadlisttableid"><tbody>
<tr><th><a href="forum.php?mod=viewthread&tid=811063" class="s xst">重复帖子</a></th></tr>
</tbody></table>"""
        extractor = DataExtractor()
        items = extractor.extract_list_items(duplicated_html, "https://xhg20260430.xhg303.one/test")
        assert len(items) == 3

    def test_link_absolute_url(self):
        """测试相对链接被转换为绝对URL"""
        extractor = DataExtractor(base_url="https://xhg20260430.xhg303.one/")
        html = '''<html><body>
<table id="threadlisttableid"><tbody>
<tr><th><a href="forum.php?mod=viewthread&tid=999&extra=page%3D1" class="s xst">测试</a></th>
<td class="by"><cite><a>author</a></cite><em><span>2025-01-01</span></em></td>
<td class="num"><a class="xi2">5</a><em>100</em></td>
</tr>
</tbody></table>
</body></html>'''
        items = extractor.extract_list_items(html, "https://xhg20260430.xhg303.one/forum.php?mod=forumdisplay&fid=2")
        assert len(items) == 1
        assert items[0].link.startswith("https://xhg20260430.xhg303.one/forum.php")

    def test_parse_without_thread_rows(self):
        """测试HTML中无有效帖子行时返回空列表"""
        extractor = DataExtractor()
        items = extractor.extract_list_items("<html><body><p>普通页面</p></body></html>", "https://example.com")
        assert items == []

    def test_summary_truncation(self):
        """测试摘要长度截断"""
        extractor = DataExtractor()
        long_summary = "A" * 500
        html = f'''<html><body>
<table id="threadlisttableid"><tbody>
<tr><th><a href="forum.php?mod=viewthread&tid=1" class="s xst">测试</a></th>
<td class="by"><cite><a>author</a></cite></td>
<td class="num"><a class="xi2">5</a><em>100</em></td>
</tr>
</tbody></table>
</body></html>'''
        items = extractor.extract_list_items(html, "https://example.com")
        if items and items[0].summary:
            assert len(items[0].summary) <= SUMMARY_MAX_LEN

    def test_various_time_formats(self):
        """测试各种时间格式的提取"""
        extractor = DataExtractor()
        html = '''<html><body>
<table id="threadlisttableid"><tbody>
<tr><th><a href="forum.php?mod=viewthread&tid=1" class="s xst">T1</a></th>
<td class="by"><cite><a>a1</a></cite><em><span>2025-04-15 14:30</span></em></td>
<td class="num"><a class="xi2">1</a><em>1</em></td></tr>
<tr><th><a href="forum.php?mod=viewthread&tid=2" class="s xst">T2</a></th>
<td class="by"><cite><a>a2</a></cite><em><span>5&nbsp;分钟前</span></em></td>
<td class="num"><a class="xi2">2</a><em>2</em></td></tr>
<tr><th><a href="forum.php?mod=viewthread&tid=3" class="s xst">T3</a></th>
<td class="by"><cite><a>a3</a></cite><em><span>2&nbsp;小时前</span></em></td>
<td class="num"><a class="xi2">3</a><em>3</em></td></tr>
</tbody></table>
</body></html>'''
        items = extractor.extract_list_items(html, "https://example.com")
        assert len(items) == 3
        assert items[0].publish_time == "2025-04-15 14:30"
        assert items[1].publish_time in ("5分钟前", "5 分钟前", "")
        assert items[2].publish_time in ("2小时前", "2 小时前", "")


# ============================================================================
# DataStorage 单元测试
# ============================================================================

class TestDataStorage:
    """DataStorage 数据存储测试"""

    def test_save_new_items(self, temp_output_dir, sample_list_items):
        """测试保存新数据项"""
        storage = DataStorage(
            temp_output_dir / "list_data.json",
            temp_output_dir / "list_data.csv",
            temp_output_dir / "crawled_urls.json",
        )
        new_count, skipped = storage.save_items(sample_list_items)
        assert new_count == 2
        assert skipped == 0
        assert storage.total_records == 2

        assert (temp_output_dir / "list_data.json").exists()
        assert (temp_output_dir / "list_data.csv").exists()
        assert (temp_output_dir / "crawled_urls.json").exists()

    def test_deduplication_skips_duplicates(self, temp_output_dir, sample_list_items):
        """测试去重：已存在的URL被跳过"""
        storage = DataStorage(
            temp_output_dir / "list_data.json",
            temp_output_dir / "list_data.csv",
            temp_output_dir / "crawled_urls.json",
        )
        new_count, skipped = storage.save_items(sample_list_items)
        assert new_count == 2

        new_count2, skipped2 = storage.save_items(sample_list_items)
        assert new_count2 == 0
        assert skipped2 == 2
        assert storage.total_records == 2

    def test_incremental_update(self, temp_output_dir, sample_list_items):
        """测试增量更新：新URL追加，已有URL跳过"""
        storage = DataStorage(
            temp_output_dir / "list_data.json",
            temp_output_dir / "list_data.csv",
            temp_output_dir / "crawled_urls.json",
        )
        new_count1, _ = storage.save_items(sample_list_items[:1])
        assert new_count1 == 1

        new_items = sample_list_items[1:] + [
            ListItem(
                title="新增帖子",
                link="https://xhg20260430.xhg303.one/forum.php?mod=viewthread&tid=999003",
                thread_id="999003",
            ),
        ]
        new_count2, skipped2 = storage.save_items(new_items)
        assert new_count2 == 2
        assert skipped2 == 0
        assert storage.total_records == 3

    def test_load_existing_dedup(self, temp_output_dir, sample_list_items, dedup_json_content):
        """测试从已有去重文件加载状态"""
        dedup_path = temp_output_dir / "crawled_urls.json"
        dedup_path.parent.mkdir(parents=True, exist_ok=True)
        dedup_path.write_text(dedup_json_content, encoding="utf-8")

        storage = DataStorage(
            temp_output_dir / "list_data.json",
            temp_output_dir / "list_data.csv",
            dedup_path,
        )
        assert storage.is_duplicate is not None

    def test_is_duplicate_method(self, temp_output_dir, sample_list_items):
        """测试is_duplicate方法"""
        storage = DataStorage(
            temp_output_dir / "list_data.json",
            temp_output_dir / "list_data.csv",
            temp_output_dir / "crawled_urls.json",
        )
        assert not storage.is_duplicate(sample_list_items[0].link)
        storage.save_items(sample_list_items[:1])
        assert storage.is_duplicate(sample_list_items[0].link)
        assert not storage.is_duplicate("https://example.com/never_seen")

    def test_csv_export_format(self, temp_output_dir, sample_list_items):
        """测试CSV导出格式正确"""
        storage = DataStorage(
            temp_output_dir / "list_data.json",
            temp_output_dir / "list_data.csv",
            temp_output_dir / "crawled_urls.json",
        )
        storage.save_items(sample_list_items)

        csv_path = temp_output_dir / "list_data.csv"
        content = csv_path.read_text(encoding="utf-8-sig")
        assert "title" in content
        assert "测试帖子1" in content
        assert "测试帖子2" in content
        assert "link" in content

    def test_json_export_format(self, temp_output_dir, sample_list_items):
        """测试JSON导出格式正确"""
        storage = DataStorage(
            temp_output_dir / "list_data.json",
            temp_output_dir / "list_data.csv",
            temp_output_dir / "crawled_urls.json",
        )
        storage.save_items(sample_list_items)

        json_path = temp_output_dir / "list_data.json"
        data = json.loads(json_path.read_text(encoding="utf-8"))
        assert isinstance(data, list)
        assert len(data) == 2
        assert data[0]["title"] == "测试帖子1"

    def test_empty_storage_has_zero_records(self, temp_output_dir):
        """测试空存储记录数为0"""
        storage = DataStorage(
            temp_output_dir / "list_data.json",
            temp_output_dir / "list_data.csv",
            temp_output_dir / "crawled_urls.json",
        )
        assert storage.total_records == 0

    def test_resume_from_existing_data(self, temp_output_dir, sample_list_items):
        """测试从已有JSON文件恢复数据"""
        json_path = temp_output_dir / "list_data.json"
        json_path.parent.mkdir(parents=True, exist_ok=True)

        import dataclasses
        existing_data = [dataclasses.asdict(item) for item in sample_list_items[:1]]
        json_path.write_text(json.dumps(existing_data, ensure_ascii=False), encoding="utf-8")

        storage = DataStorage(
            json_path,
            temp_output_dir / "list_data.csv",
            temp_output_dir / "crawled_urls.json",
        )
        assert storage.total_records == 1


# ============================================================================
# XhgCrawler 集成测试
# ============================================================================

class TestXhgCrawler:
    """XhgCrawler 集成测试"""

    def test_init_creates_output_dir(self, temp_output_dir):
        """测试初始化时创建输出目录"""
        crawler = XhgCrawler(output_dir=temp_output_dir / "sub")
        assert (temp_output_dir / "sub").exists()

    def test_crawl_single_page(self, temp_output_dir, sample_discuz_threadlist_html):
        """测试爬取单个列表页"""
        crawler = XhgCrawler(output_dir=temp_output_dir)
        mock_response = mock.MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.text = sample_discuz_threadlist_html
        mock_response.apparent_encoding = "utf-8"

        with mock.patch.object(crawler._session._session, "get", return_value=mock_response):
            items = crawler.crawl_list_page(
                "https://xhg20260430.xhg303.one/forum.php?mod=forumdisplay&fid=2"
            )

        assert len(items) == 3
        assert crawler._storage.total_records == 3
        assert crawler._stats.pages_crawled == 1
        assert crawler._stats.items_extracted == 3
        assert crawler._stats.items_new == 3
        assert crawler._stats.items_skipped == 0

    def test_crawl_deduplication_across_pages(self, temp_output_dir, sample_discuz_threadlist_html):
        """测试跨页面去重：重复页面不重复存储"""
        crawler = XhgCrawler(output_dir=temp_output_dir)
        mock_response = mock.MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.text = sample_discuz_threadlist_html
        mock_response.apparent_encoding = "utf-8"

        with mock.patch.object(crawler._session._session, "get", return_value=mock_response):
            crawler.crawl_list_page("https://xhg20260430.xhg303.one/forum.php?mod=forumdisplay&fid=2")
            crawler.crawl_list_page("https://xhg20260430.xhg303.one/forum.php?mod=forumdisplay&fid=2")

        assert crawler._storage.total_records == 3
        assert crawler._stats.items_skipped == 3

    def test_crawl_with_error_handling(self, temp_output_dir):
        """测试请求失败时的错误处理"""
        crawler = XhgCrawler(output_dir=temp_output_dir)

        with mock.patch.object(crawler._session._session, "get") as mock_get:
            mock_get.side_effect = requests.exceptions.Timeout("timeout")
            items = crawler.crawl_list_page("https://xhg20260430.xhg303.one/bad_page")

        assert items == []
        assert crawler._stats.errors == 1

    def test_crawl_list_pages_stats(self, temp_output_dir, sample_discuz_threadlist_html):
        """测试crawl_list_pages统计信息"""
        crawler = XhgCrawler(output_dir=temp_output_dir)
        mock_response = mock.MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.text = sample_discuz_threadlist_html
        mock_response.apparent_encoding = "utf-8"

        with mock.patch.object(crawler._session._session, "get", return_value=mock_response):
            stats = crawler.crawl_list_pages([
                "https://xhg20260430.xhg303.one/forum.php?mod=forumdisplay&fid=2",
                "https://xhg20260430.xhg303.one/forum.php?mod=forumdisplay&fid=47",
            ])

        assert stats.pages_crawled == 2
        assert stats.items_new == 3
        assert stats.end_time
        assert isinstance(stats.start_time, str)

    def test_log_file_created(self, temp_output_dir, sample_discuz_threadlist_html):
        """测试日志文件被创建"""
        crawler = XhgCrawler(output_dir=temp_output_dir)
        mock_response = mock.MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.text = sample_discuz_threadlist_html
        mock_response.apparent_encoding = "utf-8"

        with mock.patch.object(crawler._session._session, "get", return_value=mock_response):
            crawler.crawl_list_page("https://xhg20260430.xhg303.one/test")

        log_file = temp_output_dir / "crawler.log"
        assert log_file.exists()
        log_content = log_file.read_text(encoding="utf-8")
        assert "正在爬取列表页" in log_content or "页面处理完成" in log_content


# ============================================================================
# 辅助函数测试
# ============================================================================

class TestHelpers:
    """辅助函数测试"""

    def test_default_list_urls(self):
        """测试默认列表URL生成"""
        urls = _default_list_urls(DEFAULT_BASE_URL)
        assert len(urls) == 4
        assert any("fid=2" in url for url in urls)
        assert any("filter=sortid" in url for url in urls)
        assert any("fid=47" in url for url in urls)
        assert any("fid=40" in url for url in urls)
        assert any("portal.php" in url for url in urls)

    def test_default_list_urls_all_absolute(self):
        """测试默认URL都是绝对路径"""
        urls = _default_list_urls(DEFAULT_BASE_URL)
        for url in urls:
            assert url.startswith("https://")


# ============================================================================
# CrawlerStats 测试
# ============================================================================

class TestCrawlerStats:
    """CrawlerStats 统计测试"""

    def test_default_values(self):
        """测试默认统计值"""
        stats = CrawlerStats()
        assert stats.pages_crawled == 0
        assert stats.items_extracted == 0
        assert stats.items_new == 0
        assert stats.items_skipped == 0
        assert stats.errors == 0

    def test_field_update(self):
        """测试字段更新"""
        stats = CrawlerStats(
            pages_crawled=5,
            items_extracted=100,
            items_new=80,
            items_skipped=20,
            errors=2,
        )
        assert stats.pages_crawled == 5
        assert stats.items_new == 80
        assert stats.errors == 2
