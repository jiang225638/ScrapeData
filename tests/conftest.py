"""
pytest 共享夹具（fixtures）
"""
import json
import shutil
import uuid
from pathlib import Path

import pytest


@pytest.fixture
def sample_discuz_threadlist_html() -> str:
    """模拟Discuz! X3.4论坛帖子列表页HTML"""
    return """<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta charset="utf-8">
<title>最新信息 - 楼凤-寻欢阁</title>
<base href="https://xhg20260430.xhg303.one/">
</head>
<body>
<div id="ct">
<div class="bm">
<div class="bm_h"><h1>最新信息</h1></div>
<div class="bm_c">
<table id="threadlisttableid">
<tbody>
<tr>
  <td class="icn"><a href="forum.php?mod=viewthread&amp;tid=811063&amp;extra=page%3D1" target="_blank"><img src="static/image/common/folder_new.gif"></a></td>
  <th class="new">
    <a href="forum.php?mod=viewthread&amp;tid=811063&amp;extra=page%3D1" class="s xst">【朝阳区】验证大胸妹子的服务体验</a>
  </th>
  <td class="by">
    <cite><a href="home.php?mod=space&amp;uid=12345">test_user1</a></cite>
    <em><span>2025-04-15 14:30</span></em>
  </td>
  <td class="num"><a href="forum.php?mod=viewthread&amp;tid=811063&amp;extra=page%3D1" class="xi2">128</a><em>5200</em></td>
</tr>
<tr>
  <td class="icn"><a href="forum.php?mod=viewthread&amp;tid=811064&amp;extra=page%3D1" target="_blank"><img src="static/image/common/folder_new.gif"></a></td>
  <th class="new">
    <a href="forum.php?mod=viewthread&amp;tid=811064&amp;extra=page%3D1" class="s xst">【海淀区】新来的兼职学生妹</a>
  </th>
  <td class="by">
    <cite><a href="home.php?mod=space&amp;uid=23456">test_user2</a></cite>
    <em><span>2025-04-16 10:20</span></em>
  </td>
  <td class="num"><a href="forum.php?mod=viewthread&amp;tid=811064&amp;extra=page%3D1" class="xi2">85</a><em>3100</em></td>
</tr>
<tr>
  <td class="icn"><a href="forum.php?mod=viewthread&amp;tid=811065&amp;extra=page%3D1" target="_blank"><img src="static/image/common/folder_common.gif"></a></td>
  <th class="new">
    <a href="forum.php?mod=viewthread&amp;tid=811065&amp;extra=page%3D1" class="s xst">【东城区】高端SPA会所体验</a>
  </th>
  <td class="by">
    <cite><a href="home.php?mod=space&amp;uid=34567">test_user3</a></cite>
    <em><span>2025-04-17&nbsp;08:45</span></em>
  </td>
  <td class="num"><a href="forum.php?mod=viewthread&amp;tid=811065&amp;extra=page%3D1" class="xi2">56</a><em>1890</em></td>
</tr>
</tbody>
</table>
</div>
</div>
</div>
</body>
</html>"""


@pytest.fixture
def sample_discuz_threadlist_v2_html() -> str:
    """模拟Discuz! v2格式论坛帖子列表页HTML（使用div布局）"""
    return """<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>自荐认证 - 楼凤-寻欢阁</title></head>
<body>
<div class="tl">
<div class="bm">
<div class="bm_h"><a href="forum.php?mod=forumdisplay&fid=47">自荐认证</a></div>
<div class="bm_c">
<ul>
<li>
  <a href="forum.php?mod=viewthread&tid=811066&extra=page%3D1" class="s xst">【自荐】本人高素质提供优质服务</a>
  <div class="authi">
    <cite><a href="home.php?mod=space&uid=45678">self_recommend1</a></cite>
    <span title="2025-04-18">3&nbsp;天前</span>
  </div>
  <div class="threaddesc">个人简介：25岁，身高165，体重48kg，肤白貌美，服务态度好...</div>
  <div class="num">35回复 · 1200查看</div>
</li>
<li>
  <a href="forum.php?mod=viewthread&tid=811067&extra=page%3D1" class="s xst">【自荐】新来京大学生兼职</a>
  <div class="authi">
    <cite><a href="home.php?mod=space&uid=56789">self_recommend2</a></cite>
    <span>2&nbsp;小时前</span>
  </div>
  <div class="threaddesc">大学生兼职，年龄22，服务认真，非诚勿扰...</div>
  <div class="num">18回复 · 890查看</div>
</li>
</ul>
</div>
</div>
</div>
</body>
</html>"""


@pytest.fixture
def sample_empty_list_html() -> str:
    """模拟空列表页HTML（无帖子内容）"""
    return """<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>包养专区 - 无内容</title></head>
<body>
<div class="bm">
<div class="bm_h"><h1>包养专区</h1></div>
<div class="bm_c">
<table id="threadlisttableid">
<tbody>
<tr><td class="icn" colspan="4">本版块暂无帖子</td></tr>
</tbody>
</table>
</div>
</div>
</body>
</html>"""


@pytest.fixture
def sample_portal_html() -> str:
    """模拟门户首页HTML（从链接提取）"""
    return """<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>楼凤-寻欢阁</title></head>
<body>
<div id="portal_block">
  <div class="module cl">
    <ul>
      <li><a href="forum.php?mod=viewthread&tid=811001">最新帖子标题1</a></li>
      <li><a href="forum.php?mod=viewthread&tid=811002">最新帖子标题2</a></li>
      <li><a href="forum.php?mod=viewthread&tid=811003">最新帖子标题3</a></li>
    </ul>
  </div>
</div>
</body>
</html>"""


@pytest.fixture
def temp_output_dir():
    """创建临时输出目录，测试结束后自动清理"""
    tmpdir = Path.cwd() / ".tmp" / f"xhg_test_{uuid.uuid4().hex}"
    tmpdir.mkdir(parents=True, exist_ok=True)
    try:
        yield tmpdir
    finally:
        import logging
        # 关闭所有xhg_crawler日志处理器以释放文件句柄
        logger = logging.getLogger("xhg_crawler")
        for handler in list(logger.handlers):
            handler.close()
            logger.removeHandler(handler)
        try:
            shutil.rmtree(tmpdir)
        except (PermissionError, NotADirectoryError, OSError):
            pass


@pytest.fixture
def sample_list_items():
    """返回预定义的样本数据"""
    from xhg_crawler import ListItem
    return [
        ListItem(
            title="测试帖子1",
            link="https://xhg20260430.xhg303.one/forum.php?mod=viewthread&tid=999001",
            publish_time="2025-04-15 14:30",
            summary="这是测试摘要1内容，用于验证存储功能",
            category="最新信息",
            thread_id="999001",
            author="test_user1",
            reply_count="10",
            view_count="100",
            source_page="https://xhg20260430.xhg303.one/forum.php?mod=forumdisplay&fid=2",
        ),
        ListItem(
            title="测试帖子2",
            link="https://xhg20260430.xhg303.one/forum.php?mod=viewthread&tid=999002",
            publish_time="2025-04-16 10:20",
            summary="测试摘要2",
            category="最新信息",
            thread_id="999002",
            author="test_user2",
            reply_count="5",
            view_count="50",
            source_page="https://xhg20260430.xhg303.one/forum.php?mod=forumdisplay&fid=2",
        ),
    ]


@pytest.fixture
def dedup_json_content():
    """去重文件JSON内容"""
    return json.dumps({
        "hashes": [
            "abc123def456",
        ],
        "updated": "2025-04-01T00:00:00",
    })
