# 寻欢阁数据采集工具集

包含两套工具：
- **`xhg_scraper.py`** — 基于Playwright的帖子详情页抓取（需登录状态）
- **`xhg_crawler.py`** — 基于requests+BeautifulSoup的列表数据爬虫（无需登录）

---

## 安装依赖

需要 Python 3.10 或更高版本：

```powershell
pip install -r requirements.txt
```

如果使用 `xhg_scraper.py` 的Playwright功能，还需要安装浏览器：

```powershell
playwright install chromium
```

---

## xhg_crawler.py — 列表数据爬虫

从论坛列表页提取帖子列表数据（标题、链接、发布时间、摘要等），保存为JSON/CSV格式。

### 功能特性

- **HTTP请求与异常处理**：自动处理403/404、超时、重定向等异常
- **HTML解析**：使用BeautifulSoup+lxml解析Discuz!论坛列表页
- **结构化输出**：提取标题、链接、发布时间、摘要、分类、作者、回复数、查看数等字段
- **去重与增量更新**：同一URL只采集一次，后续运行仅追加新记录
- **限速与礼貌性爬取**：请求间隔1-2秒，携带合法User-Agent及Referer
- **日志记录**：INFO级别以上日志，网络异常写入ERROR并自动重试3次
- **多种列表页兼容**：支持表格布局和ul/li布局的列表页

### 快速开始

```powershell
# 爬取默认的列表页（带筛选条件的最新信息页面）
python xhg_crawler.py

# 指定要爬取的列表页URL
python xhg_crawler.py --urls "https://xhg20260430.xhg303.one/forum.php?mod=forumdisplay&fid=2&filter=sortid&sortid=3&searchsort=1&area=6.1"

# 从文件读取URL列表
python xhg_crawler.py --urls-file urls.txt

# 指定输出目录
python xhg_crawler.py --output-dir my_output

# 设置日志级别（DEBUG/INFO/WARNING/ERROR）
python xhg_crawler.py --log-level DEBUG
```

### 输出文件

运行后在 `output/` 目录生成以下文件：

| 文件 | 说明 |
|------|------|
| `list_data.json` | 所有采集记录的JSON格式（增量追加） |
| `list_data.csv` | 所有采集记录的CSV格式 |
| `crawled_urls.json` | 已采集URL的去重哈希表 |
| `crawler.log` | 完整运行日志 |

### 数据结构

每条记录包含以下字段（缺失时留空）：

```json
{
  "title": "帖子标题",
  "link": "帖子完整链接",
  "publish_time": "发布时间",
  "summary": "摘要内容",
  "category": "所属板块/分类",
  "thread_id": "帖子ID",
  "author": "发布者",
  "reply_count": "回复数",
  "view_count": "查看数",
  "source_page": "来源列表页URL",
  "crawl_time": "采集时间"
}
```

---

## xhg_scraper.py — 帖子详情抓取（需登录）

用于抓取需要会员权限的帖子详情页内容。必须先手动登录保存认证状态。

### 第一次登录

```powershell
python xhg_scraper.py login
```

运行后会打开浏览器窗口，手动登录后按回车保存认证状态到 `auth_state.json`。

### 抓取单个页面

```powershell
python xhg_scraper.py fetch --url "https://xhg20260430.xhg303.one/forum.php?mod=viewthread&tid=811063&fromguid=hot&extra=page%3D1&mobile=2"
```

### 自动从列表页发现并抓取全部帖子

```powershell
python xhg_scraper.py crawl --start-url "列表页URL" --pages 5
```

### 批量抓取

```powershell
python xhg_scraper.py fetch --urls-file urls.txt
```

---

## 运行测试

```powershell
# 运行所有测试并查看覆盖率
pytest tests/test_crawler.py -v --cov=xhg_crawler --cov-report=term

# 生成HTML覆盖率报告
pytest tests/test_crawler.py --cov=xhg_crawler --cov-report=html
```

覆盖率报告生成在 `htmlcov/` 目录中，用浏览器打开 `htmlcov/index.html` 即可查看。

---

## 项目结构

```
getSomeThingScript/
├── xhg_crawler.py      # 列表数据爬虫（主模块）
├── xhg_scraper.py      # 帖子详情抓取工具
├── requirements.txt    # Python依赖
├── README.md          # 使用说明
├── auth_state.json    # Playwright登录状态（需自行生成）
├── tests/
│   ├── __init__.py
│   ├── conftest.py     # pytest夹具
│   └── test_crawler.py # 测试用例
├── output/
│   ├── list_data.json       # 示例JSON输出
│   ├── list_data.csv        # 示例CSV输出
│   ├── crawled_urls.json    # 去重记录示例
│   ├── 811063.html          # 帖子页面示例
│   └── summary.csv          # 汇总示例
└── htmlcov/                 # 覆盖率报告（运行测试后生成）
```

## 常见问题

**Q: 爬取返回空列表？**
A: 检查目标页面URL是否正确，确认是Discuz!论坛的列表页（如 `forumdisplay`），日志中会显示提取的记录数。

**Q: 请求返回403/404？**
A: 日志会记录WARNING级别信息，程序会自动跳过该页面并继续处理后续URL。

**Q: 网络异常怎么办？**
A: 每个请求会自动重试3次（指数退避），3次全部失败则跳过该页面记录ERROR日志。

**Q: 如何只抓取新增内容？**
A: 程序自动维护 `crawled_urls.json` 去重文件，重复运行只会追加新发现的记录。
