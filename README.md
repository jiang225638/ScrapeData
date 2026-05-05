# 寻欢阁数据采集工具集

包含三个核心模块：
- **`xhg_crawler.py`** — 基于 requests + BeautifulSoup 的列表数据爬虫（无需登录）
- **`xhg_scraper.py`** — 基于 Playwright 的帖子详情页抓取（需登录状态，支持地区筛选）
- **`detail_parser.py`** — 详情页结构化数据解析器（正则提取，支持配置文件自定义字段）

---

## 安装依赖

需要 Python 3.10 或更高版本：

```bash
python3 -m pip install -r requirements.txt
```

如果使用 `xhg_scraper.py` 的 Playwright 功能，还需要安装浏览器：

```bash
python3 -m playwright install chromium
```

### 依赖清单

| 包 | 用途 |
|---|------|
| requests | HTTP 请求（xhg_crawler） |
| beautifulsoup4 + lxml | HTML 解析（xhg_crawler / detail_parser） |
| playwright | 浏览器自动化渲染（xhg_scraper） |
| pyyaml | 配置文件读取（xhg_scraper / detail_parser） |
| pytest + pytest-cov | 测试与覆盖率 |

---

## xhg_crawler.py — 列表数据爬虫

从论坛列表页提取帖子列表数据（标题、链接、发布时间、摘要等），保存为 JSON/CSV 格式。**无需登录**。

### 功能特性

- **HTTP 请求与异常处理**：自动处理 403/404、超时、重定向等异常
- **HTML 解析**：使用 BeautifulSoup + lxml 解析 Discuz! 论坛列表页
- **结构化输出**：提取标题、链接、发布时间、摘要、分类、作者、回复数、查看数等字段
- **去重与增量更新**：同一 URL 只采集一次（MD5 哈希），后续运行仅追加新记录
- **限速与礼貌性爬取**：请求间隔 1-2 秒，携带合法 User-Agent 及 Referer
- **日志记录**：INFO 级别以上日志，网络异常写入 ERROR 并自动重试 3 次（指数退避）
- **多种列表页兼容**：支持表格布局（table）和 ul/li 布局的列表页

### 快速开始

```bash
# 爬取默认的列表页
python3 xhg_crawler.py

# 指定要爬取的列表页 URL
python3 xhg_crawler.py --urls "https://xhg20260430.xhg303.one/forum.php?mod=forumdisplay&fid=2"

# 从文件读取 URL 列表
python3 xhg_crawler.py --urls-file urls.txt

# 指定输出目录
python3 xhg_crawler.py --output-dir my_output

# 设置日志级别（DEBUG/INFO/WARNING/ERROR）
python3 xhg_crawler.py --log-level DEBUG

# 启用 Playwright 动态渲染（用于 JS 渲染页面）
python3 xhg_crawler.py --use-playwright --auth-file auth_state.json
```

### 命令行参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--base-url` | `https://xhg20260430.xhg303.one/` | 网站基础 URL |
| `--output-dir` | `output` | 输出目录 |
| `--urls` | 默认列表页 | 要爬取的列表页 URL 列表 |
| `--urls-file` | 无 | 包含 URL 列表的文本文件（每行一个） |
| `--use-playwright` | `false` | 启用 Playwright 动态渲染 |
| `--auth-file` | 无 | Playwright 登录状态文件路径 |
| `--log-level` | `INFO` | 日志级别 |

### 输出文件

运行后在 `output/` 目录生成以下文件：

| 文件 | 说明 |
|------|------|
| `list_data.json` | 所有采集记录的 JSON 格式（增量追加） |
| `list_data.csv` | 所有采集记录的 CSV 格式 |
| `crawled_urls.json` | 已采集 URL 的 MD5 去重哈希表 |
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

用于抓取需要会员权限的帖子详情页内容，并自动解析结构化数据。必须先手动登录保存认证状态。

### 子命令概览

| 子命令 | 说明 |
|--------|------|
| `login` | 手动登录并保存浏览器状态 |
| `fetch` | 抓取指定 URL 详情页并解析 |
| `crawl` | 从列表页发现帖子链接并抓取详情 |
| `run` | ★ 一键运行：从 config.yaml 读取配置自动执行全流程 |
| `config` | 显示当前配置信息（地区、板块、字段等） |

### 第一次登录

```bash
python3 xhg_scraper.py login
```

运行后会打开浏览器窗口，手动登录后按回车保存认证状态到 `auth_state.json`。

### 抓取单个页面

```bash
python3 xhg_scraper.py fetch --url "https://xhg20260430.xhg303.one/forum.php?mod=viewthread&tid=811063&fromguid=hot&extra=page%3D1&mobile=2"
```

### 批量抓取

```bash
python3 xhg_scraper.py fetch --urls-file urls.txt
```

### 从列表页发现并抓取（支持城市筛选）

采集流程：根据城市 area 代码构造筛选 URL → 翻列表页 → 逐个详情页 → 解析输出

**添加新城市**：在浏览器访问首页 `portal.php`，点击热门城市，将跳转后 URL 中的 `area=X.X` 添加到 `config.yaml` 的 `cities` 表。

```bash
# 按城市筛选（需先在 config.yaml 的 cities 表中配置 area 代码）
python3 xhg_scraper.py crawl --city "南京市" --pages 3

# 抓取指定板块，翻5页
python3 xhg_scraper.py crawl --forum "最新信息" --pages 5

# 指定板块 + 城市
python3 xhg_scraper.py crawl --forum "自荐认证" --city "深圳市" --pages 2

# 限制最大采集数
python3 xhg_scraper.py crawl --city "广州市" --max-threads 50 --pages 5
```

### ★ 一键运行（从配置文件读取参数）

```bash
python3 xhg_scraper.py run
```

自动从 `config.yaml` 的 `target` 节读取城市、板块、翻页数等配置，完成：
**根据 cities 表查 area 代码 → 构造筛选列表 URL → 翻页 → 抓详情 → 解析 → 输出**

可通过命令行覆盖部分参数：

```bash
python3 xhg_scraper.py run --headless --delay 2.0 --output my_output
```

### 查看当前配置

```bash
python3 xhg_scraper.py config
```

### 命令行参数

#### fetch 子命令

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--url` | 无 | 单个帖子 URL |
| `--urls-file` | 无 | URL 列表文件（每行一个） |
| `--auth` | `auth_state.json` | 登录状态文件路径 |
| `--output` | `output` | 输出目录 |
| `--timeout` | `60000` | 超时毫秒数 |
| `--delay` | `1.5` | 请求间隔秒数 |
| `--headless` | `false` | 无头模式运行浏览器 |
| `--config` | `config.yaml` | 全局配置文件路径 |

#### crawl 子命令

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--city` | 无 | 城市名（需在 config.yaml 的 cities 表配置 area 代码） |
| `--pages` | `1` | 要扫描的列表页数 |
| `--max-threads` | `0`（不限） | 最大采集帖子数 |
| `--forum` | `最新信息` | 论坛板块（`最新信息`/`自荐认证`/`包养专区`） |
| `--region` | 无 | 省份名（仅作元数据标记） |
| `--auth` | `auth_state.json` | 登录状态文件路径 |
| `--output` | `output` | 输出目录 |
| `--timeout` | `60000` | 超时毫秒数 |
| `--delay` | `1.5` | 请求间隔秒数 |
| `--headless` | `false` | 无头模式运行浏览器 |
| `--config` | `config.yaml` | 全局配置文件路径 |

### 输出文件

每次运行在 `output/` 目录下创建以**时间戳**命名的子文件夹（如 `output/南京市_2025-05-05_14-30-00/`），包含：

| 文件 | 说明 |
|------|------|
| `summary.json` | 采集汇总（元数据 + 所有页面记录） |
| `detail_data.json` | 结构化详情数据汇总 |
| `detail_data.csv` | 结构化详情数据 CSV 格式 |

---

## detail_parser.py — 详情页数据解析器

从 Playwright 渲染后的详情页文本中提取结构化数据，支持配置文件自定义字段。

### 解析字段

| 字段 | key | 说明 |
|------|-----|------|
| 小姐年龄 | `age` | 数字 |
| 小姐颜值 | `beauty_score` | X分 |
| 消费水平 | `price_range` | 价格范围 |
| 服务项目 | `services` | 文本描述 |
| 与你号 | `yuni_id` | 账号 |
| 电报号 | `telegram` | Telegram 账号 |
| QQ | `qq` | QQ 号 |
| 微信 | `wechat` | 微信号 |
| 电话号码 | `phone` | 手机号 |
| 详细地址 | `address` | 地址文本 |

此外自动提取元数据：`publish_date`（发布日期）、`view_count`（浏览量）、`author`（作者）、`region_province`（省份）、`region_city`（城市）。

### 数据结构

```json
{
  "url": "详情页URL",
  "tid": "帖子ID",
  "title": "页面标题",
  "crawl_time": "抓取时间",
  "publish_date": "发布日期",
  "view_count": "浏览量",
  "author": "作者",
  "region_province": "省份",
  "region_city": "城市",
  "age": "年龄",
  "beauty_score": "颜值评分",
  "price_range": "消费水平",
  "services": "服务项目",
  "yuni_id": "与你号",
  "telegram": "电报号",
  "qq": "QQ号",
  "wechat": "微信号",
  "phone": "电话号码",
  "address": "详细地址"
}
```

---

## config.yaml — 全局配置文件

### 采集目标配置

```yaml
target:
  region: "江苏省"     # 省份（留空=不限）
  city: "南京市"       # 城市（留空=不限）
  forum: "最新信息"    # 板块名称
  pages: 1             # 列表页翻页数
  max_threads: 10      # 最大采集数（0=不限）
```

### 论坛板块

| 板块 | fid | 列表 URL |
|------|-----|----------|
| 最新信息 | 2 | `forum.php?mod=forumdisplay&fid=2` |
| 自荐认证 | 47 | `forum.php?mod=forumdisplay&fid=47` |
| 包养专区 | 40 | `forum.php?mod=forumdisplay&fid=40` |

### 可选地区

配置文件中包含 34 个省级地区（含港澳台），支持省份 + 城市二级筛选。

### 采集参数

```yaml
crawl:
  output_dir: "output"
  delay: 1.5
  timeout: 60000
  use_headless: true
```

---

## 运行测试

```bash
# 运行所有测试并查看覆盖率
pytest tests/test_crawler.py -v --cov=xhg_crawler --cov-report=term

# 生成 HTML 覆盖率报告
pytest tests/test_crawler.py --cov=xhg_crawler --cov-report=html
```

覆盖率报告生成在 `htmlcov/` 目录中，用浏览器打开 `htmlcov/index.html` 即可查看。

### 测试覆盖范围

- **ListItem** — 数据结构默认值、字段赋值、字段名列表
- **RateLimiter** — 限速延迟、首次等待、长间隔跳过
- **RetryableSession** — 正常请求、超时重试、连接错误重试、最大重试、Referer 头
- **DataExtractor** — 表格布局解析、ul/li 布局解析、空页面、门户首页、同页去重、绝对 URL、时间格式
- **DataStorage** — 新增保存、去重跳过、增量追加、CSV/JSON 导出、已有数据恢复
- **XhgCrawler** — 初始化、单页爬取、跨页去重、错误处理、统计信息、日志文件

---

## 项目结构

```
getDataScript/
├── xhg_crawler.py      # 列表数据爬虫（requests + BeautifulSoup）
├── xhg_scraper.py      # 帖子详情抓取（Playwright + 地区筛选）
├── detail_parser.py    # 详情页结构化数据解析器
├── config.yaml         # 全局配置（目标地区/板块/字段/参数）
├── requirements.txt    # Python 依赖
├── README.md           # 使用说明
├── auth_state.json     # Playwright 登录状态（需自行生成）
├── images/             # 参考截图
├── tests/
│   ├── __init__.py
│   ├── conftest.py     # pytest 夹具（HTML 样本、临时目录）
│   └── test_crawler.py # 测试用例
└── output/             # 采集输出目录（运行后生成）
    ├── list_data.json        # 列表数据 JSON
    ├── list_data.csv         # 列表数据 CSV
    ├── crawled_urls.json     # 去重哈希表
    ├── crawler.log           # 运行日志
    └── <时间戳子目录>/       # 详情页采集结果
        ├── summary.json      #   采集汇总
        ├── detail_data.json  #   结构化详情 JSON
        └── detail_data.csv   #   结构化详情 CSV
```

## 常见问题

**Q: 爬取返回空列表？**
A: 检查目标页面 URL 是否正确，确认是 Discuz! 论坛的列表页（如 `forumdisplay`），日志中会显示提取的记录数。

**Q: 请求返回 403/404？**
A: 日志会记录 WARNING 级别信息，程序会自动跳过该页面并继续处理后续 URL。

**Q: 网络异常怎么办？**
A: 每个请求会自动重试 3 次（指数退避），3 次全部失败则跳过该页面记录 ERROR 日志。

**Q: 如何只抓取新增内容？**
A: 程序自动维护 `crawled_urls.json` 去重文件，重复运行只会追加新发现的记录。

**Q: 如何修改采集目标？**
A: 编辑 `config.yaml` 的 `target` 节，修改 `city`、`forum`、`pages`、`max_threads` 等参数，然后运行 `python3 xhg_scraper.py run`。

**Q: 如何添加新的城市？**
A: 浏览器访问首页 `portal.php`，点击热门城市，跳转后 URL 中会包含 `area=X.X`。将该城市名和代码添加到 `config.yaml` 的 `cities` 表即可。

**Q: 详情页提示权限不足？**
A: 需要先登录保存状态：`python3 xhg_scraper.py login`。如果登录状态过期，重新登录即可。

**Q: 如何自定义提取字段？**
A: 编辑 `config.yaml` 的 `detail_fields` 节，添加/修改字段名（`name`）、键名（`key`）和正则模式（`pattern`）。
