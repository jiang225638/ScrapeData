"""
详情页数据解析模块

功能：
  - 从Playwright渲染后的详情页文本中提取结构化数据
  - 支持年龄、消费水平、服务项目、联系方式、详细地址等字段
  - 支持从配置文件加载字段定义
  - 输出为标准化的dataclass结构
"""
import re
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional

import yaml


# ---------------------------------------------------------------------------
# 默认正则模式（当无配置文件时使用）
# ---------------------------------------------------------------------------
DEFAULT_PATTERNS = [
    ("age", re.compile(r"小姐年龄[：:]\s*(\d+)", re.MULTILINE)),
    ("beauty_score", re.compile(r"小姐颜值[：:]\s*(\d+)", re.MULTILINE)),
    ("price_range", re.compile(r"消费水平[：:]\s*([\d,-]+)", re.MULTILINE)),
    ("services", re.compile(r"服务项目[：:]\s*(.+?)(?:\n\s*(?:联系方式|电话|手机|微信|QQ|详细地址|详情介绍|我要举报|我要收藏))", re.MULTILINE | re.DOTALL)),
    ("yuni_id", re.compile(r"与你号[：:]\s*([a-zA-Z0-9_-]+)", re.MULTILINE)),
    ("telegram", re.compile(r"电报号[：:]\s*(@?[a-zA-Z0-9_-]+)", re.MULTILINE)),
    ("qq", re.compile(r"(?:QQ[：:]|qq号[：:]|[Qq][Qq]号[：:])\s*(\d+)", re.MULTILINE | re.IGNORECASE)),
    ("wechat", re.compile(r"微信[：:]\s*([a-zA-Z0-9_-]+)", re.MULTILINE)),
    ("phone", re.compile(r"(?:电话号码|手机|电话)[：:]\s*(\d[\d\s-]{6,})", re.MULTILINE)),
    ("address", re.compile(r"详细地址[：:]\s*(.+?)(?:\n|$)", re.MULTILINE)),
]
# 中文逗号/列表分隔模式
SEPARATOR_PATTERN = re.compile(r"[，,、;；]\s*")


@dataclass
class DetailItem:
    """详情页数据结构，缺失字段留空"""
    # 基础识别信息
    url: str = ""
    tid: str = ""
    title: str = ""
    crawl_time: str = ""

    # 元数据
    publish_date: str = ""
    view_count: str = ""
    author: str = ""

    # 所在地区
    region_province: str = ""
    region_city: str = ""

    # 核心字段
    age: str = ""
    beauty_score: str = ""
    price_range: str = ""
    services: str = ""

    # 联系方式
    yuni_id: str = ""
    telegram: str = ""
    qq: str = ""
    wechat: str = ""
    phone: str = ""
    address: str = ""

    # 原始文本（保留完整文本以便后续分析）
    raw_text: str = ""

    # 额外字段（动态扩展）
    extra: dict = field(default_factory=dict)

    @staticmethod
    def fieldnames() -> list[str]:
        """返回所有字段名列表"""
        return [
            "url", "tid", "title", "crawl_time",
            "publish_date", "view_count", "author",
            "region_province", "region_city",
            "age", "beauty_score", "price_range", "services",
            "yuni_id", "telegram", "qq", "wechat", "phone", "address",
            "raw_text",
        ]

    def to_dict(self, include_raw: bool = False) -> dict:
        """转换为字典"""
        result = asdict(self)
        if not include_raw:
            result.pop("raw_text", None)
            result.pop("extra", None)
        return result


class DetailParser:
    """详情页文本解析器，从渲染后的页面文本中提取结构化字段"""

    def __init__(self, config_path: Optional[Path] = None):
        """
        初始化解析器

        Args:
            config_path: config.yaml 配置文件路径，None则使用默认模式
        """
        self._patterns: list[tuple[str, re.Pattern]] = DEFAULT_PATTERNS
        if config_path and config_path.exists():
            self._load_config(config_path)

    def _load_config(self, config_path: Path) -> None:
        """
        从config.yaml加载字段定义

        Args:
            config_path: 配置文件路径
        """
        try:
            config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
            detail_fields = config.get("detail_fields", [])
            if detail_fields:
                patterns = []
                for field_def in detail_fields:
                    key = field_def.get("key")
                    pattern_str = field_def.get("pattern")
                    if key and pattern_str:
                        patterns.append((key, re.compile(pattern_str, re.MULTILINE | re.DOTALL)))
                if patterns:
                    self._patterns = patterns
        except Exception:
            pass

    def parse(self, text: str, url: str = "", tid: str = "",
              title: str = "", crawl_time: str = "") -> DetailItem:
        """
        从详情页文本中提取结构化数据

        Args:
            text: Playwright渲染后的页面文本内容
            url: 详情页URL
            tid: 帖子ID
            title: 页面标题
            crawl_time: 抓取时间

        Returns:
            DetailItem: 结构化的详情页数据
        """
        item = DetailItem(
            url=url,
            tid=tid,
            title=title,
            crawl_time=crawl_time,
            raw_text=text[:5000],
        )

        # 提取元数据
        item.publish_date = self._extract_publish_date(text)
        item.view_count = self._extract_view_count(text)
        item.author = self._extract_author(text)

        # 提取地区
        province, city = self._extract_region(text)
        item.region_province = province
        item.region_city = city

        # 使用正则模式提取核心字段
        for key, pattern in self._patterns:
            value = self._match_first(pattern, text)
            if value:
                setattr(item, key, value)

        # 后处理：清洗数据
        self._post_process(item)

        return item

    def parse_html(self, html: str, url: str = "", tid: str = "",
                   title: str = "", crawl_time: str = "") -> DetailItem:
        """
        从HTML文本中提取结构化数据（使用BeautifulSoup清洗后再解析）

        Args:
            html: 详情页HTML内容
            url: 详情页URL
            tid: 帖子ID
            title: 页面标题
            crawl_time: 抓取时间

        Returns:
            DetailItem: 结构化的详情页数据
        """
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")
        # 移除脚本和样式标签
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        text = soup.get_text("\n")
        # 压缩多余空行
        text = re.sub(r"\n\s*\n+", "\n", text).strip()
        return self.parse(text, url, tid, title, crawl_time)

    @staticmethod
    def _match_first(pattern: re.Pattern, text: str) -> str:
        """匹配第一个结果并返回捕获组"""
        match = pattern.search(text)
        return match.group(1).strip() if match else ""

    @staticmethod
    def _extract_publish_date(text: str) -> str:
        """提取发布日期"""
        match = re.search(r"(\d{4}-\d{2}-\d{2})", text)
        return match.group(1) if match else ""

    @staticmethod
    def _extract_view_count(text: str) -> str:
        """提取浏览量"""
        match = re.search(r"(\d+)\s*(?:浏览|阅读|查看)", text)
        if not match:
            match = re.search(r"(?:浏览|阅读|查看)\s*(\d+)", text)
        if not match:
            match = re.search(r"\d{4}-\d{2}-\d{2}\s+(\d+)\s", text)
        return match.group(1) if match else ""

    @staticmethod
    def _extract_author(text: str) -> str:
        """提取作者信息"""
        match = re.search(r"(?:作者|发布者|发帖人)\s*[：:]\s*([^\n]+)", text)
        if not match:
            match = re.search(r"匿名", text)
            return "匿名" if match else ""
        return match.group(1).strip()

    @staticmethod
    def _extract_region(text: str) -> tuple[str, str]:
        """提取所属地区（省份/城市）"""
        # 匹配 "广东省 » 深圳市" 格式
        match = re.search(r"([^»\n]+\S)\s*»\s*([^»\n]+\S)", text)
        if match:
            province = re.sub(r"^(?:所属地区|地区)\s*[：:]\s*", "", match.group(1).strip())
            return province, match.group(2).strip()
        # 备用：匹配单个省市
        match = re.search(r"(\S{2,5}[省市])\s*[»\s]*\s*(\S{2,5}[市]?)", text)
        if match:
            return match.group(1).strip(), match.group(2).strip()
        return "", ""

    def _post_process(self, item: DetailItem) -> None:
        """后处理：清洗和标准化提取的数据"""
        # 清洗服务项目文本（去除标签和多余空格）
        if item.services:
            item.services = re.sub(r"\s+", "，", item.services).strip("，。.；;")
            item.services = re.sub(r"[,，]\s*(?:我要举报|我要收藏|举报|收藏)\s*", "", item.services)
            item.services = item.services.strip("，。.；;")

        # 清洗手机号码（去除空格和横线）
        if item.phone:
            item.phone = re.sub(r"[\s-]", "", item.phone)

        # 确保年龄是纯数字
        if item.age:
            item.age = re.sub(r"[^\d]", "", item.age)

        # 确保消费水平格式正确
        if item.price_range:
            item.price_range = re.sub(r"[^\d,-]", "", item.price_range)


def load_config(config_path: Optional[str] = None) -> dict:
    """
    加载全局配置文件

    Args:
        config_path: 配置文件路径，默认为项目根目录下的 config.yaml

    Returns:
        dict: 配置字典
    """
    if config_path is None:
        config_path = Path(__file__).parent / "config.yaml"
    else:
        config_path = Path(config_path)

    if not config_path.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")

    return yaml.safe_load(config_path.read_text(encoding="utf-8"))


def get_region_list(config: dict) -> list[dict]:
    """
    获取配置中的地区列表

    Args:
        config: 配置字典

    Returns:
        list[dict]: 地区列表
    """
    return config.get("regions", [])


def get_forum_list(config: dict) -> dict:
    """
    获取配置中的论坛板块列表

    Args:
        config: 配置字典

    Returns:
        dict: 论坛板块字典
    """
    return config.get("forums", {})


def get_region_by_name(config: dict, name: str) -> Optional[dict]:
    """
    根据地区名称查找地区配置

    Args:
        config: 配置字典
        name: 地区名称（如"北京市"、"广东省"）

    Returns:
        Optional[dict]: 地区配置项，找不到返回None
    """
    for region in config.get("regions", []):
        if region.get("name") == name or region.get("code") == name:
            return region
    return None
