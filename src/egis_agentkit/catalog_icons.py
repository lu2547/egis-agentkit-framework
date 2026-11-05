"""Stable presentation IDs shared by catalog clients; never remote image URLs."""

EXPERT_ICON_NAMES = (
    "developer",
    "architect",
    "founder",
    "embedded",
    "workbench",
    "wechat",
    "academic",
    "research",
    "knowledge",
    "fullstack",
    "python",
    "godot",
)
SKILL_ICON_NAMES = (
    "tencent-finance",
    "minimax-xlsx",
    "ai-news",
    "ppt",
    "web-access",
    "legal-search",
    "kdocs",
    "deep-thinking",
    "wecom",
    "minimax-docx",
    "tencent-pdfkit",
    "neodata",
    "minimax-h3",
    "dingtalk",
    "browser-skill",
)
CATALOG_ICON_IDS = frozenset(
    [
        *("expert:" + name for name in EXPERT_ICON_NAMES),
        *("skill:" + name for name in SKILL_ICON_NAMES),
        "spark",
        "book",
        "team",
        "code",
    ]
)


def catalog_icon_id(value, *, default="book"):
    if value is None:
        return default
    if not isinstance(value, str) or value not in CATALOG_ICON_IDS:
        raise ValueError("请选择标准图标库中的图标")
    return value
