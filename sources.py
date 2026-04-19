"""
Signal Radar — Multi-Source Opportunity Engine

Source adapters, keyword quality analyzer, and consensus-based opportunity scoring.
Each source returns a normalized SourceSignal; the engine combines them into a
single 0-100 opportunity score with evidence summary.

Sources:
  A. Google Trends   — existing pytrends velocity metrics (wrapper, no extra HTTP)
  B. Google Autocomplete — suggestqueries endpoint for demand/variant detection
  C. Google News RSS  — public RSS feed for media buzz measurement
  D. Shopee VN        — search hint API for marketplace validation + crowding proxy
"""

from __future__ import annotations

import html
import re
import unicodedata
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field

import requests
from requests.exceptions import RequestException, Timeout


# ---------------------------------------------------------------------------
# Keyword Normalization
# ---------------------------------------------------------------------------

def normalize_keyword(keyword: str) -> str:
    """Normalize keyword for consistent matching across sources.

    Lowercase, collapse whitespace, strip punctuation, NFD-decompose
    for Vietnamese diacritic-aware comparison.
    """
    kw = keyword.strip().lower()
    # Collapse repeated spaces
    kw = re.sub(r"\s+", " ", kw)
    # Remove noisy punctuation (keep Vietnamese diacritics)
    kw = re.sub(r"[^\w\sà-ỹÀ-Ỹ]", "", kw)
    return kw.strip()


def _strip_diacritics(text: str) -> str:
    """Remove Vietnamese diacritics for fallback matching.

    'mật ong' -> 'mat ong', 'tinh bột nghệ' -> 'tinh bot nghe'
    """
    # NFD decomposition then strip combining marks
    nfd = unicodedata.normalize("NFD", text)
    return re.sub(r"[\u0300-\u036f]", "", nfd)


# ---------------------------------------------------------------------------
# Geo-aware locale mapping for Google endpoints
# pytrends uses "vi-VN" but suggest/news need "vi" (hl) and "vn" (gl)
# ---------------------------------------------------------------------------

_GEO_LOCALE: dict[str, dict[str, str]] = {
    "VN": {"hl": "vi", "gl": "vn", "ceid": "VN:vi"},
    "US": {"hl": "en", "gl": "us", "ceid": "US:en"},
    "WW": {"hl": "en", "gl": "",   "ceid": ""},
}


# ---------------------------------------------------------------------------
# Data Structures
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SourceSignal:
    """Normalized result from a single evidence source."""
    source_name: str        # "google_trends" | "autocomplete" | "news"
    keyword: str
    geo: str
    score: float            # 0.0-1.0 normalized demand indicator
    confidence: float       # 0.0-1.0 reliability of this signal
    notes: str              # short explanation
    success: bool           # False if source failed or timed out
    raw_data: dict = field(default_factory=dict)


@dataclass
class KeywordQuality:
    """Pre-analysis of keyword intent and ambiguity."""
    ambiguity_score: float          # 0.0-1.0
    commercial_intent_score: float  # 0.0-1.0
    keyword_quality_label: str      # COMMERCIAL|INFORMATIONAL|BRAND|AMBIGUOUS|BROAD|PERSON
    refined_keywords: list[str]     # better alternatives for ambiguous keywords


@dataclass
class KeywordResolution:
    """Full keyword analysis with source-based evidence and refinement suggestions."""
    original_keyword: str
    quality_label: str              # COMMERCIAL|INFORMATIONAL|BRAND|AMBIGUOUS|BROAD|PERSON
    ambiguity_score: float          # 0.0-1.0
    commercial_intent_score: float  # 0.0-1.0
    is_weak: bool                   # keyword needs refinement before action
    resolver_reason: str            # Vietnamese explanation of quality assessment
    refined_keywords: list[str]     # better alternatives (source-based when possible)
    next_action: str                # suggested next step for user


@dataclass
class OpportunityResult:
    """Final consensus score combining all evidence layers."""
    keyword: str
    geo: str
    domain: str
    # Existing velocity metrics (from Google Trends)
    interest: float
    wow_growth_pct: float
    acceleration_pct: float
    consistency_pct: float
    peak_position_pct: float
    confidence: int
    status: str
    # Multi-source fields
    opportunity_score: float        # 0-100 consensus score
    source_count: int               # successful sources (1-4)
    source_agreement: float         # 0.0-1.0
    keyword_quality_label: str
    evidence_summary: str           # Vietnamese text
    action_label: str               # GO|WATCH|AVOID
    action_reason: str
    # Marketplace fields
    marketplace_presence: float     # 0.0-1.0 does keyword exist on marketplace
    marketplace_intent: float       # 0.0-1.0 buyer intent from marketplace hints
    crowding_risk: float            # 0.0-1.0 competition level
    normalized_keyword: str         # normalized form for matching
    sources: list[SourceSignal] = field(default_factory=list)
    keyword_quality: KeywordQuality | None = None
    resolution: KeywordResolution | None = None


# ---------------------------------------------------------------------------
# Keyword Quality Analyzer
# ---------------------------------------------------------------------------

_BRAND_PATTERNS: list[str] = [
    "shopee", "lazada", "tiktok shop", "tiki", "amazon", "tiktok",
    "facebook", "google", "apple", "samsung", "xiaomi", "nike", "adidas",
    "gucci", "zara", "h&m", "unilever", "vinamilk", "lotte", "sony",
]

_PERSON_PATTERN = re.compile(r"^[A-ZÀ-Ỹ][a-zà-ỹ]+(\s[A-ZÀ-Ỹ][a-zà-ỹ]+)+$")

# Common Vietnamese last names for person detection
_VIETNAMESE_LAST_NAMES: set[str] = {
    "nguyễn", "trần", "lê", "phạm", "hoàng", "huỳnh", "phan", "vũ", "võ",
    "đặng", "bùi", "đỗ", "ngô", "dương", "lý", "trịnh", "đinh", "lưu",
}

# Terms that indicate person/entity in autocomplete suggestions
_PERSON_EVIDENCE_TERMS: list[str] = [
    "tiktok", "instagram", "facebook", "youtube", "idol", "hot girl",
    "ca sĩ", "diễn viên", "vod", "streamer", "influencer", "channel",
]

# Terms that confirm entity/brand, not a product keyword
_ENTITY_INDICATORS: list[str] = [
    "chính chủ", "nick", "tài khoản", "fanpage", " kênh", "channel",
    "official", "verify",
]

_BROAD_TERMS: set[str] = {
    "mua", "bán", "giá", "tốt", "hay", "mới", "hot", "trend",
    "review", "top", "best", "how", "what", "why", "cách", "tại sao",
    "xu hướng", "tin tức", "news", "trending",
}

_COMMERCIAL_PATTERNS: list[str] = [
    "giá", "bán", "mua", "nhập", "sỉ", "lẻ", "giảm giá", "khuyến mãi",
    "deal", "coupon", "sale", "order", "đặt hàng", "shop", "store",
    "báo giá", "wholesale", "retail", "nhập hàng", "tồn kho",
    "sản phẩm", "hãng", "chính hãng",
]

_PRODUCT_INDICATORS: list[str] = [
    "tinh", "mật", "bột", "dầu", "kem", "serum", "viên",
    "set", "combo", "hộp", "chai", "tuýp", "gói",
    "nước", " viên", "tắm", "dưỡng", "trị", "chống",
]


def analyze_keyword_quality(keyword: str) -> KeywordQuality:
    """Analyze keyword intent and ambiguity before fetching sources."""
    kw_lower = keyword.lower().strip()
    words = kw_lower.split()

    # --- Ambiguity scoring ---
    ambiguity = 0.0
    if len(words) <= 1:
        ambiguity += 0.3
    if kw_lower in _BROAD_TERMS:
        ambiguity += 0.4
    if _PERSON_PATTERN.match(keyword.strip()):
        ambiguity += 0.3

    is_brand = any(brand in kw_lower for brand in _BRAND_PATTERNS)
    if is_brand:
        ambiguity += 0.2
    ambiguity = min(ambiguity, 1.0)

    # --- Commercial intent scoring ---
    commercial = 0.0
    if any(p in kw_lower for p in _COMMERCIAL_PATTERNS):
        commercial += 0.4
    if len(words) >= 2:
        commercial += 0.2
    if any(w in kw_lower for w in _PRODUCT_INDICATORS):
        commercial += 0.3
    commercial = min(commercial, 1.0)

    # --- Quality label ---
    if is_brand:
        label = "BRAND"
    elif ambiguity >= 0.6:
        label = "AMBIGUOUS"
    elif ambiguity >= 0.4 and len(words) <= 1:
        label = "BROAD"
    elif commercial >= 0.5:
        label = "COMMERCIAL"
    else:
        label = "INFORMATIONAL"

    # --- Refined keywords for ambiguous/broad ---
    refined: list[str] = []
    if label in ("AMBIGUOUS", "BROAD") and len(words) <= 2:
        refined = [
            f"{keyword} giá",
            f"{keyword} mua ở đâu",
            f"{keyword} review",
        ]

    return KeywordQuality(
        ambiguity_score=ambiguity,
        commercial_intent_score=commercial,
        keyword_quality_label=label,
        refined_keywords=refined,
    )


# ---------------------------------------------------------------------------
# Keyword Resolver — source-aware keyword quality + refinement
# ---------------------------------------------------------------------------

def _is_likely_person(keyword: str, ac_suggestions: list[str] | None = None) -> bool:
    """Detect if keyword is likely a person or entity name.

    Uses heuristics + autocomplete evidence.
    """
    words = keyword.strip().split()
    kw_lower = keyword.lower().strip()

    # Vietnamese name: LastName MiddleName FirstName (3+ words, first word is known last name)
    if len(words) >= 3 and words[0].lower() in _VIETNAMESE_LAST_NAMES:
        return True

    # Western-style capitalized name: "Mai Hằng", "Ngọc Trinh"
    if _PERSON_PATTERN.match(keyword.strip()) and len(words) >= 2:
        # Exclude if it contains commercial/product modifiers
        if not any(p in kw_lower for p in _PRODUCT_INDICATORS + _COMMERCIAL_PATTERNS):
            return True

    # Autocomplete evidence: suggestions are person-oriented
    if ac_suggestions:
        person_count = sum(
            1 for s in ac_suggestions[:8]
            if any(t in s.lower() for t in _PERSON_EVIDENCE_TERMS)
        )
        if person_count >= 2:
            return True

    return False


def _generate_source_based_variants(
    keyword: str,
    ac_signal: SourceSignal | None = None,
    shopee_signal: SourceSignal | None = None,
    quality: KeywordQuality | None = None,
) -> list[str]:
    """Generate refined keyword variants using source evidence.

    Prefers variants that actually exist in autocomplete/marketplace data
    over static templates.
    """
    variants: list[str] = []
    seen: set[str] = {keyword.lower().strip()}
    kw_lower = keyword.lower().strip()

    # Collect from autocomplete suggestions
    if ac_signal and ac_signal.success and ac_signal.raw_data.get("suggestions"):
        for sug in ac_signal.raw_data["suggestions"][:10]:
            sug_lower = sug.lower()
            if sug_lower in seen:
                continue
            # Prefer suggestions containing the original keyword
            if kw_lower in sug_lower:
                # Check commercial relevance
                if any(p in sug_lower for p in _COMMERCIAL_PATTERNS + _PRODUCT_INDICATORS + _MARKETPLACE_COMMERCE_TERMS):
                    variants.append(sug)
                    seen.add(sug_lower)

    # Collect from Shopee hints
    if shopee_signal and shopee_signal.success:
        for hint in shopee_signal.raw_data.get("hints", [])[:5]:
            hint_lower = hint.lower()
            if hint_lower not in seen and kw_lower in hint_lower:
                variants.append(hint)
                seen.add(hint_lower)

    # Fallback to context-aware static templates if no source variants found
    if not variants:
        label = quality.keyword_quality_label if quality else ""
        if label == "BRAND":
            brand_name = keyword.strip()
            variants = [f"{brand_name} sản phẩm", f"{brand_name} chính hãng"]
        elif label == "PERSON":
            variants = []
        else:
            variants = [
                f"{keyword} giá", f"{keyword} review", f"{keyword} mua ở đâu",
            ]

    # Deduplicate and limit
    return variants[:5]


def resolve_keyword(
    keyword: str,
    geo: str = "VN",
    ac_signal: SourceSignal | None = None,
    shopee_signal: SourceSignal | None = None,
) -> KeywordResolution:
    """Resolve keyword quality using heuristics + source evidence.

    Combines static quality analysis with autocomplete/marketplace signals
    to classify keywords and suggest better variants.

    Returns KeywordResolution with assessment, reason, and refined variants.
    """
    # Step 1: Static quality analysis
    quality = analyze_keyword_quality(keyword)
    kw_lower = keyword.lower().strip()

    # Step 2: Fetch autocomplete evidence if not provided
    if ac_signal is None:
        ac_signal = fetch_autocomplete_signal(keyword, geo)
    ac_suggestions = (
        ac_signal.raw_data.get("suggestions", []) if ac_signal.success else []
    )

    # Step 3: Detect PERSON label (heuristic + source evidence)
    is_person = _is_likely_person(keyword, ac_suggestions)
    if is_person and quality.keyword_quality_label not in ("COMMERCIAL",):
        quality_label = "PERSON"
    else:
        quality_label = quality.keyword_quality_label

    # Step 4: Re-score with source evidence
    ambiguity = quality.ambiguity_score
    commercial = quality.commercial_intent_score

    # Boost commercial intent if autocomplete has commercial suggestions
    if ac_signal.success:
        ac_commercial = sum(
            1 for s in ac_suggestions[:8]
            if any(p in s.lower() for p in _COMMERCIAL_PATTERNS + _PRODUCT_INDICATORS)
        )
        if ac_commercial >= 3:
            commercial = min(commercial + 0.2, 1.0)
        elif ac_commercial >= 1:
            commercial = min(commercial + 0.1, 1.0)

    # Boost ambiguity if autocomplete has entity indicators
    if ac_signal.success:
        entity_count = sum(
            1 for s in ac_suggestions[:8]
            if any(t in s.lower() for t in _ENTITY_INDICATORS)
        )
        if entity_count >= 2:
            ambiguity = min(ambiguity + 0.2, 1.0)

    # Step 5: Determine weakness
    is_weak = quality_label in ("AMBIGUOUS", "BROAD", "PERSON") or (
        quality_label == "BRAND" and commercial < 0.3
    )

    # Step 6: Generate context-aware refined variants
    if shopee_signal is None:
        shopee_signal = fetch_shopee_signal(keyword, geo)
    refined = _generate_source_based_variants(
        keyword, ac_signal, shopee_signal, quality,
    )

    # Step 7: Build resolver reason
    reasons: list[str] = []
    if quality_label == "PERSON":
        reasons.append("Từ khóa có vẻ là tên người, không phải sản phẩm")
    elif quality_label == "BRAND":
        reasons.append("Từ khóa là thương hiệu — cần chỉ rõ sản phẩm cụ thể")
    elif quality_label == "AMBIGUOUS":
        reasons.append("Từ khóa mơ hồ — có nhiều nghĩa khác nhau")
    elif quality_label == "BROAD":
        reasons.append("Từ khóa quá rộng — khó xác định nhu cầu mua hàng")
    elif quality_label == "COMMERCIAL":
        reasons.append("Từ khóa có ý định thương mại tốt")
    else:
        reasons.append("Từ khóa mang tính thông tin, chưa rõ ý định mua")

    if commercial >= 0.5:
        reasons.append("có tín hiệu thương mại")
    if ambiguity >= 0.5:
        reasons.append("độ mơ hồ cao")

    # Step 8: Suggest next action
    if is_weak and refined:
        next_action = f"Dùng /compare {refined[0]}, {refined[1]} để so sánh biến thể cụ thể hơn"
    elif is_weak:
        next_action = "Thử từ khóa cụ thể hơn (thêm loại sản phẩm, thương hiệu)"
    elif quality_label == "COMMERCIAL":
        next_action = "Từ khóa tốt — dùng /track để theo dõi tự động"
    else:
        next_action = "Theo dõi hoặc so sánh với từ khóa tương tự để xác nhận"

    return KeywordResolution(
        original_keyword=keyword,
        quality_label=quality_label,
        ambiguity_score=ambiguity,
        commercial_intent_score=commercial,
        is_weak=is_weak,
        resolver_reason=". ".join(reasons) + ".",
        refined_keywords=refined,
        next_action=next_action,
    )


# ---------------------------------------------------------------------------
# Source Adapter A: Google Trends (wrapper — no extra HTTP)
# ---------------------------------------------------------------------------

def fetch_google_trends_signal(
    keyword: str,
    geo: str,
    interest: float,
    wow_growth_pct: float,
    confidence: int,
    status: str,
) -> SourceSignal:
    """Wrap existing velocity_engine output as a SourceSignal."""
    score = min(interest / 100.0, 1.0)
    conf = min(confidence / 100.0, 1.0)

    wow_display = "INF" if wow_growth_pct == float("inf") else f"{wow_growth_pct:.1f}%"
    notes = f"Google Trends: interest={interest:.0f}, WoW={wow_display}, status={status}"

    return SourceSignal(
        source_name="google_trends",
        keyword=keyword,
        geo=geo,
        score=score,
        confidence=conf,
        notes=notes,
        success=True,
        raw_data={
            "interest": interest,
            "wow_growth_pct": wow_growth_pct,
            "confidence": confidence,
            "status": status,
        },
    )


# ---------------------------------------------------------------------------
# Source Adapter B: Google Autocomplete
# ---------------------------------------------------------------------------

_AUTOCOMPLETE_URL = "http://suggestqueries.google.com/complete/search"
_AUTOCOMPLETE_TIMEOUT = 8


def fetch_autocomplete_signal(
    keyword: str,
    geo: str,
    timeout: int = _AUTOCOMPLETE_TIMEOUT,
) -> SourceSignal:
    """Fetch Google Autocomplete suggestions to gauge demand and commercial variants."""
    locale = _GEO_LOCALE.get(geo, _GEO_LOCALE["VN"])
    params = {
        "client": "chrome",
        "q": keyword,
        "hl": locale["hl"],
    }
    if locale["gl"]:
        params["gl"] = locale["gl"]

    try:
        resp = requests.get(
            _AUTOCOMPLETE_URL,
            params=params,
            timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0 (compatible; SignalRadar/1.0)"},
        )
        resp.raise_for_status()

        data = resp.json()
        suggestions: list[str] = data[1] if len(data) > 1 else []

        if not suggestions:
            return SourceSignal(
                source_name="autocomplete",
                keyword=keyword, geo=geo,
                score=0.0, confidence=0.3,
                notes="Không có gợi ý autocomplete.",
                success=True,
                raw_data={"suggestions": []},
            )

        sug_count = len(suggestions)
        score = min(sug_count / 10.0, 1.0)

        commercial_sugs = [
            s for s in suggestions
            if any(p in s.lower() for p in _COMMERCIAL_PATTERNS)
        ]
        commercial_ratio = len(commercial_sugs) / max(sug_count, 1)

        if commercial_ratio > 0.3:
            score = min(score + 0.2, 1.0)

        confidence = min(0.3 + (sug_count / 10.0) * 0.7, 1.0)

        top_3 = ", ".join(suggestions[:3])
        notes = f"Autocomplete: {sug_count} gợi ý. Top: {top_3}"

        return SourceSignal(
            source_name="autocomplete",
            keyword=keyword, geo=geo,
            score=score, confidence=confidence,
            notes=notes, success=True,
            raw_data={
                "suggestions": suggestions,
                "commercial_count": len(commercial_sugs),
            },
        )

    except Timeout:
        return SourceSignal(
            source_name="autocomplete", keyword=keyword, geo=geo,
            score=0.0, confidence=0.1,
            notes="Autocomplete timeout.", success=False,
        )
    except RequestException as exc:
        return SourceSignal(
            source_name="autocomplete", keyword=keyword, geo=geo,
            score=0.0, confidence=0.1,
            notes=f"Autocomplete lỗi: {str(exc)[:60]}", success=False,
        )
    except Exception:
        return SourceSignal(
            source_name="autocomplete", keyword=keyword, geo=geo,
            score=0.0, confidence=0.1,
            notes="Autocomplete lỗi không xác định.", success=False,
        )


# ---------------------------------------------------------------------------
# Source Adapter C: Google News RSS
# ---------------------------------------------------------------------------

_NEWS_RSS_URL = "https://news.google.com/rss/search"
_NEWS_TIMEOUT = 10


def fetch_news_signal(
    keyword: str,
    geo: str,
    timeout: int = _NEWS_TIMEOUT,
) -> SourceSignal:
    """Fetch Google News RSS to detect media buzz around a keyword."""
    locale = _GEO_LOCALE.get(geo, _GEO_LOCALE["VN"])
    params = {
        "q": keyword,
        "hl": locale["hl"],
    }
    if locale["gl"]:
        params["gl"] = locale["gl"]
    if locale["ceid"]:
        params["ceid"] = locale["ceid"]

    try:
        resp = requests.get(
            _NEWS_RSS_URL,
            params=params,
            timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0 (compatible; SignalRadar/1.0)"},
        )
        resp.raise_for_status()

        root = ET.fromstring(resp.text)
        items = root.findall(".//item")
        article_count = len(items)

        if article_count == 0:
            return SourceSignal(
                source_name="news",
                keyword=keyword, geo=geo,
                score=0.0, confidence=0.4,
                notes="Không có tin tức gần đây.",
                success=True,
                raw_data={"article_count": 0},
            )

        score = min(article_count / 20.0, 1.0)
        confidence = min(0.3 + (article_count / 10.0) * 0.5, 0.9)

        headlines = []
        for item in items[:3]:
            title_el = item.find("title")
            if title_el is not None and title_el.text:
                clean = html.unescape(title_el.text)
                headlines.append(clean[:60])

        top_str = "; ".join(headlines)
        notes = f"News: {article_count} bài viết. Gần nhất: {top_str}"

        return SourceSignal(
            source_name="news",
            keyword=keyword, geo=geo,
            score=score, confidence=confidence,
            notes=notes, success=True,
            raw_data={"article_count": article_count, "headlines": headlines},
        )

    except Timeout:
        return SourceSignal(
            source_name="news", keyword=keyword, geo=geo,
            score=0.0, confidence=0.1,
            notes="News RSS timeout.", success=False,
        )
    except ET.ParseError:
        return SourceSignal(
            source_name="news", keyword=keyword, geo=geo,
            score=0.0, confidence=0.1,
            notes="News RSS parse lỗi.", success=False,
        )
    except RequestException as exc:
        return SourceSignal(
            source_name="news", keyword=keyword, geo=geo,
            score=0.0, confidence=0.1,
            notes=f"News RSS lỗi: {str(exc)[:60]}", success=False,
        )
    except Exception:
        return SourceSignal(
            source_name="news", keyword=keyword, geo=geo,
            score=0.0, confidence=0.1,
            notes="News RSS lỗi không xác định.", success=False,
        )


# ---------------------------------------------------------------------------
# Source Adapter D: Shopee VN — marketplace validation + crowding proxy
# ---------------------------------------------------------------------------

_SHOPEE_HINT_URL = "https://shopee.vn/api/v4/search/search_hint"
_SHOPEE_TIMEOUT = 10

_SHOPEE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://shopee.vn/",
    "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
    "X-Shopee-Language": "vi",
    "X-API-SOURCE": "pc",
}

# Marketplace-specific commerce modifiers in suggestions
_MARKETPLACE_COMMERCE_TERMS = [
    "nguyên chất", "chính hãng", "giá rẻ", "cao cấp", "nhập khẩu",
    "hot", "bán chạy", "freeship", "giảm giá", "khuyến mãi",
    "combo", "set", "hộp", "chai", "tuýp", "gói", "viên",
    "ml", "g", "kg", "gram",
]


def fetch_shopee_signal(
    keyword: str,
    geo: str,
    timeout: int = _SHOPEE_TIMEOUT,
) -> SourceSignal:
    """Fetch Shopee VN search hints to validate marketplace presence.

    Measures:
    - Marketplace presence: does Shopee recognize this keyword?
    - Buyer intent: are suggestions product/transaction-oriented?
    - Crowding proxy: how many suggestions (more = more crowded market)
    """
    # Shopee adapter only supports VN market
    if geo != "VN":
        return SourceSignal(
            source_name="shopee", keyword=keyword, geo=geo,
            score=0.0, confidence=0.0,
            notes="Shopee chỉ hỗ trợ thị trường VN.", success=False,
            raw_data={"marketplace_presence": 0.0, "marketplace_intent": 0.0,
                      "crowding_risk": 0.0, "hint_count": 0},
        )

    norm_kw = normalize_keyword(keyword)
    norm_kw_stripped = _strip_diacritics(norm_kw)

    try:
        resp = requests.get(
            _SHOPEE_HINT_URL,
            params={"keyword": keyword, "limit": 20},
            headers=_SHOPEE_HEADERS,
            timeout=timeout,
        )
        resp.raise_for_status()

        data = resp.json()
        hints: list[dict] = data.get("keywords") or []
        hint_count = len(hints)

        if hint_count == 0:
            return SourceSignal(
                source_name="shopee", keyword=keyword, geo=geo,
                score=0.0, confidence=0.4,
                notes="Shopee không có gợi ý cho từ khóa này.",
                success=True,
                raw_data={"marketplace_presence": 0.0, "marketplace_intent": 0.0,
                          "crowding_risk": 0.0, "hint_count": 0, "hints": []},
            )

        hint_keywords = [h.get("keyword", "") for h in hints]

        # --- Marketplace presence ---
        # Check if original keyword (or close variant) appears in hints
        presence = 0.0
        for hk in hint_keywords:
            hk_norm = normalize_keyword(hk)
            if norm_kw in hk_norm or norm_kw_stripped in _strip_diacritics(hk_norm):
                presence = 1.0
                break
        if presence == 0.0 and hint_count >= 5:
            presence = 0.5  # keyword returns results but isn't directly matched

        # --- Buyer intent ---
        # Check how many hints contain commerce-oriented modifiers
        commercial_hints = 0
        for hk in hint_keywords:
            hk_lower = hk.lower()
            if any(t in hk_lower for t in _MARKETPLACE_COMMERCE_TERMS):
                commercial_hints += 1
            # Also check if hint is product-specific (longer than 2 words)
            if len(hk_lower.split()) >= 3:
                commercial_hints += 1

        intent_score = min(commercial_hints / max(hint_count, 1) * 1.5, 1.0)

        # --- Crowding risk ---
        # More hints = more established/crowded category
        # 12 hints is typical for established categories on Shopee
        if hint_count >= 12:
            crowding = 0.7  # high competition
        elif hint_count >= 6:
            crowding = 0.4  # moderate
        else:
            crowding = 0.2  # less crowded

        # Overall marketplace score
        score = (presence * 0.4 + intent_score * 0.4 + (1 - crowding) * 0.2)
        confidence = min(0.3 + presence * 0.4 + intent_score * 0.3, 0.9)

        top_3 = ", ".join(hint_keywords[:3])
        crowding_label = "cao" if crowding >= 0.6 else "trung bình" if crowding >= 0.3 else "thấp"
        notes = f"Shopee: {hint_count} gợi ý. Top: {top_3}. Cạnh tranh: {crowding_label}"

        return SourceSignal(
            source_name="shopee", keyword=keyword, geo=geo,
            score=score, confidence=confidence,
            notes=notes, success=True,
            raw_data={
                "marketplace_presence": round(presence, 2),
                "marketplace_intent": round(intent_score, 2),
                "crowding_risk": round(crowding, 2),
                "hint_count": hint_count,
                "hints": hint_keywords[:5],
            },
        )

    except Timeout:
        return SourceSignal(
            source_name="shopee", keyword=keyword, geo=geo,
            score=0.0, confidence=0.1,
            notes="Shopee timeout.", success=False,
            raw_data={"marketplace_presence": 0.0, "marketplace_intent": 0.0,
                      "crowding_risk": 0.0, "hint_count": 0},
        )
    except RequestException as exc:
        return SourceSignal(
            source_name="shopee", keyword=keyword, geo=geo,
            score=0.0, confidence=0.1,
            notes=f"Shopee lỗi: {str(exc)[:60]}", success=False,
            raw_data={"marketplace_presence": 0.0, "marketplace_intent": 0.0,
                      "crowding_risk": 0.0, "hint_count": 0},
        )
    except Exception:
        return SourceSignal(
            source_name="shopee", keyword=keyword, geo=geo,
            score=0.0, confidence=0.1,
            notes="Shopee lỗi không xác định.", success=False,
            raw_data={"marketplace_presence": 0.0, "marketplace_intent": 0.0,
                      "crowding_risk": 0.0, "hint_count": 0},
        )


# ---------------------------------------------------------------------------
# Source Agreement Calculator
# ---------------------------------------------------------------------------

def _compute_source_agreement(signals: list[SourceSignal]) -> float:
    """Measure how well sources agree. Returns 0.0-1.0."""
    successful = [s for s in signals if s.success]
    if len(successful) < 2:
        return 0.5

    scores = [s.score for s in successful]
    mean = sum(scores) / len(scores)
    variance = sum((s - mean) ** 2 for s in scores) / len(scores)
    std_dev = variance ** 0.5

    agreement = max(0.0, 1.0 - std_dev * 2.0)
    return round(agreement, 2)


# ---------------------------------------------------------------------------
# Consensus Opportunity Scorer
# ---------------------------------------------------------------------------

def compute_opportunity_score(
    quality: KeywordQuality,
    signals: list[SourceSignal],
    agreement: float,
    wow_growth_pct: float,
    acceleration_pct: float,
    consistency_pct: float,
    confidence: int,
    marketplace_presence: float = 0.0,
    marketplace_intent: float = 0.0,
    crowding_risk: float = 0.0,
) -> tuple[float, str, str]:
    """Compute the consensus opportunity score (0-100).

    Formula:
        demand(20) + acceleration(15) + agreement(10) + commercial_intent(10)
        + stability(10) + confidence(5) + marketplace_presence(15)
        + marketplace_intent(10) - ambiguity(8) - crowding_risk(12)
        then adjusted by source count modifier.

    Source weighting:
        - Google Trends = demand momentum (via demand_score)
        - Autocomplete = intent/query quality (via demand_score)
        - News = buzz, reduced weight for broad/brand keywords (via demand_score)
        - Marketplace = strongest commercial validation (via dedicated weights)

    Returns: (opportunity_score, action_label, action_reason)
    """
    successful = [s for s in signals if s.success]
    source_count = len(successful)

    # Source-weighted demand score
    # News gets reduced weight for broad/brand keywords
    source_weights: dict[str, float] = {
        "google_trends": 1.0,
        "autocomplete": 0.8,
        "news": 0.5,       # lower — buzz doesn't equal buyer intent
        "shopee": 1.2,     # highest — marketplace = real commerce signal
    }

    # Further reduce news weight for ambiguous/broad keywords
    if quality.keyword_quality_label in ("AMBIGUOUS", "BROAD", "BRAND"):
        source_weights["news"] = 0.2

    if successful:
        weighted_num = 0.0
        weighted_den = 0.0
        for s in successful:
            w = source_weights.get(s.source_name, 1.0)
            weighted_num += s.score * s.confidence * w
            weighted_den += s.confidence * w
        demand_score = weighted_num / weighted_den if weighted_den > 0 else 0.0
    else:
        demand_score = 0.0

    # Component scores
    wow = 999.0 if wow_growth_pct == float("inf") else wow_growth_pct
    accel_score = min(max(wow, 0) / 300.0, 1.0)
    stability_score = min(consistency_pct / 100.0, 1.0)
    commercial = quality.commercial_intent_score
    ambiguity = quality.ambiguity_score
    conf_score = min(confidence / 100.0, 1.0)

    # Weighted sum with marketplace components
    raw = (
        demand_score * 20
        + accel_score * 15
        + agreement * 10
        + commercial * 10
        + stability_score * 10
        + conf_score * 5
        + marketplace_presence * 15
        + marketplace_intent * 10
        - ambiguity * 8
        - crowding_risk * 12
    )

    # Source count modifier
    if source_count >= 4:
        raw = min(raw * 1.08, 100)
    elif source_count >= 3:
        raw = min(raw * 1.03, 100)
    elif source_count == 2:
        raw = raw * 0.93
    elif source_count == 1:
        raw = raw * 0.80

    # Marketplace absence penalty: strong search buzz without marketplace = lower trust
    has_shopee = any(s.source_name == "shopee" and s.success for s in signals)
    has_strong_buzz = demand_score > 0.5 and not has_shopee
    if has_strong_buzz:
        raw = raw * 0.85  # 15% penalty for missing marketplace validation

    opportunity = round(max(0, min(raw, 100)), 1)

    # Action label from opportunity score
    if opportunity >= 65:
        action_label = "GO"
        action_reason = f"Điểm cơ hội {opportunity}/100 — bằng chứng mạnh từ {source_count} nguồn."
    elif opportunity >= 35:
        action_label = "WATCH"
        action_reason = f"Điểm cơ hội {opportunity}/100 — cần thêm xác nhận."
    else:
        action_label = "AVOID"
        action_reason = f"Điểm cơ hội {opportunity}/100 — tín hiệu yếu hoặc không rõ ràng."

    return opportunity, action_label, action_reason


# ---------------------------------------------------------------------------
# Evidence Summary Generator
# ---------------------------------------------------------------------------

def _build_evidence_summary(
    quality: KeywordQuality,
    signals: list[SourceSignal],
    agreement: float,
    source_count: int,
    marketplace_presence: float = 0.0,
    marketplace_intent: float = 0.0,
    crowding_risk: float = 0.0,
) -> str:
    """Build a short Vietnamese evidence summary with marketplace data."""
    parts: list[str] = []

    # Source count
    if source_count >= 4:
        parts.append(f"{source_count}/4 nguồn dữ liệu thu thập thành công")
    elif source_count == 3:
        parts.append("3/4 nguồn dữ liệu khả dụng")
    elif source_count == 2:
        parts.append("2/4 nguồn dữ liệu khả dụng")
    elif source_count == 1:
        parts.append("Chỉ 1/4 nguồn dữ liệu khả dụng (bằng chứng rất hạn chế)")
    else:
        parts.append("Không thu thập được dữ liệu từ các nguồn phụ")

    # Source-specific findings
    for sig in signals:
        if not sig.success:
            continue
        if sig.source_name == "autocomplete":
            sugs = sig.raw_data.get("suggestions", [])
            com_count = sig.raw_data.get("commercial_count", 0)
            if com_count > 0:
                parts.append(f"Autocomplete phát hiện {com_count} biến thể thương mại")
            elif sugs:
                parts.append(f"Autocomplete có {len(sugs)} gợi ý liên quan")
        elif sig.source_name == "news":
            count = sig.raw_data.get("article_count", 0)
            if count > 10:
                parts.append(f"News: {count} bài viết — media buzz cao")
            elif count > 0:
                parts.append(f"News: {count} bài viết gần đây")
        elif sig.source_name == "shopee":
            hint_count = sig.raw_data.get("hint_count", 0)
            mp = sig.raw_data.get("marketplace_presence", 0)
            cr = sig.raw_data.get("crowding_risk", 0)
            if mp >= 0.8:
                parts.append(f"Shopee xác nhận nhu cầu sản phẩm ({hint_count} gợi ý)")
            elif mp >= 0.4:
                parts.append(f"Shopee có gợi ý liên quan ({hint_count})")
            else:
                parts.append("Shopee chưa xác nhận rõ nhu cầu marketplace")
            if cr >= 0.6:
                parts.append("Marketplace đã đông — rủi ro cạnh tranh cao")
            elif cr >= 0.3:
                parts.append("Cạnh tranh marketplace trung bình")

    # Marketplace absence warning
    has_shopee = any(s.source_name == "shopee" and s.success for s in signals)
    if not has_shopee:
        parts.append("Thiếu tín hiệu marketplace — kết quả chỉ dựa trên search/news")

    # Agreement
    if agreement >= 0.7:
        parts.append("Các nguồn đồng thuận cao")
    elif agreement >= 0.4:
        parts.append("Các nguồn partly đồng thuận")
    else:
        parts.append("Các nguồn cho tín hiệu khác nhau")

    # Quality label
    ql = quality.keyword_quality_label
    if ql == "AMBIGUOUS":
        parts.append("Từ khóa mơ hồ — cân nhắc từ khóa cụ thể hơn")
    elif ql == "BRAND":
        parts.append("Từ khóa là thương hiệu — kết quả có thể bị ảnh hưởng")
    elif ql == "BROAD":
        parts.append("Từ khóa quá rộng — khó chuyển thành cơ hội bán hàng rõ ràng")

    return ". ".join(parts) + "."


# ---------------------------------------------------------------------------
# Main Entry Point: multi_source_engine
# ---------------------------------------------------------------------------

def multi_source_engine(
    velocity_results,   # pd.DataFrame from velocity_engine
    geo: str = "VN",
    domain_override: str | None = None,
) -> list[OpportunityResult]:
    """Run multi-source analysis on velocity_engine results.

    For each keyword: normalize → analyze quality → fetch all source signals
    (autocomplete, news, Shopee) → compute consensus opportunity score with
    marketplace validation → build evidence summary.

    Returns list of OpportunityResult sorted by opportunity_score descending.
    """
    if velocity_results.empty:
        return []

    results: list[OpportunityResult] = []

    for _, row in velocity_results.iterrows():
        keyword = str(row["keyword"])
        interest = float(row["interest"])
        wow = row["wow_growth_pct"]
        accel = float(row["acceleration_pct"])
        consist = float(row["consistency_pct"])
        peak = float(row["peak_position_pct"])
        conf = int(row["confidence"])
        status = str(row["status"])
        domain = domain_override or str(row.get("domain", "General"))
        if not domain or domain == "nan":
            from signal_radar import detect_domain
            domain = detect_domain(keyword)

        norm_kw = normalize_keyword(keyword)

        # Step 1: Keyword quality
        quality = analyze_keyword_quality(keyword)

        # Step 2: Fetch all source signals
        trends_signal = fetch_google_trends_signal(
            keyword, geo, interest, wow, conf, status,
        )
        autocomplete_signal = fetch_autocomplete_signal(keyword, geo)
        news_signal = fetch_news_signal(keyword, geo)
        shopee_signal = fetch_shopee_signal(keyword, geo)

        signals = [trends_signal, autocomplete_signal, news_signal, shopee_signal]
        successful = [s for s in signals if s.success]
        source_count = len(successful)

        # Step 3: Resolve keyword (reuses already-fetched signals)
        resolution = resolve_keyword(
            keyword, geo,
            ac_signal=autocomplete_signal,
            shopee_signal=shopee_signal,
        )
        # Update quality label from resolver (may add PERSON detection)
        quality = KeywordQuality(
            ambiguity_score=resolution.ambiguity_score,
            commercial_intent_score=resolution.commercial_intent_score,
            keyword_quality_label=resolution.quality_label,
            refined_keywords=resolution.refined_keywords,
        )

        # Step 4: Extract marketplace metrics
        marketplace_presence = shopee_signal.raw_data.get("marketplace_presence", 0.0)
        marketplace_intent = shopee_signal.raw_data.get("marketplace_intent", 0.0)
        crowding_risk = shopee_signal.raw_data.get("crowding_risk", 0.0)

        # Step 5: Source agreement
        agreement = _compute_source_agreement(signals)

        # Step 6: Consensus opportunity score (with marketplace weights)
        opportunity, action_label, action_reason = compute_opportunity_score(
            quality=quality,
            signals=signals,
            agreement=agreement,
            wow_growth_pct=wow,
            acceleration_pct=accel,
            consistency_pct=consist,
            confidence=conf,
            marketplace_presence=marketplace_presence,
            marketplace_intent=marketplace_intent,
            crowding_risk=crowding_risk,
        )

        # Step 7: Evidence summary
        evidence_summary = _build_evidence_summary(
            quality, signals, agreement, source_count,
            marketplace_presence=marketplace_presence,
            marketplace_intent=marketplace_intent,
            crowding_risk=crowding_risk,
        )

        results.append(OpportunityResult(
            keyword=keyword,
            geo=geo,
            domain=domain,
            interest=interest,
            wow_growth_pct=wow,
            acceleration_pct=accel,
            consistency_pct=consist,
            peak_position_pct=peak,
            confidence=conf,
            status=status,
            opportunity_score=opportunity,
            source_count=source_count,
            source_agreement=agreement,
            keyword_quality_label=quality.keyword_quality_label,
            evidence_summary=evidence_summary,
            action_label=action_label,
            action_reason=action_reason,
            marketplace_presence=marketplace_presence,
            marketplace_intent=marketplace_intent,
            crowding_risk=crowding_risk,
            normalized_keyword=norm_kw,
            sources=signals,
            keyword_quality=quality,
            resolution=resolution,
        ))

    results.sort(key=lambda r: r.opportunity_score, reverse=True)
    return results


# ---------------------------------------------------------------------------
# Enhanced Suggest: multi-source keyword discovery
# ---------------------------------------------------------------------------

def fetch_multi_source_suggestions(
    keyword: str,
    geo: str = "VN",
) -> list[dict]:
    """Enhanced suggest using autocomplete + related queries + commercial variants.

    Results are ranked by commercial intent — product-like and actionable
    suggestions appear first.
    """
    from signal_radar import fetch_suggestions, make_geo_config

    results: list[dict] = []
    seen: set[str] = set()

    # Source 1: Google Autocomplete
    ac_signal = fetch_autocomplete_signal(keyword, geo)
    if ac_signal.success and ac_signal.raw_data.get("suggestions"):
        for sug in ac_signal.raw_data["suggestions"]:
            if sug.lower() not in seen:
                seen.add(sug.lower())
                results.append({"keyword": sug, "value": "", "type": "autocomplete"})

    # Source 2: pytrends related queries
    config = make_geo_config(geo)
    related = fetch_suggestions(keyword, config)
    for r in related:
        if r["keyword"].lower() not in seen:
            seen.add(r["keyword"].lower())
            results.append(r)

    # Source 3: Commercial variants
    commercial_mods = ["giá", "review", "mua ở đâu", "bán"]
    for mod in commercial_mods:
        variant = f"{keyword} {mod}"
        if variant.lower() not in seen:
            seen.add(variant.lower())
            results.append({"keyword": variant, "value": "", "type": "commercial"})

    # Rank by commercial intent: product-like suggestions first
    def _commercial_score(item: dict) -> int:
        kw = item["keyword"].lower()
        score = 0
        if any(p in kw for p in _PRODUCT_INDICATORS):
            score += 3
        if any(p in kw for p in _COMMERCIAL_PATTERNS):
            score += 2
        if any(t in kw for t in _MARKETPLACE_COMMERCE_TERMS):
            score += 2
        # Longer = more specific = better
        if len(kw.split()) >= 3:
            score += 1
        # Autocomplete suggestions are validated by Google, boost them
        if item.get("type") == "autocomplete":
            score += 1
        return score

    results.sort(key=_commercial_score, reverse=True)
    return results[:15]
