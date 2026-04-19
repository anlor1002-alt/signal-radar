"""
Signal Radar - B2B Predictive Analytics Engine

Detects e-commerce product trends 2-4 weeks before they peak by measuring
the acceleration of search interest instead of relying on raw volume alone.
"""

from __future__ import annotations

import asyncio
import html
import os
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta

# Fix Unicode output on Windows (cp1252 can't handle Vietnamese)
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import pandas as pd
from pytrends.request import TrendReq
from telegram import Bot
from telegram.constants import ParseMode

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # .env file is optional; env vars can be set externally


# ---------------------------------------------------------------------------
# Global configuration
# ---------------------------------------------------------------------------

DEFAULT_TIMEFRAME_DAYS = 90
MIN_SLEEP_SECONDS = 1.5
MAX_SLEEP_SECONDS = 4.0
MA_WINDOW_DAYS = 7
MIN_REQUIRED_DAYS = 14
MIN_ABSOLUTE_INTEREST = 20
WOW_GROWTH_THRESHOLD = 3.0  # 300%


@dataclass(frozen=True)
class TrendSignalConfig:
    """Runtime config for Google Trends ingestion."""

    geo: str = "VN"
    hl: str = "vi-VN"
    tz: int = 420
    timeframe_days: int = DEFAULT_TIMEFRAME_DAYS
    min_sleep_seconds: float = MIN_SLEEP_SECONDS
    max_sleep_seconds: float = MAX_SLEEP_SECONDS


# ---------------------------------------------------------------------------
# Module 1: Early Signal Ingestion
# ---------------------------------------------------------------------------

def _build_timeframe(days: int) -> str:
    """Return a pytrends-compatible timeframe string for the last N days."""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    return f"{start_date:%Y-%m-%d} {end_date:%Y-%m-%d}"


def fetch_trend_signals(
    keywords: list[str],
    config: TrendSignalConfig | None = None,
) -> pd.DataFrame:
    """
    Fetch daily Google Trends interest for each keyword over the last N days.

    The function requests each keyword separately so every keyword keeps its
    own 0-100 normalization scale. A randomized sleep is added between calls
    to reduce the chance of HTTP 429 rate limiting.
    """
    if not keywords:
        return pd.DataFrame()

    config = config or TrendSignalConfig()
    timeframe = _build_timeframe(config.timeframe_days)
    pytrends = TrendReq(hl=config.hl, tz=config.tz, retries=2, backoff_factor=0.5)

    keyword_series: dict[str, pd.Series] = {}

    for index, keyword in enumerate(keywords):
        fetched = False

        # --- Attempt 1: exact keyword ---
        try:
            pytrends.build_payload([keyword], timeframe=timeframe, geo=config.geo)
            interest_df = pytrends.interest_over_time()

            if not interest_df.empty:
                series = interest_df[keyword].copy()
                series.name = keyword
                keyword_series[keyword] = series
                print(f"[OK] '{keyword}' — {len(series)} daily points.")
                fetched = True
        except Exception as exc:
            print(f"[WARN] Attempt 1 failed for '{keyword}': {exc}")

        # --- Attempt 2: auto-suggest from Google Trends ---
        if not fetched:
            try:
                suggestions = pytrends.suggestions(keyword)
                if suggestions:
                    alt = suggestions[0]["title"]
                    print(f"[RETRY] '{keyword}' → trying suggestion: '{alt}'")
                    time.sleep(random.uniform(1.0, 2.5))

                    pytrends.build_payload([alt], timeframe=timeframe, geo=config.geo)
                    interest_df = pytrends.interest_over_time()

                    if not interest_df.empty:
                        series = interest_df[alt].copy()
                        series.name = keyword  # keep original name for display
                        keyword_series[keyword] = series
                        print(f"[OK] '{keyword}' (via '{alt}') — {len(series)} daily points.")
                        fetched = True
            except Exception as exc:
                print(f"[WARN] Suggestion retry failed for '{keyword}': {exc}")

        if not fetched:
            print(f"[FAIL] No data for '{keyword}' — skipped.")

        if index < len(keywords) - 1:
            sleep_seconds = random.uniform(
                config.min_sleep_seconds,
                config.max_sleep_seconds,
            )
            time.sleep(sleep_seconds)

    if not keyword_series:
        return pd.DataFrame()

    result = pd.DataFrame(keyword_series).sort_index()
    result.index.name = "date"

    if "isPartial" in result.columns:
        result = result.drop(columns=["isPartial"])

    return result


# ---------------------------------------------------------------------------
# Module 2: The Velocity Engine v2
# ---------------------------------------------------------------------------
#
# Multi-timeframe analysis with 5-level classification:
#
# Metrics per keyword:
#   velocity        — WoW growth of MA7 (how fast interest is rising)
#   acceleration    — change in velocity vs the prior week (is it speeding up)
#   consistency     — % of last 7 days above MA30 (how sustained the trend is)
#   peak_position   — current interest as % of 30-day max (near peak = late)
#   confidence      — composite 0-100 score (growth + level + consistency + accel)
#
# Classification thresholds:
#   BURSTING  — WoW > 300% & interest > 20   (explosive, act NOW)
#   EMERGING  — WoW > 100% & interest > 10   (early signal, watch closely)
#   RISING    — WoW > 30%  & MA7 > MA30      (steady upward growth)
#   STABLE    — WoW -10% to +30%             (consistent baseline demand)
#   DECLINING — WoW < -10%                   (fading interest)
# ---------------------------------------------------------------------------

# Thresholds
WOW_BURSTING  = 3.0   # 300%
WOW_EMERGING  = 1.0   # 100%
WOW_RISING    = 0.3   # 30%
WOW_DECLINING = -0.1   # -10%
INTEREST_BURSTING = 20
INTEREST_EMERGING = 10


# ---------------------------------------------------------------------------
# Domain Detection
# ---------------------------------------------------------------------------

DOMAIN_RULES: list[tuple[str, list[str]]] = [
    ("E-commerce", [
        "thực phẩm", "đồ ăn", "hạt", "mật ", "tinh bột", "đường", "đồ uống",
        "trà", "cà phê", "coffee", "nước", "snack", "bánh", "kẹo", "gia vị",
        "nông sản", "thủy sản", "hải sản", "rau", "củ", "quả", "thịt",
        "nhập hàng", "bán lẻ", "shopee", "tiktok shop", "lazada",
    ]),
    ("Fashion", [
        "áo", "quần", "giày", "túi", "thời trang", "sneaker", "váy",
        "đầm", "kính", "đồng hồ", "mũ", "nón", "túi xách", "balo",
        "hoodie", "jacket", "sơ mi", "jean", "denim",
    ]),
    ("Health & Beauty", [
        "thuốc", "kem ", "serum", "vitamin", "detox", "giảm cân", "làm đẹp",
        "skincare", "makeup", "son", "phấn", "mascara", "chăm sóc da",
        "dưỡng", "trị mụn", "chống nắng", "tẩy", "tóc", "body",
        "thực phẩm chức năng", "omega", "collagen",
    ]),
    ("Technology", [
        "ai ", "gpt", "opus", "claude", "gemini", "phone", "laptop", "phone",
        "phần mềm", "app ", "code", "python", "javascript", "react",
        "chip", "gpu", "cpu", "ram", "ssd", "iphone", "samsung", "xiaomi",
        "robot", "điện thoại", "máy tính", "table", "ipad", "macbook",
        "chatgpt", "openai", "anthropic", "midjourney",
    ]),
    ("Finance", [
        "chứng khoán", "crypto", "bitcoin", "coin", "đầu tư", "cổ phiếu",
        "ethereum", "binance", "forex", "vàng", "bạc", "tài chính",
        "lãi suất", "inflation", "gdp", "thị trường", "trading",
    ]),
    ("Entertainment", [
        "game", "phim", "nhạc", "idol", "anime", "manga", "kpop", "vpop",
        "concert", "festival", "show", "streaming", "netflix", "youtube",
        "tiktok", "esport", "gaming",
    ]),
    ("Education", [
        "học", "khóa học", "đào tạo", "chứng chỉ", "ielts", "toeic",
        "universit", "thi ", "ôn thi", "sách ", "giáo trình", "tutorial",
        "course", "training", "skill",
    ]),
]

DOMAIN_RECOMMENDATIONS: dict[str, dict[str, str]] = {
    "E-commerce": {
        "BURSTING":  "Nhập hàng ngay / Lên chiến dịch marketing!",
        "EMERGING":  "Chuẩn bị nguồn hàng / Lên kế hoạch bán.",
        "RISING":    "Cân nhắc nhập thêm / Theo dõi đối thủ.",
        "STABLE":    "Nhu cầu ổn định — duy trì tồn kho.",
        "DECLINING": "Nhu cầu giảm — hạn chế nhập hàng.",
    },
    "Fashion": {
        "BURSTING":  "Lên collection ngay / Nhập hàng xu hướng!",
        "EMERGING":  "Xu hướng đang lên — chuẩn bị mẫu mới.",
        "RISING":    "Cân nhắc bổ sung size/màu hot.",
        "STABLE":    "Mặt hàng cơ bản — giữ mức tồn kho.",
        "DECLINING": "Xu hướng hạ — clearance sale.",
    },
    "Health & Beauty": {
        "BURSTING":  "Đẩy mạnh chiến dịch wellness / Cập nhật sản phẩm!",
        "EMERGING":  "Xu hướng làm đẹp mới — nghiên cứu sản phẩm.",
        "RISING":    "Nhu cầu tăng — bổ sung listing.",
        "STABLE":    "Sản phẩm ổn định — duy trì.",
        "DECLINING": "Trend giảm — chuyển focus.",
    },
    "Technology": {
        "BURSTING":  "Nghiên cứu công nghệ / Đầu tư sớm!",
        "EMERGING":  "Tín hiệu công nghệ mới — tìm hiểu ngay.",
        "RISING":    "Công nghệ đang phổ biến — theo dõi.",
        "STABLE":    "Công nghệ ổn định — phổ thông.",
        "DECLINING":  "Công nghệ lỗi thời — chú ý.",
    },
    "Finance": {
        "BURSTING":  "Phân tích đầu tư gấp / Theo dõi thị trường!",
        "EMERGING":  "Tín hiệu tài chính — cân nhắc vị thế.",
        "RISING":    "Thị trường tích cực — theo dõi tiếp.",
        "STABLE":    "Thị trường sideway — chờ đợi.",
        "DECLINING": "Thị trường tiêu cực — phòng thủ.",
    },
    "Entertainment": {
        "BURSTING":  "Tạo content ngay / Phân tích xu hướng media!",
        "EMERGING":  "Trend media mới — lên content plan.",
        "RISING":    "Nội dung đang hot — ride trend.",
        "STABLE":    "Content ổn — duy trì lịch đăng.",
        "DECLINING": "Trend hạ — tìm nội dung mới.",
    },
    "Education": {
        "BURSTING":  "Tạo khóa học / Nội dung giáo dục mới!",
        "EMERGING":  "Nhu cầu học tập tăng — phát triển content.",
        "RISING":    "Chủ đề hot — cân nhắc xây khóa học.",
        "STABLE":    "Nhu cầu học ổn định — duy trì.",
        "DECLINING": "Quan tâm giảm — cập nhật nội dung.",
    },
    "General": {
        "BURSTING":  "Xu hướng bùng nổ — hành động ngay!",
        "EMERGING":  "Tín hiệu sớm — theo dõi sát.",
        "RISING":    "Xu hướng tăng — phân tích thêm.",
        "STABLE":    "Ổn định — không có tín hiệu mạnh.",
        "DECLINING": "Giảm — không nên đầu tư.",
    },
}


def detect_domain(keyword: str) -> str:
    """Classify a keyword into a domain based on pattern matching."""
    kw_lower = keyword.lower()
    for domain, patterns in DOMAIN_RULES:
        if any(p in kw_lower for p in patterns):
            return domain
    return "General"


def get_recommendation(domain: str, status: str) -> str:
    """Return a domain-specific recommendation for the given status."""
    return DOMAIN_RECOMMENDATIONS.get(domain, DOMAIN_RECOMMENDATIONS["General"]).get(
        status, DOMAIN_RECOMMENDATIONS["General"]["STABLE"]
    )


def _compute_ma(series: pd.Series, window: int) -> pd.Series:
    """Simple moving average over *window* days."""
    return series.rolling(window=window, min_periods=window).mean()


def _wow_growth(current: float, previous: float) -> float:
    """Week-over-week growth rate. Returns inf when baseline is zero."""
    if previous == 0:
        return float("inf") if current > 0 else 0.0
    return (current / previous) - 1.0


def _classify(
    wow: float,
    interest: float,
    ma7: float,
    ma30: float | None,
) -> str:
    """Return one of: BURSTING, EMERGING, RISING, STABLE, DECLINING."""
    if wow > WOW_BURSTING and interest > INTEREST_BURSTING:
        return "BURSTING"
    if wow > WOW_EMERGING and interest > INTEREST_EMERGING:
        return "EMERGING"
    if wow > WOW_RISING and (ma30 is None or ma7 > ma30):
        return "RISING"
    if wow > WOW_DECLINING:
        return "STABLE"
    return "DECLINING"


def _confidence_score(
    wow: float,
    interest: float,
    consistency: float,
    acceleration: float,
    peak_position: float,
) -> int:
    """Composite 0-100 confidence score.

    Components (max each):
      growth magnitude  — 30 pts  (capped at WoW 500%)
      absolute level    — 20 pts  (interest 0-100)
      consistency       — 25 pts  (% of days above MA30)
      acceleration      — 15 pts  (positive accel = bonus)
      peak room         — 10 pts  (lower peak_position = more room to grow)
    """
    growth_pts = min(wow / 5.0, 1.0) * 30                       # 0-30
    level_pts  = min(interest / 100.0, 1.0) * 20                # 0-20
    consist_pts = (consistency / 100.0) * 25                     # 0-25
    accel_pts  = min(max(acceleration, 0) / 3.0, 1.0) * 15      # 0-15
    peak_pts   = (1.0 - min(peak_position / 100.0, 1.0)) * 10   # 0-10

    return int(min(growth_pts + level_pts + consist_pts + accel_pts + peak_pts, 100))


def velocity_engine(interest_df: pd.DataFrame, domain_override: str | None = None) -> pd.DataFrame:
    """
    Analyse every keyword column and return a rich summary DataFrame.

    Columns per keyword:
      keyword, interest, ma7, ma14, ma30, wow_growth_pct,
      acceleration_pct, consistency_pct, peak_position_pct,
      confidence, status
    """
    if interest_df.empty:
        return pd.DataFrame()

    rows: list[dict[str, object]] = []

    for keyword in interest_df.columns:
        raw = interest_df[keyword].dropna().astype(float)

        if len(raw) < MIN_REQUIRED_DAYS:
            print(f"[SKIP] '{keyword}' — only {len(raw)} days (need {MIN_REQUIRED_DAYS}).")
            continue

        # --- Moving averages ---
        ma7  = _compute_ma(raw, 7)
        ma14 = _compute_ma(raw, 14)
        ma30 = _compute_ma(raw, 30) if len(raw) >= 30 else None

        latest_ma7  = float(ma7.iloc[-1])
        latest_ma14 = float(ma14.iloc[-1])
        latest_ma30 = float(ma30.iloc[-1]) if ma30 is not None else None

        # --- Velocity: WoW growth of MA7 ---
        current_week  = float(ma7.iloc[-7:].mean())
        previous_week = float(ma7.iloc[-14:-7].mean()) if len(ma7) >= 14 else 0.0
        wow = _wow_growth(current_week, previous_week)

        # --- Acceleration: WoW change vs the week before that ---
        if len(ma7.dropna()) >= 21:
            prev_prev_week = float(ma7.iloc[-21:-14].mean())
            prev_wow = _wow_growth(previous_week, prev_prev_week)
            # Cap inf values to avoid -inf when subtracting
            wow_capped = 10.0 if wow == float("inf") else wow   # 1000% cap
            prev_wow_capped = 10.0 if prev_wow == float("inf") else prev_wow
            accel = wow_capped - prev_wow_capped
        else:
            accel = 0.0

        # --- Consistency: % of last 7 days above MA30 ---
        if latest_ma30 is not None:
            last7 = raw.iloc[-7:]
            consistency = float((last7 > latest_ma30).sum() / len(last7) * 100)
        else:
            consistency = 50.0  # neutral when MA30 unavailable

        # --- Peak position: current vs 30-day max ---
        window = raw.tail(30)
        peak_max = float(window.max())
        current_interest = float(raw.iloc[-1])
        peak_pos = (current_interest / peak_max * 100) if peak_max > 0 else 0.0

        # --- Classify ---
        status = _classify(wow, current_interest, latest_ma7, latest_ma30)
        domain = domain_override or detect_domain(keyword)

        # --- Confidence ---
        conf = _confidence_score(wow, current_interest, consistency, accel, peak_pos)

        # --- Format WoW ---
        wow_pct = round(wow * 100, 1) if wow != float("inf") else float("inf")

        rows.append({
            "keyword":            keyword,
            "domain":             domain,
            "interest":           round(current_interest, 1),
            "ma7":                round(latest_ma7, 1),
            "ma14":               round(latest_ma14, 1),
            "ma30":               round(latest_ma30, 1) if latest_ma30 is not None else None,
            "wow_growth_pct":     wow_pct,
            "acceleration_pct":   round(accel * 100, 1),
            "consistency_pct":    round(consistency, 0),
            "peak_position_pct":  round(peak_pos, 0),
            "confidence":         conf,
            "status":             status,
        })

    results = pd.DataFrame(rows)
    if results.empty:
        return results

    status_order = {"BURSTING": 0, "EMERGING": 1, "RISING": 2, "STABLE": 3, "DECLINING": 4}
    results["_sort"] = results["status"].map(status_order)
    results = results.sort_values(
        by=["_sort", "confidence"],
        ascending=[True, False],
    ).drop(columns=["_sort"]).reset_index(drop=True)

    return results


# ---------------------------------------------------------------------------
# Module 3: Telegram Notifier
# ---------------------------------------------------------------------------

def _format_telegram_message(row: dict) -> str:
    """Build a single-keyword alert for bursting/emerging trends."""
    kw = html.escape(str(row["keyword"]))
    wow = row["wow_growth_pct"]
    wow_str = "INF" if wow == float("inf") else f"{wow:.1f}"
    conf = int(row["confidence"])

    return (
        "<b>\U0001F6A8 DỰ BÁO XU HƯỚNG BÙNG NỔ \U0001F6A8</b>\n"
        f"- Từ khóa: {kw}\n"
        f"- Tăng trưởng (WoW): +{wow_str}%\n"
        f"- Confidence: {conf}/100\n"
        "- Đánh giá: Sẵn sàng nhập hàng / Lên chiến dịch marketing ngay!"
    )


async def send_telegram_alert(bot_token: str, chat_id: str, results: pd.DataFrame) -> None:
    """Send push alerts for BURSTING and EMERGING trends."""
    if results.empty or "status" not in results.columns:
        print("[INFO] No results to notify.")
        return

    hot = results[results["status"].isin(["BURSTING", "EMERGING"])]
    if hot.empty:
        print("[INFO] No bursting/emerging trends detected — no alerts sent.")
        return

    bot = Bot(token=bot_token)

    for _, row in hot.iterrows():
        message = _format_telegram_message(row.to_dict())
        await bot.send_message(chat_id=chat_id, text=message, parse_mode=ParseMode.HTML)
        await asyncio.sleep(0.5)


# ---------------------------------------------------------------------------
# Main script: smoke test with dummy keywords
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    KEYWORDS = [
        "m\u1eadt hoa d\u1eeba",
        "tinh ngh\u1ec7 t\u01b0\u01a1i",
        "\u0111\u01b0\u1eddng \u0103n ki\u00eang",
    ]

    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

    print("=" * 72)
    print("SIGNAL RADAR | EARLY SIGNAL INGESTION")
    print("=" * 72)
    interest_data = fetch_trend_signals(KEYWORDS)

    if interest_data.empty:
        print("[ABORT] No Google Trends data retrieved. Check network or keywords.")
    else:
        print(f"[INFO] Retrieved shape: {interest_data.shape}")
        print()
        print("=" * 72)
        print("SIGNAL RADAR | VELOCITY ENGINE")
        print("=" * 72)

        trend_results = velocity_engine(interest_data)
        if trend_results.empty:
            print("[INFO] No analyzable keywords after preprocessing.")
        else:
            print(trend_results.to_string(index=False))
            print()
            hot = trend_results[trend_results["status"].isin(["BURSTING", "EMERGING"])]
            print(
                f"[INFO] Hot trends (bursting + emerging): "
                f"{len(hot)}/{len(trend_results)}"
            )

        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            asyncio.run(
                send_telegram_alert(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, trend_results)
            )
        else:
            print("[INFO] Telegram credentials not set. Create a .env file (see .env.example).")
