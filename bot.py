"""
Signal Radar — Interactive Telegram Bot

Long-running bot that lets users scan e-commerce trend signals
directly from Telegram.

Commands:
    /start  — Welcome + menu
    /scan   — Start keyword input flow
    /help   — Usage instructions
"""

from __future__ import annotations

import asyncio
import html
import os
from datetime import datetime

from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from signal_radar import (
    ACTION_EMOJI,
    ACTION_LABEL_VI,
    GEO_FLAGS,
    GEO_LABELS,
    TrendSignalConfig,
    compute_action,
    fetch_suggestions,
    fetch_trend_signals,
    get_recommendation,
    make_geo_config,
    velocity_engine,
)
from sources import (
    KeywordResolution,
    OpportunityResult,
    fetch_multi_source_suggestions,
    multi_source_engine,
    resolve_keyword,
)
from database import (
    add_keyword,
    create_project,
    delete_project,
    export_user_history_csv,
    get_all_tracked_keywords,
    get_keywords_for_project_ids,
    get_keyword_history,
    get_keyword_history_normalized,
    get_last_alert_time,
    get_project,
    get_project_keywords,
    get_twice_daily_projects,
    get_user_keywords,
    get_user_projects,
    init_db,
    insert_scan_history,
    register_user,
    remove_keyword,
    update_alert_time,
    update_keyword_status,
)

load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")

# Conversation states
AWAITING_KEYWORDS = 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

STATUS_EMOJI = {
    "BURSTING":  "\U0001F6A8",
    "EMERGING":  "\U0001F525",
    "RISING":    "\U0001F4C8",
    "STABLE":    "\U0001F4CA",
    "DECLINING": "\U0001F4C9",
}

DOMAIN_EMOJI = {
    "E-commerce":      "\U0001F6D2",
    "Fashion":         "\U0001F455",
    "Health & Beauty": "\U0001F9F4",
    "Technology":      "\U0001F4BB",
    "Finance":         "\U0001F4B0",
    "Entertainment":   "\U0001F3AC",
    "Education":       "\U0001F4DA",
    "General":         "\U0001F310",
}


# ---------------------------------------------------------------------------
# Telegram Message Cleanup & Callback Token Helpers
# ---------------------------------------------------------------------------

async def safe_edit_message(message, text: str, **kwargs) -> None:
    """Edit a message safely — ignores already-deleted or unchanged."""
    try:
        await message.edit_text(text, **kwargs)
    except Exception:
        pass


async def safe_delete_message(bot, chat_id, message_id) -> None:
    """Delete a message safely — ignores already-deleted."""
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass


async def close_callback(query) -> None:
    """Close an inline UI message: edit to short text, remove keyboard."""
    try:
        await query.message.edit_text(
            "Đã đóng.", parse_mode=ParseMode.HTML,
        )
    except Exception:
        try:
            await query.message.delete()
        except Exception:
            pass


# --- Callback token registry (avoids putting raw text in callback_data) ---
# Stores keyword/geo payloads under short IDs like "0", "1", "2" in user_data.
# Tokens survive across the session but expire on bot restart (acceptable).


def _cb_token(context: ContextTypes.DEFAULT_TYPE, data: dict) -> str:
    """Store payload in session token map, return short ID for callback_data."""
    tokens: dict = context.user_data.setdefault("_cb_tokens", {})
    # Find next available ID
    idx = len(tokens)
    key = str(idx)
    tokens[key] = data
    print(f"[CB_TOKEN] Created token {key}: {data}")
    return key


def _cb_resolve(context: ContextTypes.DEFAULT_TYPE, token: str) -> dict | None:
    """Retrieve payload by token. Returns None if expired (restart)."""
    tokens: dict = context.user_data.get("_cb_tokens", {})
    data = tokens.get(token)
    if data is None:
        print(f"[CB_TOKEN] Token {token} expired or unknown")
    return data


# --- Typed UI state management ---
# Tracks interactive bot messages so they can be cleaned up on next view.
# Types: "menu", "picker", "result", "history"


def _remember_ui(
    context: ContextTypes.DEFAULT_TYPE,
    message,
    ui_type: str = "menu",
) -> None:
    """Store the current interactive UI message with its type."""
    context.user_data["_ui_state"] = {
        "chat_id": message.chat_id,
        "message_id": message.message_id,
        "ui_type": ui_type,
    }


async def _cleanup_prev_ui(
    context: ContextTypes.DEFAULT_TYPE,
    replace_type: str | None = None,
) -> None:
    """Delete the previous interactive UI message if it exists.

    If replace_type is set, only clean up if the previous UI was of that type.
    If None, always clean up.
    """
    state = context.user_data.pop("_ui_state", None)
    if not state:
        return
    if replace_type and state.get("ui_type") != replace_type:
        # Put it back — wrong type to clean up
        context.user_data["_ui_state"] = state
        return
    await safe_delete_message(context.bot, state["chat_id"], state["message_id"])
    print(f"[UI] Cleaned up {state.get('ui_type')} message {state['message_id']}")


async def _expired_callback(query) -> None:
    """Handle a callback whose session state has expired (after restart)."""
    try:
        await query.answer("Phiên cũ đã hết hạn.")
    except Exception:
        pass
    try:
        await query.message.edit_text(
            "\u26A0 Phiên thao tác cũ đã hết hạn.\n"
            "Dùng /start hoặc /history để mở lại.",
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Signal Change (Delta) Logic
# ---------------------------------------------------------------------------

_DELTA_ARROWS = {
    "up":   "\U0001F4C8",   # chart up
    "down": "\U0001F4C9",   # chart down
    "flat": "\U0001F4CA",   # chart flat
}


def compute_delta(current: dict, previous: dict | None) -> str:
    """Compare current scan vs previous and return a short Vietnamese delta explanation.

    Each dict should have at least: status, confidence, wow_growth, interest.
    Returns a one-line string like 'Confidence +12 | WoW tăng mạnh | Status: RISING → EMERGING'
    Returns empty string if no previous data.
    """
    if previous is None:
        return ""

    parts: list[str] = []

    # --- Confidence delta ---
    cur_conf = int(current.get("confidence", 0))
    prev_conf = int(previous.get("confidence", 0))
    conf_diff = cur_conf - prev_conf
    if abs(conf_diff) >= 5:
        arrow = "\U0001F4C8" if conf_diff > 0 else "\U0001F4C9"
        parts.append(f"Conf {arrow}{conf_diff:+d}")

    # --- Status change ---
    cur_status = str(current.get("status", ""))
    prev_status = str(previous.get("status", ""))
    if cur_status != prev_status:
        parts.append(f"{prev_status} → {cur_status}")

    # --- WoW acceleration / cooling ---
    cur_wow = current.get("wow_growth", 0)
    prev_wow = previous.get("wow_growth", 0)
    if cur_wow and prev_wow:
        # Handle inf
        cur_w = 999.0 if cur_wow == float("inf") else float(cur_wow)
        prev_w = 999.0 if prev_wow == float("inf") else float(prev_wow)
        wow_diff = cur_w - prev_w
        if abs(wow_diff) > 10:
            if wow_diff > 0:
                parts.append("WoW tăng tốc")
            else:
                parts.append("WoW giảm tốc")

    # --- Interest trend ---
    cur_int = float(current.get("interest", 0))
    prev_int = float(previous.get("interest", 0))
    if prev_int > 0:
        int_change = ((cur_int - prev_int) / prev_int) * 100
        if int_change > 20:
            parts.append("Interest tăng mạnh")
        elif int_change < -20:
            parts.append("Interest giảm")

    if not parts:
        return ""

    return " | ".join(parts)


def _format_single_report(row) -> str:
    """Build a per-keyword HTML report with action label."""
    kw = html.escape(str(row["keyword"]))
    status = str(row["status"])
    domain = str(row.get("domain", "General"))
    emoji = STATUS_EMOJI.get(status, "\U0001F4CA")
    domain_emoji = DOMAIN_EMOJI.get(domain, "\U0001F310")
    wow = row["wow_growth_pct"]
    wow_str = "INF" if wow == float("inf") else f"{wow:+.1f}"
    conf = int(row["confidence"])
    consistency = int(row["consistency_pct"])
    peak = int(row["peak_position_pct"])
    accel = row["acceleration_pct"]
    accel_str = f"{accel:+.1f}%"
    interest = int(round(float(row["interest"])))

    rec = get_recommendation(domain, status)

    # Action label
    action, reason = compute_action(
        status, conf, wow, peak, accel, consistency,
    )
    act_emoji = ACTION_EMOJI[action]
    act_vi = ACTION_LABEL_VI[action]

    filled = conf // 10
    bar = "\u2588" * filled + "\u2591" * (10 - filled)

    return (
        f"<b>SIGNAL RADAR</b>\n\n"
        f"{emoji} <b>{kw}</b> {domain_emoji} {domain}\n"
        f"  Interest: {interest} | WoW: {wow_str}%\n"
        f"  Gia tốc: {accel_str} | Bền vững: {consistency}%\n"
        f"  Đỉnh 30d: {peak}% | Confidence: {bar} {conf}/100\n"
        f"  {act_emoji} <b>{act_vi}</b> — {reason}\n"
        f"  → <b>{rec}</b>"
    )


def _format_summary(results) -> str:
    """Build the summary message after all per-keyword reports."""
    total = len(results)
    status_counts = results["status"].value_counts().to_dict()
    bursting = status_counts.get("BURSTING", 0)
    emerging = status_counts.get("EMERGING", 0)
    rising = status_counts.get("RISING", 0)
    stable = status_counts.get("STABLE", 0)
    declining = status_counts.get("DECLINING", 0)

    # Action label counts
    action_counts = {"GO": 0, "WATCH": 0, "AVOID": 0}
    for _, r in results.iterrows():
        action, _ = compute_action(
            str(r["status"]), int(r["confidence"]),
            r["wow_growth_pct"], r["peak_position_pct"],
            r["acceleration_pct"], r["consistency_pct"],
        )
        action_counts[action] += 1

    domain_counts = results["domain"].value_counts().to_dict() if "domain" in results.columns else {}
    domain_lines = []
    for d, count in domain_counts.items():
        de = DOMAIN_EMOJI.get(d, "\U0001F310")
        domain_lines.append(f"  {de} {d}: {count}")

    top3 = results.nlargest(3, "confidence")[["keyword", "confidence", "status", "domain"]]
    top_lines = []
    for _, r in top3.iterrows():
        e = STATUS_EMOJI.get(str(r["status"]), "")
        de = DOMAIN_EMOJI.get(str(r.get("domain", "")), "")
        top_lines.append(
            f"  {e} {de} {html.escape(str(r['keyword']))} ({int(r['confidence'])}/100)"
        )

    if bursting > 0:
        rec = "\U0001F6A8 Xu hướng bùng nổ phát hiện — hành động ngay!"
    elif emerging > 0:
        rec = "\U0001F525 Tín hiệu sớm — cần theo dõi sát."
    elif rising > 0:
        rec = "\U0001F4C8 Xu hướng đang tăng — phân tích thêm."
    else:
        rec = "\U0001F4CA Thị trường ổn định — chưa có tín hiệu mạnh."

    return (
        f"<b>TỔNG KẾT — {total} từ khóa</b>\n\n"
        f"\U0001F7E2 GO: {action_counts['GO']} | \U0001F7E1 WATCH: {action_counts['WATCH']} | "
        f"\U0001F534 AVOID: {action_counts['AVOID']}\n\n"
        f"\U0001F6A8 Bursting: {bursting} | \U0001F525 Emerging: {emerging} | "
        f"\U0001F4C8 Rising: {rising}\n"
        f"\U0001F4CA Stable: {stable} | \U0001F4C9 Declining: {declining}\n\n"
        f"<b>Lĩnh vực:</b>\n" + "\n".join(domain_lines) +
        f"\n\n<b>Top tiềm năng:</b>\n" + "\n".join(top_lines) + f"\n\n{rec}"
    )


# ---------------------------------------------------------------------------
# Multi-Source Opportunity Formatters
# ---------------------------------------------------------------------------

_QUALITY_BADGES = {
    "COMMERCIAL":     "\U0001F4B0",
    "INFORMATIONAL":  "\U0001F4D6",
    "BRAND":          "\u2122\uFE0F",
    "AMBIGUOUS":      "\u26A0\uFE0F",
    "BROAD":          "\U0001F30D",
    "PERSON":         "\U0001F464",
}


def _format_opportunity_report(opp: OpportunityResult) -> str:
    """Build a per-keyword HTML report with multi-source opportunity score."""
    kw = html.escape(opp.keyword)
    status = opp.status
    domain = opp.domain
    emoji = STATUS_EMOJI.get(status, "\U0001F4CA")
    domain_emoji = DOMAIN_EMOJI.get(domain, "\U0001F310")
    wow = opp.wow_growth_pct
    wow_str = "INF" if wow == float("inf") else f"{wow:+.1f}"
    conf = opp.confidence

    act_emoji = ACTION_EMOJI[opp.action_label]
    act_vi = ACTION_LABEL_VI[opp.action_label]

    filled = conf // 10
    bar = "\u2588" * filled + "\u2591" * (10 - filled)

    rec = get_recommendation(domain, status)

    # Source indicator dots
    source_dots = []
    for sig in opp.sources:
        if sig.success:
            source_dots.append("\U0001F7E2")  # green
        else:
            source_dots.append("\U0001F534")  # red
    source_bar = "".join(source_dots)

    q_badge = _QUALITY_BADGES.get(opp.keyword_quality_label, "")

    # Marketplace validation line
    mp_line = ""
    if opp.marketplace_presence > 0 or opp.marketplace_intent > 0:
        presence_str = f"{opp.marketplace_presence:.0%}"
        intent_str = f"{opp.marketplace_intent:.0%}"
        crowd_emoji = "\U0001F7E2" if opp.crowding_risk < 0.3 else ("\U0001F7E1" if opp.crowding_risk < 0.6 else "\U0001F534")
        mp_line = (
            f"  \U0001F6D2 Marketplace: {presence_str} presence | "
            f"{intent_str} intent | {crowd_emoji} crowding {opp.crowding_risk:.0%}\n"
        )

    # Decision card: Kết luận / Vì sao / Tiếp theo
    why = opp.action_reason
    next_step = ""
    if opp.resolution:
        why = opp.resolution.resolver_reason
        next_step = opp.resolution.next_action
    else:
        if opp.action_label == "GO":
            next_step = "Hành động ngay — dùng /track để theo dõi tự động"
        elif opp.action_label == "WATCH":
            next_step = "Theo dõi sát — /compare với từ khóa tương tự"
        else:
            next_step = "Chưa đủ tín hiệu — thử từ khóa cụ thể hơn"

    return (
        f"<b>SIGNAL RADAR</b>\n\n"
        f"{emoji} <b>{kw}</b> {domain_emoji} {domain}\n"
        f"  Interest: {int(opp.interest)} | WoW: {wow_str}%\n"
        f"  Gia tốc: {opp.acceleration_pct:+.1f}% | Bền vững: {int(opp.consistency_pct)}%\n"
        f"  Đỉnh 30d: {int(opp.peak_position_pct)}% | Confidence: {bar} {conf}/100\n"
        f"  <b>Opportunity: {opp.opportunity_score}/100</b> | "
        f"Nguồn: {source_bar} {opp.source_count}/4 | "
        f"Đồng thuận: {opp.source_agreement:.0%}\n"
        f"{mp_line}"
        f"  {q_badge} {opp.keyword_quality_label}\n"
        f"  \U0001F4CB <b>Kết luận:</b> {act_emoji} {act_vi}\n"
        f"  \U0001F4A1 <b>Vì sao:</b> {why}\n"
        f"  \U0001F449 <b>Tiếp:</b> {next_step}\n"
        f"  <i>{opp.evidence_summary}</i>"
    )


def _format_opportunity_summary(results: list[OpportunityResult]) -> str:
    """Build summary from multi-source opportunity results."""
    total = len(results)
    action_counts = {"GO": 0, "WATCH": 0, "AVOID": 0}
    status_counts: dict[str, int] = {}
    domain_counts: dict[str, int] = {}

    for r in results:
        action_counts[r.action_label] = action_counts.get(r.action_label, 0) + 1
        status_counts[r.status] = status_counts.get(r.status, 0) + 1
        domain_counts[r.domain] = domain_counts.get(r.domain, 0) + 1

    bursting = status_counts.get("BURSTING", 0)
    emerging = status_counts.get("EMERGING", 0)
    rising = status_counts.get("RISING", 0)
    stable = status_counts.get("STABLE", 0)
    declining = status_counts.get("DECLINING", 0)

    domain_lines = []
    for d, count in domain_counts.items():
        de = DOMAIN_EMOJI.get(d, "\U0001F310")
        domain_lines.append(f"  {de} {d}: {count}")

    top3 = results[:3]  # already sorted by opportunity_score
    top_lines = []
    for r in top3:
        e = STATUS_EMOJI.get(r.status, "")
        de = DOMAIN_EMOJI.get(r.domain, "\U0001F310")
        top_lines.append(
            f"  {e} {de} {html.escape(r.keyword)} "
            f"({r.opportunity_score}/100)"
        )

    avg_opp = sum(r.opportunity_score for r in results) / max(total, 1)

    if bursting > 0:
        rec = "\U0001F6A8 Xu hướng bùng nổ phát hiện — hành động ngay!"
    elif emerging > 0:
        rec = "\U0001F525 Tín hiệu sớm — cần theo dõi sát."
    elif rising > 0:
        rec = "\U0001F4C8 Xu hướng đang tăng — phân tích thêm."
    else:
        rec = "\U0001F4CA Thị trường ổn định — chưa có tín hiệu mạnh."

    return (
        f"<b>TỔNG KẾT — {total} từ khóa</b>\n"
        f"<b>Trung bình Opportunity: {avg_opp:.1f}/100</b>\n\n"
        f"\U0001F7E2 GO: {action_counts['GO']} | \U0001F7E1 WATCH: {action_counts['WATCH']} | "
        f"\U0001F534 AVOID: {action_counts['AVOID']}\n\n"
        f"\U0001F6A8 Bursting: {bursting} | \U0001F525 Emerging: {emerging} | "
        f"\U0001F4C8 Rising: {rising}\n"
        f"\U0001F4CA Stable: {stable} | \U0001F4C9 Declining: {declining}\n\n"
        f"<b>Lĩnh vực:</b>\n" + "\n".join(domain_lines) +
        f"\n\n<b>Top cơ hội:</b>\n" + "\n".join(top_lines) + f"\n\n{rec}"
    )


# ---------------------------------------------------------------------------
# Command Handlers
# ---------------------------------------------------------------------------

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send welcome message with domain selection menu."""
    await _cleanup_prev_ui(context, replace_type="menu")
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("\U0001F6D2 E-commerce", callback_data="domain:E-commerce"),
            InlineKeyboardButton("\U0001F455 Fashion", callback_data="domain:Fashion"),
        ],
        [
            InlineKeyboardButton("\U0001F9F4 Sức khỏe & Làm đẹp", callback_data="domain:Health & Beauty"),
            InlineKeyboardButton("\U0001F4BB Công nghệ", callback_data="domain:Technology"),
        ],
        [
            InlineKeyboardButton("\U0001F4B0 Tài chính", callback_data="domain:Finance"),
            InlineKeyboardButton("\U0001F3AC Giải trí", callback_data="domain:Entertainment"),
        ],
        [
            InlineKeyboardButton("\U0001F4DA Giáo dục", callback_data="domain:Education"),
            InlineKeyboardButton("\U0001F310 Tự phát hiện", callback_data="domain:auto"),
        ],
        [
            InlineKeyboardButton("\U0001F4D6 Hướng dẫn", callback_data="help"),
            InlineKeyboardButton("\U0001F3C6 Top cơ hội", callback_data="top"),
        ],
        [
            InlineKeyboardButton("Đóng", callback_data="close"),
        ],
    ])

    msg = await update.message.reply_text(
        "<b>SIGNAL RADAR</b>\n\n"
        "Phát hiện xu hướng trước 2-4 tuần khi demand bùng nổ.\n\n"
        "Chọn lĩnh vực muốn quét:",
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
    )
    _remember_ui(context, msg, ui_type="menu")


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show usage instructions."""
    await update.message.reply_text(
        "<b>Hướng dẫn sử dụng</b>\n\n"
        "/start — Menu chính (chọn lĩnh vực)\n"
        "/scan — Quét nhanh từ khóa\n"
        "/track &lt;kw&gt; [VN|US|WW] — Theo dõi từ khóa\n"
        "/untrack &lt;kw&gt; [VN|US|WW] — Bỏ theo dõi\n"
        "/mylist — Xem danh sách theo dõi + GO/WATCH/AVOID\n"
        "/history &lt;kw&gt; — Xem lịch sử quét + xu hướng thay đổi\n"
        "/compare kw1, kw2 — So sánh 2-5 từ khóa\n"
        "/suggest &lt;kw&gt; — Gợi ý từ khóa liên quan\n"
        "/export &lt;kw|project tên|all&gt; — Xuất CSV\n"
        "/top — Xem top cơ hội hiện tại\n\n"
        "<b>Projects:</b>\n"
        "/pnew &lt;tên&gt; [daily|twice_daily] — Tạo project\n"
        "/plist — Danh sách projects\n"
        "/padd &lt;project&gt; &lt;kw&gt; [VN|US|WW] — Thêm từ khóa vào project\n"
        "/pview &lt;project&gt; — Xem dashboard project\n"
        "/pdel &lt;project&gt; — Xoá project\n\n"
        "\U0001F7E2 GO = Hành động | \U0001F7E1 WATCH = Theo dõi | \U0001F534 AVOID = Tránh\n"
        "\U0001F1FB\U0001F1F3 VN | \U0001F1FA\U0001F1F8 US | \U0001F30D WW (toàn cầu)\n\n"
        "Bot tự động quét từ khóa đã /track mỗi ngày\n"
        "và gửi báo cáo tổng hợp (digest) cho bạn.",
        parse_mode=ParseMode.HTML,
    )


async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Enter keyword-input conversation state."""
    await update.message.reply_text(
        "\U0001F50D <b>Nhập từ khóa cần quét</b>\n\n"
        "Gửi từ khóa, cách nhau bằng dấu phẩy.\n"
        "Ví dụ: <i>mật ong, tinh bột nghệ, đường ăn kiêng</i>\n\n"
        "Gửi /cancel để huỷ.",
        parse_mode=ParseMode.HTML,
    )
    return AWAITING_KEYWORDS


async def handle_keywords(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Receive keywords, run the pipeline, reply with results."""
    raw_text = update.message.text.strip()
    keywords = [kw.strip() for kw in raw_text.split(",") if kw.strip()]
    domain_override = context.user_data.pop("selected_domain", None)

    if not keywords:
        await update.message.reply_text("Không nhận được từ khóa hợp lệ. Thử lại hoặc /cancel.")
        return AWAITING_KEYWORDS

    # Limit to 5 keywords per scan to avoid rate-limit pain
    if len(keywords) > 5:
        keywords = keywords[:5]
        await update.message.reply_text(
            f"Giới hạn 5 từ khóa/lần. Chọn: {', '.join(keywords)}"
        )

    # Let user know we're working on it
    processing_msg = await update.message.reply_text(
        f"\u23F3 Đang phân tích {len(keywords)} từ khóa (multi-source)... (mất ~{len(keywords) * 8}s)"
    )

    # Run the heavy pipeline in a thread so the bot stays responsive
    config = TrendSignalConfig()
    interest_df = await asyncio.to_thread(fetch_trend_signals, keywords, config)

    if interest_df.empty:
        await processing_msg.edit_text(
            "Không lấy được dữ liệu Google Trends. Kiểm tra lại từ khóa hoặc thử lại sau."
        )
        return ConversationHandler.END

    velocity_df = await asyncio.to_thread(velocity_engine, interest_df, domain_override)

    if velocity_df.empty:
        await processing_msg.edit_text("Không đủ dữ liệu để phân tích.")
        return ConversationHandler.END

    # Multi-source enrichment
    geo_code = "VN"  # default geo for interactive scans
    opportunities = await asyncio.to_thread(
        multi_source_engine, velocity_df, geo_code, domain_override,
    )

    # Send individual report per keyword
    await processing_msg.edit_text(f"Phân tích xong {len(opportunities)} từ khóa (multi-source)!")

    # Store resolutions for inline button callbacks
    resolve_index = 0
    for opp in opportunities:
        report = _format_opportunity_report(opp)

        # If keyword is weak, offer inline buttons for refined variants
        if opp.resolution and opp.resolution.is_weak and opp.resolution.refined_keywords:
            res_key = f"res_{resolve_index}"
            context.user_data[res_key] = {
                "original": opp.keyword,
                "refined": opp.resolution.refined_keywords,
                "quality": opp.resolution.quality_label,
            }
            resolve_index += 1

            # Build inline keyboard with next-step options
            variants_str = ", ".join(opp.resolution.refined_keywords[:3])
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton(
                        f"\U0001F50D Quét biến thể ({len(opp.resolution.refined_keywords)})",
                        callback_data=f"res:s:{res_key}",
                    ),
                ],
                [
                    InlineKeyboardButton(
                        "\U0001F4CA So sánh biến thể",
                        callback_data=f"res:c:{res_key}",
                    ),
                    InlineKeyboardButton(
                        "\u2705 Vẫn theo dõi",
                        callback_data=f"res:t:{res_key}",
                    ),
                ],
            ])
            await update.message.reply_text(
                report, parse_mode=ParseMode.HTML, reply_markup=keyboard,
            )
        else:
            await update.message.reply_text(report, parse_mode=ParseMode.HTML)
        await asyncio.sleep(0.3)

    # Send summary
    summary = _format_opportunity_summary(opportunities)
    await update.message.reply_text(summary, parse_mode=ParseMode.HTML)

    # Persist scan results to history so /history works immediately
    await register_user(update.effective_user.id)
    for opp in opportunities:
        wow = 999.0 if opp.wow_growth_pct == float("inf") else float(opp.wow_growth_pct)
        await insert_scan_history(
            keyword=opp.keyword,
            chat_id=update.effective_user.id,
            domain=opp.domain,
            status=opp.status,
            wow_growth=wow,
            confidence=opp.confidence,
            interest=opp.interest,
            acceleration=opp.acceleration_pct,
            consistency=opp.consistency_pct,
            peak_position=opp.peak_position_pct,
            action_label=opp.action_label,
            action_reason=opp.action_reason,
            geo=geo_code,
            opportunity_score=opp.opportunity_score,
            source_count=opp.source_count,
            source_agreement=opp.source_agreement,
            keyword_quality_label=opp.keyword_quality_label,
            evidence_summary=opp.evidence_summary,
            marketplace_presence_score=opp.marketplace_presence,
            marketplace_intent_score=opp.marketplace_intent,
            crowding_risk_score=opp.crowding_risk,
            normalized_keyword=opp.normalized_keyword,
            ambiguity_score=opp.resolution.ambiguity_score if opp.resolution else 0.0,
            commercial_intent_score=opp.resolution.commercial_intent_score if opp.resolution else 0.0,
            resolver_reason=opp.resolution.resolver_reason if opp.resolution else "",
            dedup_minutes=5,
        )
    print(f"[SCAN] Saved {len(opportunities)} keywords to history for user {update.effective_user.id}")

    return ConversationHandler.END


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancel the current conversation."""
    await update.message.reply_text("Đã huỷ. /scan để quét lại, /start để về menu.")
    return ConversationHandler.END


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle inline keyboard button presses."""
    query = update.callback_query
    await query.answer()

    if query.data and query.data.startswith("domain:"):
        domain = query.data.split(":", 1)[1]
        context.user_data["selected_domain"] = domain if domain != "auto" else None
        context.user_data["awaiting_keywords"] = True

        # Close the start menu after domain selection
        await close_callback(query)

        domain_label = domain if domain != "auto" else "Tự phát hiện"
        msg = await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=(
                f"\U0001F50D <b>Lĩnh vực: {domain_label}</b>\n\n"
                "Nhập từ khóa cần quét, cách nhau bằng dấu phẩy.\n"
                "Ví dụ: <i>mật ong, tinh bột nghệ, đường ăn kiêng</i>\n\n"
                "Gửi /cancel để huỷ."
            ),
            parse_mode=ParseMode.HTML,
        )
        _remember_ui(context, msg)

    elif query.data and query.data.startswith("hist:"):
        # History picker callback — token-based
        token = query.data.split(":", 1)[1]
        payload = _cb_resolve(context, token)
        if payload is None:
            await _expired_callback(query)
            return
        keyword = payload["keyword"]
        geo = payload["geo"]
        # Close the picker
        await close_callback(query)
        # Run history lookup
        rows = await get_keyword_history(update.effective_user.id, keyword, limit=10, geo=geo)
        if not rows:
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=(
                    f"Chưa có lịch sử cho <b>{html.escape(keyword)}</b> [{geo}].\n"
                    "Bot sẽ quét tự động khi từ khóa được theo dõi."
                ),
                parse_mode=ParseMode.HTML,
            )
            return
        # Reuse history display logic
        kw_escaped = html.escape(keyword)
        lines = [f"<b>Lịch sử — {kw_escaped}</b> [{geo}] ({len(rows)} lần quét)\n"]
        interests = [r["interest"] for r in reversed(rows)]
        spark = _sparkline(interests)
        if len(rows) >= 2:
            trend_delta = compute_delta(rows[0], rows[-1])
            if trend_delta:
                lines.append(f"<b>Xu hướng tổng:</b> {trend_delta}\n")
        for i, r in enumerate(rows):
            status = r["status"]
            emoji = STATUS_EMOJI.get(status, "\U0001F4CA")
            wow = r["wow_growth"]
            wow_str = f"{wow:+.1f}%" if wow else "—"
            conf = r["confidence"]
            interest = int(r["interest"])
            date_str = r["scanned_at"][:10]
            act_label = r.get("action_label") or ""
            act_line = ""
            if act_label:
                act_emoji = ACTION_EMOJI.get(act_label, "")
                act_vi = ACTION_LABEL_VI.get(act_label, "")
                act_line = f" | {act_emoji} {act_vi}"
            opp_score = r.get("opportunity_score") or 0
            ms_line = f" | Opp: {opp_score:.0f}/100" if opp_score > 0 else ""
            prev_row = rows[i + 1] if i + 1 < len(rows) else None
            delta = compute_delta(r, prev_row)
            delta_line = f"\n    ↪ {delta}" if delta else ""
            lines.append(
                f"{emoji} {date_str} | {status} | WoW: {wow_str} | "
                f"Int: {interest} | Conf: {conf}{act_line}{ms_line}{delta_line}"
            )
        lines.append(f"\nInterest trend: <code>{spark}</code>")
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="\n".join(lines),
            parse_mode=ParseMode.HTML,
        )

    elif query.data == "close":
        await close_callback(query)

    elif query.data == "help":
        await close_callback(query)
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=(
                "<b>Hướng dẫn sử dụng</b>\n\n"
                "1. Nhấn /start → chọn lĩnh vực\n"
                "2. Nhập từ khóa (cách nhau dấu phẩy)\n"
                "3. Bot phân tích và trả kết quả\n\n"
                "/track &lt;kw&gt; — Theo dõi tự động hàng ngày\n"
                "/history &lt;kw&gt; — Xem lịch sử + xu hướng thay đổi\n"
                "/history — Chọn từ khóa từ danh sách theo dõi\n"
                "/mylist — Danh sách theo dõi + GO/WATCH/AVOID\n"
                "/compare kw1, kw2 — So sánh cơ hội 2-5 từ khóa\n"
                "/suggest &lt;kw&gt; — Gợi ý từ khóa liên quan\n"
                "/top — Xem top cơ hội hiện tại\n"
                "/start — Menu chính\n"
                "/help — Xem hướng dẫn này"
            ),
            parse_mode=ParseMode.HTML,
        )
    elif query.data == "top":
        # Trigger /top via inline button — build a fake update
        await cmd_top(update, context)


async def resolve_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle inline button callbacks from keyword resolver suggestions."""
    query = update.callback_query
    await query.answer()

    if not query.data or not query.data.startswith("res:"):
        return

    parts = query.data.split(":", 2)
    if len(parts) < 3:
        return

    action = parts[1]  # s=scan, c=compare, t=track
    res_key = parts[2]

    res_data = context.user_data.get(res_key)
    if not res_data:
        await _expired_callback(query)
        return

    original = res_data["original"]
    refined = res_data["refined"]

    if action == "s":
        # Scan refined variants
        keywords = refined[:5]
        await query.message.reply_text(
            f"\U0001F50D Đang quét {len(keywords)} biến thể của <b>{html.escape(original)}</b>...",
            parse_mode=ParseMode.HTML,
        )
        config = TrendSignalConfig()
        interest_df = await asyncio.to_thread(fetch_trend_signals, keywords, config)
        if interest_df.empty:
            await query.message.reply_text("Không lấy được dữ liệu. Thử lại sau.")
            return
        velocity_df = await asyncio.to_thread(velocity_engine, interest_df)
        if velocity_df.empty:
            await query.message.reply_text("Không đủ dữ liệu để phân tích.")
            return
        opportunities = await asyncio.to_thread(multi_source_engine, velocity_df, "VN")
        for opp in opportunities:
            report = _format_opportunity_report(opp)
            await query.message.reply_text(report, parse_mode=ParseMode.HTML)
            await asyncio.sleep(0.3)
        if opportunities:
            summary = _format_opportunity_summary(opportunities)
            await query.message.reply_text(summary, parse_mode=ParseMode.HTML)

    elif action == "c":
        # Compare refined variants
        if len(refined) < 2:
            await query.message.reply_text("Cần ít nhất 2 biến thể để so sánh.")
            return
        keywords = refined[:5]
        await query.message.reply_text(
            f"\U0001F4CA Đang so sánh {len(keywords)} biến thể...",
            parse_mode=ParseMode.HTML,
        )
        config = TrendSignalConfig()
        interest_df = await asyncio.to_thread(fetch_trend_signals, keywords, config)
        if interest_df.empty:
            await query.message.reply_text("Không lấy được dữ liệu.")
            return
        velocity_df = await asyncio.to_thread(velocity_engine, interest_df)
        if velocity_df.empty:
            await query.message.reply_text("Không đủ dữ liệu.")
            return
        opportunities = await asyncio.to_thread(multi_source_engine, velocity_df, "VN")

        lines = [f"<b>SO SÁNH BIẾN THỂ — {html.escape(original)}</b>\n"]
        for rank, opp in enumerate(opportunities, 1):
            kw_e = html.escape(opp.keyword)
            emoji = STATUS_EMOJI.get(opp.status, "\U0001F4CA")
            act_emoji = ACTION_EMOJI[opp.action_label]
            wow = opp.wow_growth_pct
            wow_str = "INF" if wow == float("inf") else f"{wow:+.0f}%"
            medal = "\U0001F947 " if rank == 1 else ("\U0001F948 " if rank == 2 else "")
            lines.append(
                f"{medal}#{rank} <b>{kw_e}</b>\n"
                f"  <b>Opp: {opp.opportunity_score}/100</b> | {emoji} {opp.status} | WoW: {wow_str}\n"
                f"  {act_emoji} {ACTION_LABEL_VI[opp.action_label]} — {opp.action_reason}"
            )

        winner = opportunities[0]
        if winner.action_label == "GO":
            lines.append(f"\n\U0001F3C6 Biến thể tốt nhất: <b>{html.escape(winner.keyword)}</b>!")
        await query.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)

    elif action == "t":
        # Track original keyword anyway
        await register_user(update.effective_user.id)
        from signal_radar import detect_domain
        domain = detect_domain(original)
        added = await add_keyword(update.effective_user.id, original, domain, geo="VN")
        if added:
            await query.message.reply_text(
                f"\u2705 Đã theo dõi: <b>{html.escape(original)}</b> (bỏ qua gợi ý tinh chỉnh)",
                parse_mode=ParseMode.HTML,
            )
        else:
            await query.message.reply_text(
                f"Bạn đã theo dõi <b>{html.escape(original)}</b> rồi.",
                parse_mode=ParseMode.HTML,
            )

    # Clean up stored resolution data
    context.user_data.pop(res_key, None)


# Fallback message handler — catches free-text when user pressed "Quét trend" button
async def fallback_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle free-text input when user is in the awaiting-keywords state via button."""
    if not context.user_data.get("awaiting_keywords"):
        await update.message.reply_text("Dùng /scan để bắt đầu quét từ khóa.")
        return ConversationHandler.END

    context.user_data["awaiting_keywords"] = False
    return await handle_keywords(update, context)


# ---------------------------------------------------------------------------
# Tracking Commands: /track, /untrack, /mylist
# ---------------------------------------------------------------------------

async def cmd_track(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Add a keyword to the user's tracking list. Supports optional geo."""
    await register_user(update.effective_user.id)

    if not context.args:
        await update.message.reply_text(
            "Dùng: <code>/track từ khóa [VN|US|WW]</code>\n"
            "Ví dụ: <code>/track mật ong</code> (VN mặc định)\n"
            "       <code>/track collagen US</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    # Last arg might be a geo code
    args = list(context.args)
    geo = "VN"
    if len(args) >= 2 and args[-1].upper() in ("VN", "US", "WW"):
        geo = args.pop().upper()

    keyword = " ".join(args).strip()
    if not keyword:
        return

    from signal_radar import detect_domain
    domain = detect_domain(keyword)
    added = await add_keyword(update.effective_user.id, keyword, domain, geo=geo)
    print(f"[TRACK] user={update.effective_user.id} kw={keyword!r} geo={geo} added={added}")

    geo_flag = GEO_FLAGS.get(geo, "\U0001F310")
    geo_label = GEO_LABELS.get(geo, geo)

    if added:
        de = DOMAIN_EMOJI.get(domain, "\U0001F310")
        await update.message.reply_text(
            f"\u2705 Đã theo dõi: <b>{html.escape(keyword)}</b> {de} {domain} {geo_flag} {geo_label}\n"
            "Bot sẽ tự động quét hàng ngày và báo khi có thay đổi trạng thái.",
            parse_mode=ParseMode.HTML,
        )
    else:
        await update.message.reply_text(
            f"Bạn đã theo dõi <b>{html.escape(keyword)}</b> {geo_flag} {geo_label} rồi.",
            parse_mode=ParseMode.HTML,
        )


async def cmd_untrack(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Remove a keyword from the user's tracking list. Supports optional geo."""
    if not context.args:
        await update.message.reply_text(
            "Dùng: <code>/untrack từ khóa [VN|US|WW]</code>\n"
            "Không chỉ định geo = bỏ theo dõi tất cả thị trường.",
            parse_mode=ParseMode.HTML,
        )
        return

    args = list(context.args)
    geo = None
    if len(args) >= 2 and args[-1].upper() in ("VN", "US", "WW"):
        geo = args.pop().upper()

    keyword = " ".join(args).strip()
    removed = await remove_keyword(update.effective_user.id, keyword, geo=geo)

    geo_str = f" {GEO_FLAGS.get(geo, '')} {geo}" if geo else " (tất cả thị trường)"
    if removed:
        await update.message.reply_text(
            f"\u274C Đã bỏ theo dõi: <b>{html.escape(keyword)}</b>{geo_str}",
            parse_mode=ParseMode.HTML,
        )
    else:
        await update.message.reply_text(
            f"Không tìm thấy <b>{html.escape(keyword)}</b>{geo_str} trong danh sách.",
            parse_mode=ParseMode.HTML,
        )


async def cmd_mylist(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show all tracked keywords sorted by importance with action labels + geo + project."""
    keywords = await get_user_keywords(update.effective_user.id)

    if not keywords:
        await update.message.reply_text(
            "Danh sách trống. Dùng /track để thêm từ khóa.",
        )
        return

    # Sort: GO first, then WATCH, then AVOID; within each by confidence desc
    action_order = {"GO": 0, "WATCH": 1, "AVOID": 2}

    def _kw_sort_key(kw):
        status = kw.get("last_status") or "NEW"
        conf = kw.get("last_confidence") or 0
        wow = kw.get("last_wow_growth") or 0
        action, _ = compute_action(status, conf, wow or 0, 50, 0, 50)
        return (action_order.get(action, 2), -conf)

    keywords.sort(key=_kw_sort_key)

    lines = [f"<b>Danh sách theo dõi ({len(keywords)})</b>\n"]
    for kw in keywords:
        status = kw["last_status"] or "NEW"
        domain = kw["domain"] or "General"
        emoji = STATUS_EMOJI.get(status, "\u2753")
        de = DOMAIN_EMOJI.get(domain, "\U0001F310")
        wow = kw["last_wow_growth"]
        wow_str = f"{wow:+.1f}%" if wow and wow != 0 else "—"
        conf = kw["last_confidence"] or 0
        geo = kw.get("geo") or "VN"
        geo_flag = GEO_FLAGS.get(geo, "\U0001F310")

        # Compute action from available data (use neutral defaults for missing metrics)
        action, reason = compute_action(status, conf, wow or 0, 50, 0, 50)
        act_emoji = ACTION_EMOJI[action]
        act_vi = ACTION_LABEL_VI[action]

        # Project info (show if assigned)
        proj_line = ""
        if kw.get("project_id"):
            # We don't have project name here, just note it
            proj_line = " \U0001F4CB"

        lines.append(
            f"{act_emoji} {geo_flag} {de} <b>{html.escape(kw['keyword'])}</b> [{geo}]{proj_line}\n"
            f"  {emoji} {status} | WoW: {wow_str} | Conf: {conf}/100\n"
            f"  <b>{act_vi}</b> — {reason}"
        )

    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


# ---------------------------------------------------------------------------
# Project Commands: /pnew, /plist, /padd, /pview, /pdel
# ---------------------------------------------------------------------------

async def cmd_pnew(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Create a new project. Syntax: /pnew <name> [daily|twice_daily]"""
    await register_user(update.effective_user.id)

    if not context.args:
        await update.message.reply_text(
            "Dùng: <code>/pnew tên_project [daily|twice_daily]</code>\n"
            "Ví dụ: <code>/pnew skincare daily</code>\n"
            "       <code>/pnew supplements twice_daily</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    args = list(context.args)
    scan_freq = "daily"
    if len(args) >= 2 and args[-1].lower() in ("daily", "twice_daily"):
        scan_freq = args.pop().lower()

    name = " ".join(args).strip().lower()
    if not name:
        return

    created = await create_project(update.effective_user.id, name, scan_freq)
    if created:
        freq_vi = "2 lần/ngày" if scan_freq == "twice_daily" else "1 lần/ngày"
        await update.message.reply_text(
            f"\u2705 Tạo project: <b>{html.escape(name)}</b>\n"
            f"Tần suất quét: {freq_vi}\n\n"
            f"Dùng <code>/padd {name} từ_khóa</code> để thêm từ khóa.",
            parse_mode=ParseMode.HTML,
        )
    else:
        await update.message.reply_text(
            f"Project <b>{html.escape(name)}</b> đã tồn tại.",
            parse_mode=ParseMode.HTML,
        )


async def cmd_plist(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """List all projects for the user."""
    projects = await get_user_projects(update.effective_user.id)

    if not projects:
        await update.message.reply_text(
            "Chưa có project nào. Dùng <code>/pnew tên</code> để tạo.",
            parse_mode=ParseMode.HTML,
        )
        return

    lines = ["<b>Danh sách Projects</b>\n"]
    for p in projects:
        freq_vi = "2x/ngày" if p["scan_freq"] == "twice_daily" else "1x/ngày"
        lines.append(
            f"\U0001F4CB <b>{html.escape(p['name'])}</b> ({p['kw_count']} từ khóa) — {freq_vi}\n"
            f"  /pview {html.escape(p['name'])}"
        )

    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def cmd_padd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Add a keyword to a project. Syntax: /padd <project> <keyword> [VN|US|WW]"""
    if not context.args or len(context.args) < 2:
        await update.message.reply_text(
            "Dùng: <code>/padd project từ khóa [VN|US|WW]</code>\n"
            "Ví dụ: <code>/padd skincare mật ong</code>\n"
            "       <code>/padd skincare collagen US</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    # First arg is project name, rest is keyword + optional geo
    project_name = context.args[0].lower()
    args = list(context.args[1:])

    geo = "VN"
    if len(args) >= 2 and args[-1].upper() in ("VN", "US", "WW"):
        geo = args.pop().upper()

    keyword = " ".join(args).strip()
    if not keyword:
        return

    # Check project exists
    project = await get_project(update.effective_user.id, project_name)
    if not project:
        await update.message.reply_text(
            f"Project <b>{html.escape(project_name)}</b> không tồn tại.\n"
            f"Dùng <code>/pnew {html.escape(project_name)}</code> để tạo.",
            parse_mode=ParseMode.HTML,
        )
        return

    from signal_radar import detect_domain
    domain = detect_domain(keyword)
    added = await add_keyword(
        update.effective_user.id, keyword, domain,
        geo=geo, project_id=project["id"],
    )

    geo_flag = GEO_FLAGS.get(geo, "\U0001F310")
    if added:
        await update.message.reply_text(
            f"\u2705 Thêm <b>{html.escape(keyword)}</b> {geo_flag} {geo} vào project <b>{html.escape(project_name)}</b>",
            parse_mode=ParseMode.HTML,
        )
    else:
        await update.message.reply_text(
            f"<b>{html.escape(keyword)}</b> {geo_flag} {geo} đã có trong danh sách.",
            parse_mode=ParseMode.HTML,
        )


async def cmd_pview(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """View project dashboard."""
    if not context.args:
        await update.message.reply_text(
            "Dùng: <code>/pview tên_project</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    project_name = " ".join(context.args).strip().lower()
    project = await get_project(update.effective_user.id, project_name)

    if not project:
        await update.message.reply_text(
            f"Project <b>{html.escape(project_name)}</b> không tồn tại.",
            parse_mode=ParseMode.HTML,
        )
        return

    keywords = await get_project_keywords(update.effective_user.id, project_name)
    freq_vi = "2 lần/ngày" if project["scan_freq"] == "twice_daily" else "1 lần/ngày"

    lines = [
        f"<b>Project: {html.escape(project_name)}</b> ({len(keywords)} từ khóa, {freq_vi})\n"
    ]

    if not keywords:
        lines.append("Chưa có từ khóa. Dùng /padd để thêm.")
        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)
        return

    action_order = {"GO": 0, "WATCH": 1, "AVOID": 2}

    def _sort_key(kw):
        status = kw.get("last_status") or "NEW"
        conf = kw.get("last_confidence") or 0
        wow = kw.get("last_wow_growth") or 0
        action, _ = compute_action(status, conf, wow or 0, 50, 0, 50)
        return (action_order.get(action, 2), -conf)

    keywords.sort(key=_sort_key)

    go_count = 0
    watch_count = 0
    avoid_count = 0

    for kw in keywords:
        status = kw["last_status"] or "NEW"
        domain = kw["domain"] or "General"
        emoji = STATUS_EMOJI.get(status, "\u2753")
        de = DOMAIN_EMOJI.get(domain, "\U0001F310")
        wow = kw["last_wow_growth"]
        wow_str = f"{wow:+.1f}%" if wow and wow != 0 else "—"
        conf = kw["last_confidence"] or 0
        geo = kw.get("geo") or "VN"
        geo_flag = GEO_FLAGS.get(geo, "\U0001F310")

        action, reason = compute_action(status, conf, wow or 0, 50, 0, 50)
        act_emoji = ACTION_EMOJI[action]
        act_vi = ACTION_LABEL_VI[action]

        if action == "GO":
            go_count += 1
        elif action == "AVOID":
            avoid_count += 1
        else:
            watch_count += 1

        lines.append(
            f"{act_emoji} {geo_flag} <b>{html.escape(kw['keyword'])}</b> [{geo}]\n"
            f"  {emoji} {status} | WoW: {wow_str} | Conf: {conf}\n"
            f"  {reason}"
        )

    lines.append(
        f"\n\U0001F7E2 GO: {go_count} | \U0001F7E1 WATCH: {watch_count} | \U0001F534 AVOID: {avoid_count}"
    )

    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def cmd_pdel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Delete a project. Keywords become unassigned (not deleted)."""
    if not context.args:
        await update.message.reply_text(
            "Dùng: <code>/pdel tên_project</code>\n"
            "Từ khóa trong project sẽ không bị xoá, chỉ gỡ khỏi project.",
            parse_mode=ParseMode.HTML,
        )
        return

    project_name = " ".join(context.args).strip().lower()
    deleted = await delete_project(update.effective_user.id, project_name)

    if deleted:
        await update.message.reply_text(
            f"\u274C Đã xoá project <b>{html.escape(project_name)}</b>.\n"
            "Từ khóa vẫn còn trong danh sách theo dõi.",
            parse_mode=ParseMode.HTML,
        )
    else:
        await update.message.reply_text(
            f"Project <b>{html.escape(project_name)}</b> không tồn tại.",
            parse_mode=ParseMode.HTML,
        )


# ---------------------------------------------------------------------------
# /top command — ranked opportunity feed
# ---------------------------------------------------------------------------

async def cmd_top(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show user's top opportunities ranked by score + quality."""
    snapshots = await get_latest_user_snapshots(update.effective_user.id)

    if not snapshots:
        await update.message.reply_text(
            "\U0001F4CA <b>Top cơ hội</b>\n\n"
            "Chưa có dữ liệu. Bắt đầu với:\n"
            "\u2022 /scan — quét từ khóa\n"
            "\u2022 /track mật ong — theo dõi từ khóa\n"
            "\u2022 Bot sẽ tự động quét hàng ngày và xếp hạng cơ hội.",
            parse_mode=ParseMode.HTML,
        )
        return

    # Classify into tiers
    strong_go: list[dict] = []    # score >= 65 and good quality
    worth_watching: list[dict] = []  # score 35-64 or high score but weak quality
    weak_noisy: list[dict] = []   # score < 35 or PERSON/AMBIGUOUS

    for snap in snapshots:
        score = snap.get("opportunity_score") or 0
        quality = snap.get("keyword_quality_label") or ""
        geo = snap.get("geo") or "VN"

        if quality in ("PERSON",) or score < 25:
            weak_noisy.append(snap)
        elif score >= 65 and quality in ("COMMERCIAL", ""):
            strong_go.append(snap)
        elif score >= 35:
            worth_watching.append(snap)
        elif quality in ("AMBIGUOUS", "BROAD", "BRAND"):
            weak_noisy.append(snap)
        else:
            worth_watching.append(snap)

    # Sort each tier by opportunity_score descending
    strong_go.sort(key=lambda s: -(s.get("opportunity_score") or 0))
    worth_watching.sort(key=lambda s: -(s.get("opportunity_score") or 0))
    weak_noisy.sort(key=lambda s: -(s.get("opportunity_score") or 0))

    total = len(snapshots)
    lines = [f"<b>TOP CƠ HỘI</b> ({total} từ khóa)\n"]

    # Quick summary
    go_count = len(strong_go)
    watch_count = len(worth_watching)
    weak_count = len(weak_noisy)
    lines.append(
        f"\U0001F7E2 GO mạnh: {go_count} | \U0001F7E1 Đáng xem: {watch_count} | "
        f"\U0001F534 Yếu/Nhiễu: {weak_count}\n"
    )

    def _format_snap_item(snap: dict, idx: int) -> str:
        kw = html.escape(snap["keyword"])
        score = snap.get("opportunity_score") or 0
        status = snap.get("status") or "UNKNOWN"
        geo = snap.get("geo") or "VN"
        quality = snap.get("keyword_quality_label") or ""
        wow = snap.get("wow_growth") or 0
        wow_str = f"{wow:+.0f}%" if wow else "—"
        conf = snap.get("confidence") or 0
        emoji = STATUS_EMOJI.get(status, "\U0001F4CA")
        geo_flag = GEO_FLAGS.get(geo, "\U0001F310")
        q_badge = _QUALITY_BADGES.get(quality, "")

        # Decision hint
        if score >= 65 and quality in ("COMMERCIAL", ""):
            hint = "Hành động ngay"
        elif quality in ("AMBIGUOUS", "BROAD", "BRAND"):
            hint = "Cần từ khóa cụ thể hơn"
        elif quality == "PERSON":
            hint = "Không phải sản phẩm"
        elif score >= 35:
            hint = "Theo dõi sát"
        else:
            hint = "Tín hiệu yếu"

        return (
            f"  {idx}. {geo_flag} {emoji} <b>{kw}</b> [{geo}]\n"
            f"     Opp: <b>{score:.0f}</b>/100 | WoW: {wow_str} | Conf: {conf}\n"
            f"     {q_badge} {quality} — {hint}"
        )

    if strong_go:
        lines.append(f"\n\U0001F7E2 <b>STRONG GO — Hành động ngay</b>")
        for i, snap in enumerate(strong_go[:5], 1):
            lines.append(_format_snap_item(snap, i))

    if worth_watching:
        lines.append(f"\n\U0001F7E1 <b>WORTH WATCHING — Theo dõi</b>")
        for i, snap in enumerate(worth_watching[:5], 1):
            lines.append(_format_snap_item(snap, i))

    if weak_noisy:
        lines.append(f"\n\U0001F534 <b>WEAK / NOISY — Cần cải thiện</b>")
        for i, snap in enumerate(weak_noisy[:3], 1):
            lines.append(_format_snap_item(snap, i))

    # Next action suggestion
    if strong_go:
        best = strong_go[0]
        best_kw = html.escape(best["keyword"])
        lines.append(
            f"\n\U0001F449 <b>Tiếp:</b> /compare {best_kw}, [từ khóa 2] — so sánh cơ hội tốt nhất"
        )
    elif worth_watching:
        lines.append("\n\U0001F449 <b>Tiếp:</b> Dùng /scan để tìm từ khóa chất lượng hơn")
    else:
        lines.append(
            "\n\U0001F449 <b>Tiếp:</b> Thử /scan với từ khóa cụ thể hơn "
            "(thêm loại sản phẩm, thương hiệu)"
        )

    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


# ---------------------------------------------------------------------------

# Sparkline characters from low to high
_SPARK_CHARS = "▁▂▃▄▅▆▇█"


def _sparkline(values: list[float]) -> str:
    """Generate a Unicode sparkline from a list of numeric values."""
    if not values or max(values) == min(values):
        return _SPARK_CHARS[0] * len(values)
    lo, hi = min(values), max(values)
    scale = len(_SPARK_CHARS) - 1
    return "".join(
        _SPARK_CHARS[min(int((v - lo) / (hi - lo) * scale), scale)]
        for v in values
    )


async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show recent scan history for a tracked keyword with action labels + delta."""
    chat_id = update.effective_user.id

    # --- No args: show inline picker of tracked keywords ---
    if not context.args:
        await _cleanup_prev_ui(context, replace_type="picker")
        keywords = await get_user_keywords(chat_id)
        if not keywords:
            await update.message.reply_text(
                "Danh sách theo dõi trống. Dùng /track để thêm từ khóa trước.",
            )
            return
        buttons = []
        for kw in keywords:
            geo = kw.get("geo") or "VN"
            geo_flag = GEO_FLAGS.get(geo, "\U0001F310")
            token = _cb_token(context, {"keyword": kw["keyword"], "geo": geo})
            buttons.append([InlineKeyboardButton(
                f"{geo_flag} {kw['keyword']} [{geo}]",
                callback_data=f"hist:{token}",
            )])
        buttons.append([InlineKeyboardButton("Đóng", callback_data="close")])
        msg = await update.message.reply_text(
            "<b>Lịch sử — Chọn từ khóa</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons),
        )
        _remember_ui(context, msg, ui_type="picker")
        return

    # --- With args: lookup history ---
    keyword = " ".join(context.args).strip()

    # Extract optional geo from last arg
    geo = None
    parts = keyword.rsplit(None, 1)
    if len(parts) == 2 and parts[-1].upper() in ("VN", "US", "WW"):
        geo = parts[-1].upper()
        keyword = parts[0]

    # Try exact match first
    rows = await get_keyword_history(chat_id, keyword, limit=10, geo=geo)
    print(f"[HISTORY] Exact lookup: user={chat_id} kw={keyword!r} geo={geo} rows={len(rows)}")

    # Fallback: try normalized keyword match
    if not rows:
        from sources import normalize_keyword
        norm_kw = normalize_keyword(keyword)
        rows = await get_keyword_history_normalized(chat_id, norm_kw, limit=10, geo=geo)
        print(f"[HISTORY] Normalized fallback: norm={norm_kw!r} rows={len(rows)}")

    # Fallback: try matching against tracked keywords
    if not rows:
        from sources import normalize_keyword
        tracked = await get_user_keywords(chat_id)
        for tk in tracked:
            if normalize_keyword(tk["keyword"]) == normalize_keyword(keyword):
                rows = await get_keyword_history(
                    chat_id, tk["keyword"], limit=10,
                    geo=tk.get("geo"),
                )
                if rows:
                    keyword = tk["keyword"]
                    print(f"[HISTORY] Tracked fallback: matched={keyword!r} rows={len(rows)}")
                    break

    if not rows:
        # Clear error message explaining why
        tracked = await get_user_keywords(chat_id)
        tracked_names = [tk["keyword"] for tk in tracked]
        is_tracked = any(
            tk["keyword"].lower() == keyword.lower() for tk in tracked
        )
        if is_tracked:
            msg = (
                f"Đã theo dõi <b>{html.escape(keyword)}</b> nhưng chưa có lịch sử quét.\n\n"
                "Bot sẽ quét tự động vào đêm nay.\n"
                "Hoặc dùng <code>/scan</code> để quét ngay."
            )
        else:
            msg = (
                f"Chưa có lịch sử cho <b>{html.escape(keyword)}</b>.\n\n"
                "Dùng <code>/track {keyword}</code> để theo dõi\n"
                "hoặc <code>/scan</code> để quét ngay."
            )
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
        return

    kw_escaped = html.escape(keyword)
    lines = [f"<b>Lịch sử — {kw_escaped}</b> ({len(rows)} lần quét gần nhất)\n"]

    # Build sparkline from interest values (reversed to chronological order)
    interests = [r["interest"] for r in reversed(rows)]
    spark = _sparkline(interests)

    # Determine overall trend from first (newest) vs last (oldest)
    if len(rows) >= 2:
        newest = rows[0]
        oldest = rows[-1]
        trend_delta = compute_delta(newest, oldest)
        if trend_delta:
            lines.append(f"<b>Xu hướng tổng:</b> {trend_delta}\n")

    for i, r in enumerate(rows):
        status = r["status"]
        emoji = STATUS_EMOJI.get(status, "\U0001F4CA")
        wow = r["wow_growth"]
        wow_str = f"{wow:+.1f}%" if wow else "—"
        conf = r["confidence"]
        interest = int(r["interest"])
        date_str = r["scanned_at"][:10]  # YYYY-MM-DD

        # Action label (from DB column, or compute if missing for old rows)
        act_label = r.get("action_label") or ""
        if act_label:
            act_emoji = ACTION_EMOJI.get(act_label, "")
            act_vi = ACTION_LABEL_VI.get(act_label, "")
            act_line = f" | {act_emoji} {act_vi}"
        else:
            act_line = ""

        # Delta vs previous row
        prev_row = rows[i + 1] if i + 1 < len(rows) else None
        delta = compute_delta(r, prev_row)
        delta_line = f"\n    ↪ {delta}" if delta else ""

        # Multi-source fields (graceful for old rows where these may be 0/empty)
        opp_score = r.get("opportunity_score") or 0
        src_count = r.get("source_count") or 0
        evidence = r.get("evidence_summary") or ""
        ms_line = ""
        if opp_score and opp_score > 0:
            ms_line = f" | Opp: {opp_score:.0f}/100"
            if src_count and src_count > 1:
                ms_line += f" | Nguồn: {src_count}/4"

        # Marketplace fields (graceful for old rows)
        mp_presence = r.get("marketplace_presence_score") or 0
        mp_intent = r.get("marketplace_intent_score") or 0
        mp_crowding = r.get("crowding_risk_score") or 0
        mp_line = ""
        if mp_presence > 0 or mp_intent > 0:
            crowd_emoji = "\U0001F7E2" if mp_crowding < 0.3 else ("\U0001F7E1" if mp_crowding < 0.6 else "\U0001F534")
            mp_line = f"\n    \U0001F6D2 MP: {mp_presence:.0%} presence | {mp_intent:.0%} intent | {crowd_emoji} {mp_crowding:.0%} crowding"

        evidence_line = f"\n    <i>{evidence}</i>" if evidence else ""

        lines.append(
            f"{emoji} {date_str} | {status} | WoW: {wow_str} | "
            f"Int: {interest} | Conf: {conf}{act_line}{ms_line}{delta_line}{mp_line}{evidence_line}"
        )

    lines.append(f"\nInterest trend: <code>{spark}</code>")

    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


# ---------------------------------------------------------------------------
# /compare command — rank 2-5 keywords by opportunity
# ---------------------------------------------------------------------------

async def cmd_compare(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Compare 2-5 keywords side-by-side, ranked by opportunity."""
    if not context.args or len(context.args) < 2:
        await update.message.reply_text(
            "Dùng: <code>/compare kw1, kw2, kw3</code> (2-5 từ khóa)\n"
            "Ví dụ: <code>/compare mật ong, tinh bột nghệ, collagen</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    # Parse keywords — support both comma-separated and space-separated
    raw = " ".join(context.args)
    keywords = [kw.strip() for kw in raw.split(",") if kw.strip()]

    if len(keywords) < 2:
        await update.message.reply_text("Cần ít nhất 2 từ khóa để so sánh.")
        return
    if len(keywords) > 5:
        keywords = keywords[:5]
        await update.message.reply_text(f"Giới hạn 5 từ khóa. So sánh: {', '.join(keywords)}")

    processing_msg = await update.message.reply_text(
        f"\u23F3 Đang so sánh {len(keywords)} từ khóa (multi-source)... (mất ~{len(keywords) * 8}s)"
    )

    config = TrendSignalConfig()
    interest_df = await asyncio.to_thread(fetch_trend_signals, keywords, config)

    if interest_df.empty:
        await processing_msg.edit_text(
            "Không lấy được dữ liệu Google Trends. Kiểm tra lại từ khóa."
        )
        return

    velocity_df = await asyncio.to_thread(velocity_engine, interest_df)

    if velocity_df.empty:
        await processing_msg.edit_text("Không đủ dữ liệu để phân tích.")
        return

    # Multi-source enrichment
    opportunities = await asyncio.to_thread(multi_source_engine, velocity_df, "VN")

    # Build comparison table ranked by opportunity_score
    lines = [f"<b>SO SÁNH CƠ HỘI — {len(opportunities)} từ khóa</b>\n"]

    for rank, opp in enumerate(opportunities, 1):
        kw = html.escape(opp.keyword)
        status = opp.status
        emoji = STATUS_EMOJI.get(status, "\U0001F4CA")
        act_emoji = ACTION_EMOJI[opp.action_label]
        act_vi = ACTION_LABEL_VI[opp.action_label]
        conf = opp.confidence
        wow = opp.wow_growth_pct
        wow_str = "INF" if wow == float("inf") else f"{wow:+.0f}%"
        de = DOMAIN_EMOJI.get(opp.domain, "\U0001F310")

        medal = ""
        if rank == 1:
            medal = "\U0001F947 "
        elif rank == 2:
            medal = "\U0001F948 "
        elif rank == 3:
            medal = "\U0001F949 "

        lines.append(
            f"{medal}<b>#{rank}</b> {de} <b>{kw}</b>\n"
            f"  <b>Opportunity: {opp.opportunity_score}/100</b> | Nguồn: {opp.source_count}/4\n"
            f"  {act_emoji} <b>{act_vi}</b> | {emoji} {status} | "
            f"WoW: {wow_str} | Conf: {conf}/100\n"
            f"  {opp.action_reason}"
        )

        # Marketplace line if available
        if opp.marketplace_presence > 0 or opp.marketplace_intent > 0:
            crowd_emoji = "\U0001F7E2" if opp.crowding_risk < 0.3 else ("\U0001F7E1" if opp.crowding_risk < 0.6 else "\U0001F534")
            lines.append(
                f"  \U0001F6D2 MP: {opp.marketplace_presence:.0%} presence | "
                f"{opp.marketplace_intent:.0%} intent | {crowd_emoji} {opp.crowding_risk:.0%} crowding"
            )

        # Quality note for weak keywords
        if opp.resolution and opp.resolution.is_weak:
            q_badge = _QUALITY_BADGES.get(opp.keyword_quality_label, "\u26A0\uFE0F")
            lines.append(
                f"  {q_badge} Từ khóa {opp.keyword_quality_label.lower()} — "
                f"điểm có thể không phản ánh đúng cơ hội"
            )

    # Winner callout
    winner = opportunities[0]
    winner_kw = html.escape(winner.keyword)
    if winner.action_label == "GO":
        lines.append(
            f"\n\U0001F3C6 <b>Cơ hội tốt nhất:</b> {winner_kw} — Điểm {winner.opportunity_score}/100!"
        )
    elif winner.action_label == "WATCH":
        lines.append(
            f"\n\U0001F448 <b>Đáng chú ý nhất:</b> {winner_kw} — Theo dõi sát."
        )
    else:
        lines.append(
            f"\n\U0001F6A7 <b>Kết quả:</b> Không có cơ hội rõ ràng — chờ thêm tín hiệu."
        )

    await processing_msg.edit_text("\n".join(lines), parse_mode=ParseMode.HTML)


# ---------------------------------------------------------------------------
# /suggest command — discover related keywords
# ---------------------------------------------------------------------------

async def cmd_suggest(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Suggest related keywords using multi-source discovery."""
    if not context.args:
        await update.message.reply_text(
            "Dùng: <code>/suggest từ khóa</code>\n"
            "Ví dụ: <code>/suggest mật ong</code>\n\n"
            "Bot sẽ tìm các từ khóa liên quan từ nhiều nguồn.",
            parse_mode=ParseMode.HTML,
        )
        return

    keyword = " ".join(context.args).strip()
    processing_msg = await update.message.reply_text(
        f"\U0001F50D Đang tìm từ khóa liên quan đến <b>{html.escape(keyword)}</b> (multi-source)..."
    )

    suggestions = await asyncio.to_thread(fetch_multi_source_suggestions, keyword, "VN")

    if not suggestions:
        await processing_msg.edit_text(
            f"Không tìm thấy từ khóa liên quan cho <b>{html.escape(keyword)}</b>.\n"
            "Thử từ khóa khác hoặc kiểm tra lại chính tả.",
            parse_mode=ParseMode.HTML,
        )
        return

    lines = [f"<b>Gợi ý — {html.escape(keyword)}</b>\n"]

    # Group by source type
    autocomplete = [s for s in suggestions if s["type"] == "autocomplete"]
    rising = [s for s in suggestions if s["type"] == "rising"]
    top = [s for s in suggestions if s["type"] == "top"]
    commercial = [s for s in suggestions if s["type"] == "commercial"]

    if autocomplete:
        lines.append("\U0001F4F1 <b>Autocomplete gợi ý:</b>")
        for s in autocomplete[:5]:
            lines.append(f"  \u2022 {html.escape(s['keyword'])}")
        lines.append("")

    if rising:
        lines.append("\U0001F525 <b>Đang tăng:</b>")
        for s in rising:
            kw = html.escape(s["keyword"])
            val = html.escape(s["value"]) if s["value"] else ""
            val_str = f" ({val})" if val else ""
            lines.append(f"  \u2022 {kw}{val_str}")
        lines.append("")

    if top:
        lines.append("\U0001F4C8 <b>Phổ biến:</b>")
        for s in top:
            kw = html.escape(s["keyword"])
            val = html.escape(s["value"]) if s["value"] else ""
            val_str = f" ({val})" if val else ""
            lines.append(f"  \u2022 {kw}{val_str}")
        lines.append("")

    if commercial:
        lines.append("\U0001F6D2 <b>Biến thể thương mại:</b>")
        for s in commercial:
            lines.append(f"  \u2022 {html.escape(s['keyword'])}")

    lines.append(f"\n\u2022 Dùng <code>/track từ khóa</code> để theo dõi")
    lines.append(f"\u2022 Dùng <code>/compare {html.escape(keyword)}, từ_khóa_2</code> để so sánh")

    await processing_msg.edit_text("\n".join(lines), parse_mode=ParseMode.HTML)


# ---------------------------------------------------------------------------
# Background Silent Tracker (runs daily at 00:00)
# ---------------------------------------------------------------------------

_BURSTING_IMMEDIATE = {"BURSTING"}


async def _daily_scan(context: ContextTypes.DEFAULT_TYPE, keywords_override: list[dict] | None = None) -> None:
    """Background job: scan tracked keywords, save history, push digest + alerts."""
    print("[TRACKER] Scan starting...")
    all_keywords = keywords_override or await get_all_tracked_keywords()

    if not all_keywords:
        print("[TRACKER] No tracked keywords — skipping.")
        return

    # Group by geo for multi-market scanning
    geo_groups: dict[str, list[dict]] = {}
    for kw in all_keywords:
        geo = kw.get("geo") or "VN"
        geo_groups.setdefault(geo, []).append(kw)

    # Process each geo separately
    all_results: list[tuple[OpportunityResult, dict, str]] = []  # (opp, tracked, geo)

    for geo, geo_keywords in geo_groups.items():
        config = make_geo_config(geo)
        unique_keywords = list({kw["keyword"] for kw in geo_keywords})

        interest_df = await asyncio.to_thread(fetch_trend_signals, unique_keywords, config)
        if interest_df.empty:
            print(f"[TRACKER] No data for geo={geo} — skipping.")
            continue

        velocity_df = await asyncio.to_thread(velocity_engine, interest_df)
        if velocity_df.empty:
            print(f"[TRACKER] Velocity engine empty for geo={geo} — skipping.")
            continue

        # Multi-source enrichment
        opportunities = await asyncio.to_thread(multi_source_engine, velocity_df, geo)
        opp_map = {o.keyword: o for o in opportunities}

        for tracked in geo_keywords:
            opp = opp_map.get(tracked["keyword"])
            if opp is None:
                continue
            all_results.append((opp, tracked, geo))

    if not all_results:
        print("[TRACKER] No results across all geos — skipping.")
        return

    # ---- Phase 1: Save history + update tracked_keywords ----
    for opp, tracked, geo in all_results:
        new_status = opp.status
        wow = 999.0 if opp.wow_growth_pct == float("inf") else float(opp.wow_growth_pct)
        conf = opp.confidence
        domain = opp.domain
        interest = opp.interest

        await insert_scan_history(
            keyword=tracked["keyword"],
            chat_id=tracked["chat_id"],
            domain=domain,
            status=new_status,
            wow_growth=wow,
            confidence=conf,
            interest=interest,
            acceleration=opp.acceleration_pct,
            consistency=opp.consistency_pct,
            peak_position=opp.peak_position_pct,
            action_label=opp.action_label,
            action_reason=opp.action_reason,
            geo=geo,
            opportunity_score=opp.opportunity_score,
            source_count=opp.source_count,
            source_agreement=opp.source_agreement,
            keyword_quality_label=opp.keyword_quality_label,
            evidence_summary=opp.evidence_summary,
            marketplace_presence_score=opp.marketplace_presence,
            marketplace_intent_score=opp.marketplace_intent,
            crowding_risk_score=opp.crowding_risk,
            normalized_keyword=opp.normalized_keyword,
            ambiguity_score=opp.resolution.ambiguity_score if opp.resolution else 0.0,
            commercial_intent_score=opp.resolution.commercial_intent_score if opp.resolution else 0.0,
            resolver_reason=opp.resolution.resolver_reason if opp.resolution else "",
        )

        await update_keyword_status(
            keyword_id=tracked["id"],
            status=new_status,
            wow_growth=wow,
            confidence=conf,
            domain=domain,
        )

    # ---- Phase 2: Immediate BURSTING alerts (with noise reduction) ----
    _ALERT_COOLDOWN_HOURS = 24

    for opp, tracked, geo in all_results:
        new_status = opp.status
        old_status = tracked.get("last_status") or "UNKNOWN"
        action = opp.action_label
        reason = opp.action_reason

        # Noise reduction: only alert on status transition to BURSTING/EMERGING
        if new_status not in {"BURSTING", "EMERGING"} or old_status == new_status:
            continue

        # Noise reduction: must be GO or high confidence
        conf = opp.confidence
        if action != "GO" and conf < 30:
            continue

        # Noise reduction: 24h cooldown per keyword
        last_alert = await get_last_alert_time(tracked["id"])
        if last_alert:
            try:
                last_dt = datetime.fromisoformat(last_alert)
                hours_since = (datetime.utcnow() - last_dt).total_seconds() / 3600
                if hours_since < _ALERT_COOLDOWN_HOURS:
                    continue
            except (ValueError, TypeError):
                pass

        domain = opp.domain
        wow = 999.0 if opp.wow_growth_pct == float("inf") else float(opp.wow_growth_pct)
        de = DOMAIN_EMOJI.get(domain, "\U0001F310")
        rec = get_recommendation(domain, new_status)
        act_emoji = ACTION_EMOJI[action]
        act_vi = ACTION_LABEL_VI[action]
        geo_flag = GEO_FLAGS.get(geo, "\U0001F310")

        delta = compute_delta(
            {"status": new_status, "confidence": conf, "wow_growth": wow,
             "interest": opp.interest},
            {"status": old_status, "confidence": tracked.get("last_confidence") or 0,
             "wow_growth": tracked.get("last_wow_growth") or 0, "interest": 0},
        )
        delta_line = f"\n↪ {delta}" if delta else ""

        message = (
            f"<b>\U0001F6A8 {new_status} ALERT \U0001F6A8</b>\n\n"
            f"<b>{html.escape(tracked['keyword'])}</b> {geo_flag} {geo} {de} {domain}\n"
            f"Trạng thái: {old_status} → <b>{new_status}</b>\n"
            f"Wow: +{wow:.1f}% | Confidence: {conf}/100 | Opportunity: {opp.opportunity_score}/100\n"
            f"{act_emoji} <b>{act_vi}</b> — {reason}{delta_line}\n"
            f"→ <b>{rec}</b>"
        )

        try:
            await context.bot.send_message(
                chat_id=tracked["chat_id"],
                text=message,
                parse_mode=ParseMode.HTML,
            )
            await update_alert_time(tracked["id"])
        except Exception as exc:
            print(f"[TRACKER] Failed to alert {tracked['chat_id']}: {exc}")

    # ---- Phase 3: Per-user daily digest (portfolio style) ----
    user_chat_ids = list({kw["chat_id"] for kw in all_keywords}) if keywords_override is None else list({t["chat_id"] for _, t, _ in all_results})

    for chat_id in user_chat_ids:
        user_items = [(o, t, g) for o, t, g in all_results if t["chat_id"] == chat_id]

        if not user_items:
            continue

        # Partition into GO / WATCH / AVOID
        go_items: list[tuple] = []
        watch_items: list[tuple] = []
        avoid_items: list[tuple] = []

        for opp, tracked, geo in user_items:
            delta = compute_delta(
                {"status": opp.status, "confidence": opp.confidence,
                 "wow_growth": opp.wow_growth_pct, "interest": opp.interest},
                {"status": tracked.get("last_status") or "UNKNOWN",
                 "confidence": tracked.get("last_confidence") or 0,
                 "wow_growth": tracked.get("last_wow_growth") or 0, "interest": 0},
            )

            entry = (opp, opp.action_label, opp.action_reason, delta, geo)
            if opp.action_label == "GO":
                go_items.append(entry)
            elif opp.action_label == "AVOID":
                avoid_items.append(entry)
            else:
                watch_items.append(entry)

        # Build takeaway
        parts = []
        if go_items:
            parts.append(f"{len(go_items)} cơ hội đáng vào")
        if watch_items:
            parts.append(f"{len(watch_items)} từ khóa cần theo dõi")
        if avoid_items:
            parts.append(f"{len(avoid_items)} tín hiệu đang yếu đi")
        takeaway = "Hôm nay có " + ", ".join(parts) + "." if parts else "Không có thay đổi đáng chú ý."

        # Hottest keyword by opportunity score
        all_sorted = sorted(user_items, key=lambda x: -x[0].opportunity_score)
        hottest = all_sorted[0] if all_sorted else None
        hottest_line = ""
        if hottest:
            h_opp, _, h_geo = hottest
            h_flag = GEO_FLAGS.get(h_geo, "\U0001F310")
            hottest_line = (
                f"\U0001F525 Hot nhất: <b>{html.escape(h_opp.keyword)}</b> {h_flag} {h_geo} "
                f"(Opp: {h_opp.opportunity_score}/100)"
            )

        lines = [f"<b>BÁO CÁO HÀNG NGÀY</b>\n"]
        lines.append(f"<i>{takeaway}</i>")
        if hottest_line:
            lines.append(f"{hottest_line}\n")

        def _format_section(title: str, emoji: str, items: list[tuple]) -> None:
            if not items:
                return
            items.sort(key=lambda x: -x[0].opportunity_score)
            lines.append(f"{emoji} <b>{title}</b>")
            for opp, action, reason, delta, geo in items:
                status = opp.status
                domain = opp.domain
                s_emoji = STATUS_EMOJI.get(status, "\U0001F4CA")
                de = DOMAIN_EMOJI.get(domain, "\U0001F310")
                kw = html.escape(opp.keyword)
                wow = opp.wow_growth_pct
                wow_str = "INF" if wow == float("inf") else f"{wow:+.1f}"
                conf = opp.confidence
                interest = int(round(opp.interest))
                geo_flag = GEO_FLAGS.get(geo, "\U0001F310")
                delta_line = f"\n  ↪ {delta}" if delta else ""

                lines.append(
                    f"  {geo_flag} {de} <b>{kw}</b> [{geo}]\n"
                    f"  {s_emoji} {status} | Opp: {opp.opportunity_score}/100 | WoW: {wow_str}% | Conf: {conf}\n"
                    f"  {reason}{delta_line}"
                )
            lines.append("")

        _format_section("HÀNH ĐỘNG", "\U0001F7E2", go_items)
        _format_section("THEO DÕI", "\U0001F7E1", watch_items)
        _format_section("TRÁNH / YẾU", "\U0001F534", avoid_items)

        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text="\n".join(lines),
                parse_mode=ParseMode.HTML,
            )
        except Exception as exc:
            print(f"[TRACKER] Failed to send digest to {chat_id}: {exc}")

    total = len(all_results)
    print(f"[TRACKER] Scan complete — {total} keywords processed across {len(geo_groups)} geos.")


# ---------------------------------------------------------------------------
# /export command — export scan history as CSV
# ---------------------------------------------------------------------------

import tempfile


async def cmd_export(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Export scan history as CSV. /export [keyword|project_name|all]"""
    if not context.args:
        await update.message.reply_text(
            "Dùng: <code>/export từ_khóa</code> — xuất 1 từ khóa\n"
            "     <code>/export project tên</code> — xuất theo project\n"
            "     <code>/export all</code> — xuất tất cả",
            parse_mode=ParseMode.HTML,
        )
        return

    args = list(context.args)

    if args[0].lower() == "project" and len(args) >= 2:
        project_name = " ".join(args[1:]).strip().lower()
        project = await get_project(update.effective_user.id, project_name)
        if not project:
            await update.message.reply_text(
                f"Project <b>{html.escape(project_name)}</b> không tồn tại.",
                parse_mode=ParseMode.HTML,
            )
            return
        csv_content = await export_user_history_csv(update.effective_user.id, project_name=project_name)
        filename = f"signal_radar_{project_name}.csv"
    elif args[0].lower() == "all":
        csv_content = await export_user_history_csv(update.effective_user.id)
        filename = "signal_radar_all.csv"
    else:
        keyword = " ".join(args).strip()
        csv_content = await export_user_history_csv(update.effective_user.id, keyword=keyword)
        filename = f"signal_radar_{keyword.replace(' ', '_')}.csv"

    if not csv_content.strip():
        await update.message.reply_text("Không có dữ liệu để xuất.")
        return

    # Write to temp file and send as document
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, encoding="utf-8"
    ) as f:
        f.write(csv_content)
        tmp_path = f.name

    try:
        with open(tmp_path, "rb") as f:
            await update.message.reply_document(
                document=f,
                filename=filename,
                caption=f"Signal Radar — {filename}",
            )
    finally:
        os.unlink(tmp_path)


# ---------------------------------------------------------------------------
# Midday scan for twice_daily projects
# ---------------------------------------------------------------------------

async def _midday_scan(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Run the midday scan for twice_daily projects only."""
    print("[TRACKER] Midday scan for twice_daily projects...")
    twice_daily = await get_twice_daily_projects()

    if not twice_daily:
        print("[TRACKER] No twice_daily projects — skipping midday.")
        return

    project_ids = [p["id"] for p in twice_daily]
    keywords = await get_keywords_for_project_ids(project_ids)

    if not keywords:
        print("[TRACKER] No keywords in twice_daily projects — skipping.")
        return

    await _daily_scan(context, keywords_override=keywords)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def _post_init(application) -> None:
    """Run after Application is built — init database."""
    await init_db()
    print("[BOT] Database initialised.")


def main() -> None:
    if not BOT_TOKEN:
        print("[ERROR] TELEGRAM_BOT_TOKEN not set. Check .env file.")
        return

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(_post_init)
        .build()
    )

    # Conversation for /scan flow
    scan_conversation = ConversationHandler(
        entry_points=[CommandHandler("scan", cmd_scan)],
        states={
            AWAITING_KEYWORDS: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_keywords)],
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
    )

    application.add_handler(scan_conversation)
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("help", cmd_help))
    application.add_handler(CommandHandler("track", cmd_track))
    application.add_handler(CommandHandler("untrack", cmd_untrack))
    application.add_handler(CommandHandler("mylist", cmd_mylist))
    application.add_handler(CommandHandler("history", cmd_history))
    application.add_handler(CommandHandler("compare", cmd_compare))
    application.add_handler(CommandHandler("suggest", cmd_suggest))
    application.add_handler(CommandHandler("export", cmd_export))
    application.add_handler(CommandHandler("top", cmd_top))
    application.add_handler(CommandHandler("pnew", cmd_pnew))
    application.add_handler(CommandHandler("plist", cmd_plist))
    application.add_handler(CommandHandler("padd", cmd_padd))
    application.add_handler(CommandHandler("pview", cmd_pview))
    application.add_handler(CommandHandler("pdel", cmd_pdel))
    application.add_handler(CallbackQueryHandler(resolve_callback, pattern=r"^res:"))
    application.add_handler(CallbackQueryHandler(button_callback))

    # Catch free-text when user clicked the inline button (not in /scan conversation)
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, fallback_text)
    )

    # Background tracker — daily at 00:00
    application.job_queue.run_daily(
        _daily_scan,
        time=datetime.strptime("00:00", "%H:%M").time(),
    )

    # Midday scan for twice_daily projects at 12:00
    application.job_queue.run_daily(
        _midday_scan,
        time=datetime.strptime("12:00", "%H:%M").time(),
    )

    print("Signal Radar bot v5 is running... Press Ctrl+C to stop.")
    application.run_polling()


if __name__ == "__main__":
    main()
