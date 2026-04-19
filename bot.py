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
    TrendSignalConfig,
    fetch_trend_signals,
    get_recommendation,
    velocity_engine,
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


def _format_results_message(results) -> str:
    """Build a rich HTML-formatted report from velocity engine v2 results."""
    if results.empty:
        return "Không có đủ dữ liệu để phân tích. Thử từ khóa khác hoặc kiểm tra kết nối mạng."

    lines = ["<b>SIGNAL RADAR — Báo cáo phân tích</b>\n"]

    # --- Per-keyword detail ---
    for _, row in results.iterrows():
        kw = html.escape(str(row["keyword"]))
        status = str(row["status"])
        domain = str(row.get("domain", "General"))
        emoji = STATUS_EMOJI.get(status, "\U0001F4CA")
        domain_emoji = DOMAIN_EMOJI.get(domain, "\U0001F310")
        wow = row["wow_growth_pct"]
        wow_str = "INF" if wow == float("inf") else f"{wow:.1f}"
        conf = int(row["confidence"])
        consistency = int(row["consistency_pct"])
        peak = int(row["peak_position_pct"])
        accel = row["acceleration_pct"]
        accel_str = f"{accel:+.1f}%"
        interest = int(round(float(row["interest"])))

        # Domain-specific recommendation
        rec = get_recommendation(domain, status)

        # Confidence bar
        filled = conf // 10
        bar = "\u2588" * filled + "\u2591" * (10 - filled)

        lines.append(
            f"{emoji} <b>{kw}</b> {domain_emoji} {domain}\n"
            f"  Interest: {interest} | WoW: +{wow_str}%\n"
            f"  Gia tốc: {accel_str} | Bền vững: {consistency}%\n"
            f"  Đỉnh 30d: {peak}% | Confidence: {bar} {conf}/100\n"
            f"  → <b>{rec}</b>"
        )

    # --- Summary block ---
    total = len(results)
    status_counts = results["status"].value_counts().to_dict()
    bursting = status_counts.get("BURSTING", 0)
    emerging = status_counts.get("EMERGING", 0)
    rising = status_counts.get("RISING", 0)
    stable = status_counts.get("STABLE", 0)
    declining = status_counts.get("DECLINING", 0)

    # Domain breakdown
    domain_counts = results["domain"].value_counts().to_dict() if "domain" in results.columns else {}
    domain_lines = []
    for d, count in domain_counts.items():
        de = DOMAIN_EMOJI.get(d, "\U0001F310")
        domain_lines.append(f"  {de} {d}: {count}")

    # Top 3 by confidence
    top3 = results.nlargest(3, "confidence")[["keyword", "confidence", "status", "domain"]]
    top_lines = []
    for _, r in top3.iterrows():
        e = STATUS_EMOJI.get(str(r["status"]), "")
        de = DOMAIN_EMOJI.get(str(r.get("domain", "")), "")
        top_lines.append(
            f"  {e} {de} {html.escape(str(r['keyword']))} ({int(r['confidence'])}/100)"
        )

    # Overall recommendation
    if bursting > 0:
        rec = "\U0001F6A8 Xu hướng bùng nổ phát hiện — hành động ngay!"
    elif emerging > 0:
        rec = "\U0001F525 Tín hiệu sớm — cần theo dõi sát."
    elif rising > 0:
        rec = "\U0001F4C8 Xu hướng đang tăng — phân tích thêm."
    else:
        rec = "\U0001F4CA Thị trường ổn định — chưa có tín hiệu mạnh."

    lines.append(
        f"\n<b>TỔNG KẾT</b>\n"
        f"Quét: {total} từ khóa\n"
        f"\U0001F6A8 Bursting: {bursting} | \U0001F525 Emerging: {emerging} | "
        f"\U0001F4C8 Rising: {rising}\n"
        f"\U0001F4CA Stable: {stable} | \U0001F4C9 Declining: {declining}\n\n"
        f"<b>Lĩnh vực:</b>\n" + "\n".join(domain_lines) +
        f"\n\n<b>Top tiềm năng:</b>\n" + "\n".join(top_lines) + f"\n\n{rec}"
    )

    return "\n\n".join(lines)


# ---------------------------------------------------------------------------
# Command Handlers
# ---------------------------------------------------------------------------

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send welcome message with domain selection menu."""
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
        ],
    ])

    await update.message.reply_text(
        "<b>SIGNAL RADAR</b>\n\n"
        "Phát hiện xu hướng trước 2-4 tuần khi demand bùng nổ.\n\n"
        "Chọn lĩnh vực muốn quét:",
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show usage instructions."""
    await update.message.reply_text(
        "<b>Hướng dẫn sử dụng</b>\n\n"
        "/scan — Bắt đầu quét từ khóa\n"
        "  Bot sẽ hỏi bạn nhập từ khóa.\n"
        "  Nhập mỗi từ khóa cách nhau bằng dấu phẩy.\n"
        "  Ví dụ: <i>mật ong, tinh bột nghệ, đường ăn kiêng</i>\n\n"
        "/start — Về menu chính\n"
        "/help — Xem hướng dẫn này",
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
        f"\u23F3 Đang phân tích {len(keywords)} từ khóa... (mất ~{len(keywords) * 5}s)"
    )

    # Run the heavy pipeline in a thread so the bot stays responsive
    config = TrendSignalConfig()
    interest_df = await asyncio.to_thread(fetch_trend_signals, keywords, config)

    if interest_df.empty:
        await processing_msg.edit_text(
            "Không lấy được dữ liệu Google Trends. Kiểm tra lại từ khóa hoặc thử lại sau."
        )
        return ConversationHandler.END

    results = await asyncio.to_thread(velocity_engine, interest_df, domain_override)
    reply = _format_results_message(results)

    # Telegram message limit is 4096 chars — split if needed
    if len(reply) <= 4096:
        await processing_msg.edit_text(reply, parse_mode=ParseMode.HTML)
    else:
        await processing_msg.edit_text("Phân tích xong! Kết quả chi tiết:")
        # Split into chunks of ~4000 chars on double-newline boundaries
        chunks, current = [], ""
        for paragraph in reply.split("\n\n"):
            if len(current) + len(paragraph) + 2 > 4000:
                chunks.append(current)
                current = paragraph
            else:
                current = current + "\n\n" + paragraph if current else paragraph
        if current:
            chunks.append(current)

        for chunk in chunks:
            await update.message.reply_text(chunk, parse_mode=ParseMode.HTML)

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

        domain_label = domain if domain != "auto" else "Tự phát hiện"
        await query.message.reply_text(
            f"\U0001F50D <b>Lĩnh vực: {domain_label}</b>\n\n"
            "Nhập từ khóa cần quét, cách nhau bằng dấu phẩy.\n"
            "Ví dụ: <i>mật ong, tinh bột nghệ, đường ăn kiêng</i>\n\n"
            "Gửi /cancel để huỷ.",
            parse_mode=ParseMode.HTML,
        )
    elif query.data == "help":
        await query.message.reply_text(
            "<b>Hướng dẫn sử dụng</b>\n\n"
            "1. Nhấn /start → chọn lĩnh vực\n"
            "2. Nhập từ khóa (cách nhau dấu phẩy)\n"
            "3. Bot phân tích và trả kết quả\n\n"
            "Hoặc dùng /scan để quét nhanh (tự detect lĩnh vực).\n\n"
            "/start — Menu chính\n"
            "/help — Xem hướng dẫn này",
            parse_mode=ParseMode.HTML,
        )


# Fallback message handler — catches free-text when user pressed "Quét trend" button
async def fallback_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle free-text input when user is in the awaiting-keywords state via button."""
    if not context.user_data.get("awaiting_keywords"):
        await update.message.reply_text("Dùng /scan để bắt đầu quét từ khóa.")
        return ConversationHandler.END

    context.user_data["awaiting_keywords"] = False
    return await handle_keywords(update, context)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if not BOT_TOKEN:
        print("[ERROR] TELEGRAM_BOT_TOKEN not set. Check .env file.")
        return

    application = Application.builder().token(BOT_TOKEN).build()

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
    application.add_handler(CallbackQueryHandler(button_callback))

    # Catch free-text when user clicked the inline button (not in /scan conversation)
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, fallback_text)
    )

    print("Signal Radar bot is running... Press Ctrl+C to stop.")
    application.run_polling()


if __name__ == "__main__":
    main()
