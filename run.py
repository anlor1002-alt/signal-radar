"""Run Signal Radar bot + health check server for Hugging Face Spaces."""
from __future__ import annotations

import os
import sys
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

from dotenv import load_dotenv

load_dotenv()


class HealthHandler(BaseHTTPRequestHandler):
    """Minimal HTTP server on port 7860 so HF Spaces sees the container as healthy."""

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Signal Radar bot is running.")

    def log_message(self, format, *args):
        pass  # silence request logs


def _start_health_server():
    server = HTTPServer(("0.0.0.0", 7860), HealthHandler)
    server.serve_forever()


if __name__ == "__main__":
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    if not token:
        print("[ERROR] TELEGRAM_BOT_TOKEN not set.")
        sys.exit(1)

    # Health check in background thread (HF Spaces requirement)
    threading.Thread(target=_start_health_server, daemon=True).start()

    # Start the Telegram bot (blocking)
    from bot import main
    main()
