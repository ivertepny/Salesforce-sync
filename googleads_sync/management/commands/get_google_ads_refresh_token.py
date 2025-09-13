import os, sys, json, webbrowser
from urllib.parse import urlencode, parse_qs
from http.server import HTTPServer, BaseHTTPRequestHandler
from django.core.management.base import BaseCommand
import requests

AUTH_URI = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URI = "https://oauth2.googleapis.com/token"
SCOPE = "https://www.googleapis.com/auth/adwords"  # Google Ads API

class Command(BaseCommand):
    help = "Interactive flow to obtain Google Ads OAuth2 refresh token."

    def handle(self, *args, **opts):
        client_id = os.environ.get("GOOGLE_ADS_CLIENT_ID")
        client_secret = os.environ.get("GOOGLE_ADS_CLIENT_SECRET")
        if not client_id or not client_secret:
            self.stderr.write("Set GOOGLE_ADS_CLIENT_ID and GOOGLE_ADS_CLIENT_SECRET in env.")
            sys.exit(1)

        # Для Installed App — використовуємо локальний редірект
        redirect_uri = "http://127.0.0.1:8080/oauth2callback"

        # 1) Отримуємо авторизаційний код
        auth_params = {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "access_type": "offline",
            "prompt": "consent",
            "scope": SCOPE,
        }
        auth_url = f"{AUTH_URI}?{urlencode(auth_params)}"
        self.stdout.write(f"Opening browser for Google OAuth:\n{auth_url}\n")
        try:
            webbrowser.open(auth_url)
        except Exception:
            self.stdout.write("Open this URL manually if the browser didn't open.")

        code_holder = {"code": None}

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                qs = parse_qs(self.path.split("?", 1)[1]) if "?" in self.path else {}
                code_holder["code"] = qs.get("code", [None])[0]
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.end_headers()
                self.wfile.write(b"You can close this window.")
            def log_message(self, fmt, *args):  # silence
                return

        httpd = HTTPServer(("127.0.0.1", 8080), Handler)
        self.stdout.write("Waiting for Google redirect on http://127.0.0.1:8080 ...")
        while code_holder["code"] is None:
            httpd.handle_request()
        code = code_holder["code"]

        # 2) Обмін коду на токени
        data = {
            "code": code,
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        }
        resp = requests.post(TOKEN_URI, data=data, timeout=30)
        resp.raise_for_status()
        token_payload = resp.json()
        refresh_token = token_payload.get("refresh_token")
        access_token = token_payload.get("access_token")
        self.stdout.write(self.style.SUCCESS("Success!"))
        self.stdout.write(json.dumps(token_payload, indent=2))
        if not refresh_token:
            self.stderr.write("No refresh_token returned. Ensure 'prompt=consent' and Desktop App creds.")
            sys.exit(2)

        self.stdout.write("\nAdd to your .env:")
        self.stdout.write(f"GOOGLE_ADS_REFRESH_TOKEN={refresh_token}")
        self.stdout.write(f"# Temporary access token (will be auto-refreshed by client libs): {access_token[:12]}...")
