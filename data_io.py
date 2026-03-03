"""
共通データI/O関数
Streamlitに依存しない汎用的なファイル操作
"""
import os
import json
import requests
import time

from config import (
    WATCHLIST_FILE, NAME_OVERRIDE_FILE, JQUANTS_TOKEN_FILE,
    LAST_DATA_FILE, BUY_TIMING_HISTORY_FILE
)


def load_watchlists():
    """ウォッチリストをJSONから読み込む"""
    if os.path.exists(WATCHLIST_FILE):
        try:
            with open(WATCHLIST_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"メイン": "7203, 7974, AAPL"}


def save_watchlists(data):
    """ウォッチリストをJSONに保存する"""
    with open(WATCHLIST_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def load_name_overrides():
    """企業名の手動修正辞書を読み込む"""
    if os.path.exists(NAME_OVERRIDE_FILE):
        try:
            with open(NAME_OVERRIDE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_name_overrides(overrides):
    """企業名の手動修正辞書を保存する"""
    with open(NAME_OVERRIDE_FILE, "w", encoding="utf-8") as f:
        json.dump(overrides, f, ensure_ascii=False, indent=2)


def get_jquants_company_name(api_key, code, retries=3):
    """J-Quants V2 APIから企業名を取得"""
    jq_code = code + "0" if len(code) == 4 else code
    url = f"https://api.jquants.com/v2/equities/master?code={jq_code}"
    headers = {"x-api-key": api_key}
    for attempt in range(retries):
        try:
            res = requests.get(url, headers=headers, timeout=10)
            if res.status_code == 200:
                data = res.json().get("data", [])
                if data:
                    name = data[-1].get("CompanyName") or data[-1].get("CoName")
                    if name:
                        return name
            elif res.status_code == 429:
                wait = 3.0 * (attempt + 1)
                print(f"  [WAIT] J-Quants 429 レート制限 ({code}), {wait}秒待機...")
                time.sleep(wait)
                continue
            else:
                print(f"  [WARN] J-Quants {res.status_code} ({code})")
                break
        except Exception as e:
            print(f"  [ERR] J-Quants 例外 ({code}): {e}")
    return None


def get_earnings_date(stock):
    """yfinanceのget_calendar()から次回決算日を取得するヘルパー関数"""
    try:
        cal = stock.get_calendar()
        if cal is None:
            return "-"
        if isinstance(cal, dict):
            edates = cal.get('Earnings Date')
            if not edates:
                return "-"
            if isinstance(edates, list) and len(edates) > 0:
                val = edates[0]
            else:
                val = edates
            return val.strftime('%Y-%m-%d') if hasattr(val, 'strftime') else str(val)
        if hasattr(cal, 'empty') and not cal.empty and 'Earnings Date' in cal.index:
            edates = cal.loc['Earnings Date']
            if isinstance(edates, list) and len(edates) > 0:
                val = edates[0]
            else:
                val = edates
            return val.strftime('%Y-%m-%d') if hasattr(val, 'strftime') else str(val)
    except Exception:
        pass
    return "-"


def get_jquants_api_key_from_env():
    """環境変数またはローカルファイルからAPIキーを取得（Streamlit非依存）"""
    env_key = os.environ.get("JQUANTS_API_KEY", "")
    if env_key:
        return env_key
    if os.path.exists(JQUANTS_TOKEN_FILE):
        with open(JQUANTS_TOKEN_FILE, "r", encoding="utf-8") as f:
            return f.read().strip()
    return ""
