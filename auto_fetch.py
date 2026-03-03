"""
自動データ取得スクリプト (GitHub Actions用)
全ウォッチリストのデータを取得してJSONに保存する
"""
import yfinance as yf
import pandas as pd
from datetime import datetime
import os
import json
import re
from deep_translator import GoogleTranslator
import time
import warnings

from config import WATCHLIST_FILE, LAST_DATA_FILE, NAME_OVERRIDE_FILE, BUY_TIMING_HISTORY_FILE
from scoring import (
    _score_to_label,
    calculate_buy_timing_score,
    calculate_buy_timing_score_v2,
    calculate_buy_timing_score_v3,
    calculate_buy_timing_score_v4,
)
from chart_utils import generate_price_chart_svg, generate_score_trend_svg
from data_io import (
    load_watchlists, load_name_overrides, save_name_overrides,
    get_jquants_company_name, get_earnings_date, get_jquants_api_key_from_env,
)

warnings.filterwarnings('ignore')

translator = GoogleTranslator(source='auto', target='ja')


def fetch_and_save():
    """全ウォッチリストを取得してファイルに保存"""
    watchlists = load_watchlists()
    name_overrides = load_name_overrides()
    jq_api_key = get_jquants_api_key_from_env()

    all_saved = {}
    if os.path.exists(LAST_DATA_FILE):
        try:
            with open(LAST_DATA_FILE, "r", encoding="utf-8") as f:
                all_saved = json.load(f)
        except Exception:
            all_saved = {}

    history = {}
    if os.path.exists(BUY_TIMING_HISTORY_FILE):
        try:
            with open(BUY_TIMING_HISTORY_FILE, "r", encoding="utf-8") as f:
                history = json.load(f)
        except Exception:
            history = {}

    fetch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for wl_name, tickers_input in watchlists.items():
        if not tickers_input.strip():
            continue

        print(f"\n📂 ウォッチリスト: {wl_name}")
        tickers_raw = re.split(r'[,\n\s]+', tickers_input)
        tickers = [t.strip() for t in tickers_raw if t.strip()]

        data_list = []
        history_entry = {"time": fetch_time, "scores": {}}

        for raw_ticker in tickers:
            if raw_ticker.isdigit() and len(raw_ticker) == 4:
                query_ticker = raw_ticker + ".T"
                display_ticker = raw_ticker
            elif raw_ticker.endswith(".T") and raw_ticker[:-2].isdigit() and len(raw_ticker[:-2]) == 4:
                query_ticker = raw_ticker
                display_ticker = raw_ticker[:-2]
            else:
                query_ticker = raw_ticker
                display_ticker = raw_ticker

            is_japan_stock = query_ticker.endswith(".T")

            try:
                stock = yf.Ticker(query_ticker)
                info = stock.info

                # 企業名取得（キャッシュ優先 → J-Quants → yfinance → 翻訳）
                name = None

                # 1. キャッシュ（最優先 - API不要で高速）
                cached = name_overrides.get(display_ticker, None)
                if cached:
                    name = cached

                # 2. J-Quants API（キャッシュにない日本株のみ）
                if not name and is_japan_stock and jq_api_key:
                    name = get_jquants_company_name(jq_api_key, display_ticker)
                    if name:
                        name_overrides[display_ticker] = name
                        save_name_overrides(name_overrides)
                        print(f"  [CACHE] キャッシュ保存: {display_ticker} -> {name}")
                        time.sleep(12)  # Freeプラン: 1分5回制限

                # 3. yfinance shortName（日本語を含む場合のみ）
                if not name and is_japan_stock:
                    short = info.get("shortName", None)
                    if short and any('\u3000' <= c <= '\u9fff' or '\uff00' <= c <= '\uffef' for c in short):
                        name = short

                # 4. yfinance longName + Google翻訳
                if not name:
                    raw_name = info.get("longName", info.get("shortName", display_ticker))
                    try:
                        name = translator.translate(raw_name)
                    except Exception:
                        name = raw_name

                if not name:
                    name = display_ticker

                if name:
                    name = name.replace("株式会社", "").strip()

                # 株価
                raw_price = info.get("currentPrice", info.get("regularMarketPrice", None))
                if raw_price is not None:
                    c_price = float(raw_price)
                    current_price = f"{c_price:,.0f}" if c_price >= 100 else f"{c_price:,.2f}"
                else:
                    c_price = None
                    current_price = "-"

                # 配当情報
                dividend_rate = info.get("dividendRate", "無配")
                if dividend_rate not in ("無配", "-", None):
                    dr = float(dividend_rate)
                    dividend_rate = f"{dr:,.0f}" if dr >= 100 else f"{dr:,.2f}"
                dividend_yield = info.get("dividendYield", "-")
                if dividend_yield != "-" and dividend_yield is not None:
                    dividend_yield = f"{float(dividend_yield):.2f}%"
                ex_div_date = info.get("exDividendDate", None)
                if ex_div_date:
                    ex_div_date = datetime.fromtimestamp(ex_div_date).strftime('%Y-%m-%d')
                else:
                    ex_div_date = "-"
                earnings_date = get_earnings_date(stock)

                # 買い時率 (V1〜V4)
                predicted_trend = "-"
                buy_timing_rate = "-"
                buy_timing_v2 = "-"
                buy_timing_v3 = "-"
                buy_timing_v4 = "-"

                hist = stock.history(period="1y")
                if not hist.empty and raw_price is not None:
                    daily_returns = hist['Close'].pct_change().dropna()
                    mean_return = daily_returns.mean()
                    estimated_1mo_return = (1 + mean_return) ** 21 - 1
                    predicted_price = c_price * (1 + estimated_1mo_return)
                    if estimated_1mo_return >= 0:
                        predicted_trend = f"📈 {predicted_price:.2f} (+{estimated_1mo_return*100:.2f}%)"
                    else:
                        predicted_trend = f"📉 {predicted_price:.2f} ({estimated_1mo_return*100:.2f}%)"

                    rate_now, _ = calculate_buy_timing_score(hist)
                    if rate_now: buy_timing_rate = rate_now
                    rate_v2, _ = calculate_buy_timing_score_v2(hist)
                    if rate_v2: buy_timing_v2 = rate_v2
                    rate_v3, _ = calculate_buy_timing_score_v3(hist)
                    if rate_v3: buy_timing_v3 = rate_v3
                    rate_v4, _ = calculate_buy_timing_score_v4(hist)
                    if rate_v4: buy_timing_v4 = rate_v4

                # 出来高
                volume_str = "-"
                avg_volume_str = "-"
                volume_ratio_str = "-"
                try:
                    if not hist.empty:
                        last_vol = int(hist['Volume'].iloc[-1])
                        volume_str = f"{last_vol:,}"
                    avg_vol = info.get("averageVolume", None)
                    if avg_vol:
                        avg_vol = int(avg_vol)
                        avg_volume_str = f"{avg_vol:,}"
                        if last_vol and avg_vol > 0:
                            ratio = last_vol / avg_vol
                            if ratio >= 1.5:
                                volume_ratio_str = f"🔥 {ratio:.2f}x"
                            elif ratio >= 1.0:
                                volume_ratio_str = f"📈 {ratio:.2f}x"
                            else:
                                volume_ratio_str = f"📉 {ratio:.2f}x"
                except Exception:
                    pass

                if is_japan_stock:
                    links_html = (
                        f'<a href="https://shikiho.toyokeizai.net/stocks/{display_ticker}" target="_blank" rel="noopener noreferrer" title="四季報">📘</a> '
                        f'<a href="https://minkabu.jp/stock/{display_ticker}" target="_blank" rel="noopener noreferrer" title="みんかぶ">📗</a> '
                        f'<a href="https://kabutan.jp/stock/?code={display_ticker}" target="_blank" rel="noopener noreferrer" title="かぶたん">📙</a> '
                        f'<a href="https://www.buffett-code.com/company/{display_ticker}/" target="_blank" rel="noopener noreferrer" title="バフェットコード">📕</a>'
                    )
                else:
                    links_html = f'<a href="https://finance.yahoo.com/quote/{query_ticker}" target="_blank" rel="noopener noreferrer" title="Yahoo Finance">🌐</a>'

                # SVGチャート生成（共通ヘルパー使用）
                chart_svg = ""
                v1_chart_svg = ""
                v2_chart_svg = ""
                v3_chart_svg = ""
                v4_chart_svg = ""
                try:
                    if not hist.empty:
                        chart_svg = generate_price_chart_svg(hist['Close'].tail(20).tolist())
                        if len(hist) >= 40:
                            v1_chart_svg = generate_score_trend_svg(hist, calculate_buy_timing_score)
                            v2_chart_svg = generate_score_trend_svg(hist, calculate_buy_timing_score_v2)
                            v3_chart_svg = generate_score_trend_svg(hist, calculate_buy_timing_score_v3)
                            v4_chart_svg = generate_score_trend_svg(hist, calculate_buy_timing_score_v4)
                except Exception:
                    pass

                data = {
                    "銘柄コード": display_ticker,
                    "企業名": name,
                    "リンク": links_html,
                    "現在株価": current_price,
                    "チャート": chart_svg,
                    "V1トレンド": v1_chart_svg,
                    "V2トレンド": v2_chart_svg,
                    "V3トレンド": v3_chart_svg,
                    "V4トレンド": v4_chart_svg,
                    "買い時率V1": buy_timing_rate,
                    "買い時率V2": buy_timing_v2,
                    "買い時率V3": buy_timing_v3,
                    "買い時率V4": buy_timing_v4,
                    "1か月後予想株価": predicted_trend,
                    "出来高": volume_str,
                    "平均出来高": avg_volume_str,
                    "出来高比率": volume_ratio_str,
                    "配当金(年額)": dividend_rate,
                    "配当利回り": dividend_yield,
                    "配当落ち日": ex_div_date,
                    "次回決算日": earnings_date,
                    "_score_v1": 0,
                    "_score_v2": 0,
                    "_score_v3": 0,
                    "_score_v4": 0,
                }

                # スコア抽出
                try:
                    for key, field in [("_score_v1", buy_timing_rate), ("_score_v2", buy_timing_v2),
                                       ("_score_v3", buy_timing_v3), ("_score_v4", buy_timing_v4)]:
                        m = re.search(r'(\d+)%', str(field))
                        if m:
                            data[key] = int(m.group(1))
                except Exception:
                    pass

                data_list.append(data)

                # 履歴エントリ
                score_entry = {}
                for vkey, field in [("v1", buy_timing_rate), ("v2", buy_timing_v2),
                                     ("v3", buy_timing_v3), ("v4", buy_timing_v4)]:
                    m = re.search(r'(\d+)%', str(field))
                    score_entry[vkey] = int(m.group(1)) if m else None
                score_entry["price"] = current_price
                history_entry["scores"][display_ticker] = score_entry

                print(f"  ✅ {display_ticker} ({name}) - {current_price}")
                time.sleep(0.5)  # レート制限対策

            except Exception as e:
                print(f"  ❌ {display_ticker}: {e}")

        if data_list:
            all_saved[wl_name] = {"fetch_time": fetch_time, "data": data_list}

            if wl_name not in history:
                history[wl_name] = []
            history[wl_name].append(history_entry)
            if len(history[wl_name]) > 100:
                history[wl_name] = history[wl_name][-100:]

    # 保存
    with open(LAST_DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(all_saved, f, ensure_ascii=False, default=str)

    with open(BUY_TIMING_HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, default=str)

    print(f"\n🎉 取得完了！ ({fetch_time})")

if __name__ == "__main__":
    fetch_and_save()
