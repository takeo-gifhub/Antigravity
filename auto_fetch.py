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
import requests
import time
import warnings

warnings.filterwarnings('ignore')

# ファイルパス
WATCHLIST_FILE = "watchlists.json"
LAST_DATA_FILE = "last_stock_data.json"
NAME_OVERRIDE_FILE = "name_overrides.json"
BUY_TIMING_HISTORY_FILE = "buy_timing_history.json"

translator = GoogleTranslator(source='auto', target='ja')

def load_watchlists():
    if os.path.exists(WATCHLIST_FILE):
        with open(WATCHLIST_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"デフォルト": ""}

def load_name_overrides():
    if os.path.exists(NAME_OVERRIDE_FILE):
        try:
            with open(NAME_OVERRIDE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

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
                print(f"  ⏳ J-Quants 429 レート制限 ({code}), {wait}秒待機...")
                time.sleep(wait)
                continue
            else:
                print(f"  ⚠️ J-Quants {res.status_code} ({code})")
                break
        except Exception as e:
            print(f"  ⚠️ J-Quants 例外 ({code}): {e}")
    return None

def _score_to_label(score):
    if score >= 85: return f"🔥🔥 {score}% (絶好機)"
    elif score >= 65: return f"🔥 {score}% (買い時)"
    elif score >= 40: return f"⭐ {score}% (中立)"
    else: return f"❄️ {score}% (様子見)"

def calculate_buy_timing_score(hist, raw=False):
    try:
        close = hist['Close']
        volume = hist['Volume']
        c_price = float(close.iloc[-1])
        
        ema20 = close.ewm(span=20).mean().iloc[-1]
        vwap = (close * volume).sum() / volume.sum()
        avg_vol = volume.rolling(20).mean().iloc[-1]
        last_vol = volume.iloc[-1]
        rvol = last_vol / avg_vol if avg_vol > 0 else 0
        
        ema12 = close.ewm(span=12).mean()
        ema26 = close.ewm(span=26).mean()
        macd = ema12 - ema26
        signal = macd.ewm(span=9).mean()
        
        recent = close.tail(5)
        pullback = (recent.max() - c_price) / recent.max() * 100 if recent.max() > 0 else 0
        
        score = 0
        if c_price > ema20: score += 20
        elif c_price > ema20 * 0.98: score += 10
        if c_price <= vwap * 1.02: score += 20
        elif c_price <= vwap * 1.05: score += 10
        if rvol >= 1.5: score += 20
        elif rvol >= 1.0: score += 10
        if macd.iloc[-1] > signal.iloc[-1]: score += 20
        elif macd.iloc[-1] > macd.iloc[-2]: score += 10
        if 1 <= pullback <= 5: score += 20
        elif pullback < 1: score += 10
        
        return (score, c_price) if raw else (_score_to_label(score), c_price)
    except Exception:
        return (0, None) if raw else (None, None)

def calculate_buy_timing_score_v2(hist, raw=False):
    try:
        if hist.empty:
            return ("-", None) if not raw else (0, None)
        close = hist['Close']
        volume = hist['Volume']
        c_price = float(close.iloc[-1])
        
        ema20 = close.ewm(span=20).mean().iloc[-1]
        vwap = (close * volume).sum() / volume.sum()
        avg_vol = volume.rolling(20).mean().iloc[-1]
        last_vol = volume.iloc[-1]
        rvol = last_vol / avg_vol if avg_vol > 0 else 0
        
        ema12 = close.ewm(span=12).mean()
        ema26 = close.ewm(span=26).mean()
        macd = ema12 - ema26
        signal = macd.ewm(span=9).mean()
        
        recent = close.tail(5)
        pullback = (recent.max() - c_price) / recent.max() * 100 if recent.max() > 0 else 0
        
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rs = gain / loss
        rsi = (100 - 100 / (1 + rs)).iloc[-1]
        
        sma20 = close.rolling(20).mean()
        std20 = close.rolling(20).std()
        lower_band = (sma20 - 2 * std20).iloc[-1]
        upper_band = (sma20 + 2 * std20).iloc[-1]
        
        raw_score = 0
        if c_price > ema20: raw_score += 20
        elif c_price > ema20 * 0.98: raw_score += 10
        if c_price <= vwap * 1.02: raw_score += 20
        elif c_price <= vwap * 1.05: raw_score += 10
        if rvol >= 1.5: raw_score += 20
        elif rvol >= 1.0: raw_score += 10
        if macd.iloc[-1] > signal.iloc[-1]: raw_score += 20
        elif macd.iloc[-1] > macd.iloc[-2]: raw_score += 10
        if 1 <= pullback <= 5: raw_score += 20
        elif pullback < 1: raw_score += 10
        if 30 <= rsi <= 50: raw_score += 20
        elif 25 <= rsi < 30 or 50 < rsi <= 60: raw_score += 10
        bb_range = upper_band - lower_band
        if bb_range > 0:
            bb_pos = (c_price - lower_band) / bb_range
            if bb_pos <= 0.2: raw_score += 20
            elif bb_pos <= 0.4: raw_score += 10
        
        score = round(raw_score / 140 * 100)
        return (score, c_price) if raw else (_score_to_label(score), c_price)
    except Exception:
        return ("-", None) if not raw else (0, None)

def get_earnings_date(stock):
    try:
        cal = stock.get_calendar()
        if cal is not None:
            if isinstance(cal, dict):
                ed = cal.get("Earnings Date", None)
                if ed:
                    if isinstance(ed, list) and len(ed) > 0:
                        return str(ed[0])[:10]
                    return str(ed)[:10]
            elif isinstance(cal, pd.DataFrame):
                if "Earnings Date" in cal.columns:
                    return str(cal["Earnings Date"].iloc[0])[:10]
    except Exception:
        pass
    return "-"

def get_jquants_api_key():
    env_key = os.environ.get("JQUANTS_API_KEY", "")
    if env_key:
        return env_key
    if os.path.exists("jquants_token.txt"):
        with open("jquants_token.txt", "r", encoding="utf-8") as f:
            return f.read().strip()
    return ""

def fetch_and_save():
    """全ウォッチリストを取得してファイルに保存"""
    watchlists = load_watchlists()
    name_overrides = load_name_overrides()
    jq_api_key = get_jquants_api_key()
    
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
            elif raw_ticker.endswith(".T"):
                query_ticker = raw_ticker
                display_ticker = raw_ticker[:-2]
            else:
                query_ticker = raw_ticker
                display_ticker = raw_ticker
            
            is_japan_stock = query_ticker.endswith(".T")
            
            try:
                stock = yf.Ticker(query_ticker)
                info = stock.info
                
                # 企業名取得
                name = None
                if is_japan_stock and jq_api_key:
                    name = get_jquants_company_name(jq_api_key, display_ticker)
                
                if not name:
                    name = name_overrides.get(display_ticker, None)
                
                if not name and is_japan_stock:
                    short = info.get("shortName", None)
                    if short and any('\u3000' <= c <= '\u9fff' or '\uff00' <= c <= '\uffef' for c in short):
                        name = short
                
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
                
                # 買い時率
                predicted_trend = "-"
                buy_timing_rate = "-"
                buy_timing_v2 = "-"
                
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
                        f'<a href="https://shikiho.toyokeizai.net/stocks/{display_ticker}" target="_blank" title="四季報">📘</a> '
                        f'<a href="https://minkabu.jp/stock/{display_ticker}" target="_blank" title="みんかぶ">📗</a> '
                        f'<a href="https://kabutan.jp/stock/?code={display_ticker}" target="_blank" title="かぶたん">📙</a> '
                        f'<a href="https://www.buffett-code.com/company/{display_ticker}/" target="_blank" title="バフェットコード">📕</a>'
                    )
                else:
                    links_html = f'<a href="https://finance.yahoo.com/quote/{query_ticker}" target="_blank" title="Yahoo Finance">🌐</a>'
                
                chart_svg = ""
                v1_chart_svg = ""
                v2_chart_svg = ""
                try:
                    if not hist.empty and len(hist) >= 40:
                        # --- 株価チャート ---
                        hist_20 = hist['Close'].tail(20)
                        min_val = hist_20.min()
                        max_val = hist_20.max()
                        width, height = 80, 24
                        svg_pts = []
                        if max_val > min_val:
                            for idx, val in enumerate(hist_20):
                                x = idx * (width / 19)
                                y = height - ((val - min_val) / (max_val - min_val) * height)
                                svg_pts.append(f"{x:.1f},{y:.1f}")
                            color = "#4caf50" if hist_20.iloc[-1] >= hist_20.iloc[0] else "#ff5252"
                            pts_str = " ".join(svg_pts)
                            chart_svg = f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg"><polyline points="{pts_str}" fill="none" stroke="{color}" stroke-width="1.5"/></svg>'
                        
                        # --- V1推移チャート ---
                        scores_20 = []
                        for i in range(20, 0, -1):
                            sub_hist = hist if i == 1 else hist.iloc[:-i+1]
                            score, _ = calculate_buy_timing_score(sub_hist, raw=True)
                            scores_20.append(score if score is not None and score != "-" else 0)
                        
                        min_score, max_score = 0, 100
                        svg_pts_v1 = []
                        for idx, val in enumerate(scores_20):
                            x = idx * (width / 19)
                            y = height - ((val - min_score) / (max_score - min_score) * height)
                            svg_pts_v1.append(f"{x:.1f},{y:.1f}")
                        
                        v1_color = "#ff5252" if scores_20[-1] < scores_20[0] else "#4caf50"
                        pts_str_v1 = " ".join(svg_pts_v1)
                        line_50 = f'<line x1="0" y1="{height/2}" x2="{width}" y2="{height/2}" stroke="#666666" stroke-width="1" stroke-dasharray="2,2"/>'
                        v1_chart_svg = f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">{line_50}<polyline points="{pts_str_v1}" fill="none" stroke="{v1_color}" stroke-width="1.5"/></svg>'
                        
                        # --- V2推移チャート ---
                        scores_20_v2 = []
                        for i in range(20, 0, -1):
                            sub_hist = hist if i == 1 else hist.iloc[:-i+1]
                            score, _ = calculate_buy_timing_score_v2(sub_hist, raw=True)
                            scores_20_v2.append(score if score is not None and score != "-" else 0)
                        
                        svg_pts_v2 = []
                        for idx, val in enumerate(scores_20_v2):
                            x = idx * (width / 19)
                            y = height - ((val - min_score) / (max_score - min_score) * height)
                            svg_pts_v2.append(f"{x:.1f},{y:.1f}")
                        
                        v2_color = "#ff5252" if scores_20_v2[-1] < scores_20_v2[0] else "#4caf50"
                        pts_str_v2 = " ".join(svg_pts_v2)
                        v2_chart_svg = f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">{line_50}<polyline points="{pts_str_v2}" fill="none" stroke="{v2_color}" stroke-width="1.5"/></svg>'
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
                    "買い時率V1": buy_timing_rate,
                    "買い時率V2": buy_timing_v2,
                    "1か月後予想株価": predicted_trend,
                    "出来高": volume_str,
                    "平均出来高": avg_volume_str,
                    "出来高比率": volume_ratio_str,
                    "配当金(年額)": dividend_rate,
                    "配当利回り": dividend_yield,
                    "配当落ち日": ex_div_date,
                    "次回決算日": earnings_date,
                    "_score_v1": 0,
                    "_score_v2": 0
                }
                
                # スコア抽出
                try:
                    m1 = re.search(r'(\d+)%', str(buy_timing_rate))
                    if m1: data["_score_v1"] = int(m1.group(1))
                    m2 = re.search(r'(\d+)%', str(buy_timing_v2))
                    if m2: data["_score_v2"] = int(m2.group(1))
                except Exception:
                    pass
                
                data_list.append(data)
                
                # 履歴エントリ
                v1_match = re.search(r'(\d+)%', str(buy_timing_rate))
                v2_match = re.search(r'(\d+)%', str(buy_timing_v2))
                history_entry["scores"][display_ticker] = {
                    "v1": int(v1_match.group(1)) if v1_match else None,
                    "v2": int(v2_match.group(1)) if v2_match else None,
                    "price": current_price
                }
                
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
