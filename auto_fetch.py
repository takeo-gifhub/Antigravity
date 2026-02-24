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

def _score_to_label(score):
    if score >= 80: return f"🟢 {score}%"
    elif score >= 60: return f"🔵 {score}%"
    elif score >= 40: return f"🟡 {score}%"
    elif score >= 20: return f"🟠 {score}%"
    else: return f"🔴 {score}%"

def calculate_buy_timing_score(hist):
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
        
        return _score_to_label(score), c_price
    except Exception:
        return None, None

def calculate_buy_timing_score_v2(hist):
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
        
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rs = gain / loss
        rsi = (100 - 100 / (1 + rs)).iloc[-1]
        
        sma20 = close.rolling(20).mean()
        std20 = close.rolling(20).std()
        lower_band = (sma20 - 2 * std20).iloc[-1]
        upper_band = (sma20 + 2 * std20).iloc[-1]
        
        raw = 0
        if c_price > ema20: raw += 20
        elif c_price > ema20 * 0.98: raw += 10
        if c_price <= vwap * 1.02: raw += 20
        elif c_price <= vwap * 1.05: raw += 10
        if rvol >= 1.5: raw += 20
        elif rvol >= 1.0: raw += 10
        if macd.iloc[-1] > signal.iloc[-1]: raw += 20
        elif macd.iloc[-1] > macd.iloc[-2]: raw += 10
        if 1 <= pullback <= 5: raw += 20
        elif pullback < 1: raw += 10
        if 30 <= rsi <= 50: raw += 20
        elif 25 <= rsi < 30 or 50 < rsi <= 60: raw += 10
        bb_range = upper_band - lower_band
        if bb_range > 0:
            bb_pos = (c_price - lower_band) / bb_range
            if bb_pos <= 0.2: raw += 20
            elif bb_pos <= 0.4: raw += 10
        
        score = round(raw / 140 * 100)
        return _score_to_label(score), c_price
    except Exception:
        return None, None

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

def fetch_and_save():
    """全ウォッチリストを取得してファイルに保存"""
    watchlists = load_watchlists()
    name_overrides = load_name_overrides()
    jq_api_key = os.environ.get("JQUANTS_API_KEY", "")
    
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
                    from app import get_jquants_company_name
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
                buy_timing_1w = "-"
                price_1w = "-"
                chg_1w = "-"
                buy_timing_2w = "-"
                price_2w = "-"
                chg_2w = "-"
                
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
                    
                    if len(hist) > 5:
                        hist_1w = hist.iloc[:-5]
                        rate_1w, p_1w = calculate_buy_timing_score(hist_1w)
                        if rate_1w: buy_timing_1w = rate_1w
                        if p_1w:
                            price_1w = f"{p_1w:,.0f}" if p_1w >= 100 else f"{p_1w:,.2f}"
                            pct_chg = (c_price - p_1w) / p_1w * 100
                            chg_1w = f"📈 +{pct_chg:.2f}%" if pct_chg >= 0 else f"📉 {pct_chg:.2f}%"
                    
                    if len(hist) > 10:
                        hist_2w = hist.iloc[:-10]
                        rate_2w, p_2w = calculate_buy_timing_score(hist_2w)
                        if rate_2w: buy_timing_2w = rate_2w
                        if p_2w:
                            price_2w = f"{p_2w:,.0f}" if p_2w >= 100 else f"{p_2w:,.2f}"
                            pct_chg = (c_price - p_2w) / p_2w * 100
                            chg_2w = f"📈 +{pct_chg:.2f}%" if pct_chg >= 0 else f"📉 {pct_chg:.2f}%"
                
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
                
                data = {
                    "銘柄コード": display_ticker,
                    "企業名": name,
                    "リンク": "",
                    "現在株価": current_price,
                    "チャート": "",
                    "買い時率V1": buy_timing_rate,
                    "買い時率V2": buy_timing_v2,
                    "1W前買い時率": buy_timing_1w,
                    "1W前株価": price_1w,
                    "1W変動": chg_1w,
                    "2W前買い時率": buy_timing_2w,
                    "2W前株価": price_2w,
                    "2W変動": chg_2w,
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
