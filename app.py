import streamlit as st
import yfinance as yf
import pandas as pd
from datetime import datetime
import warnings
import os
import json
import re
from deep_translator import GoogleTranslator
import requests
import time
import base64
import io

# 登録済みウォッチリストの保存先ファイル
WATCHLIST_FILE = "watchlists.json"
JQUANTS_TOKEN_FILE = "jquants_token.txt"
LAST_DATA_FILE = "last_stock_data.json"
NAME_OVERRIDE_FILE = "name_overrides.json"
BUY_TIMING_HISTORY_FILE = "buy_timing_history.json"

def load_watchlists():
    if os.path.exists(WATCHLIST_FILE):
        try:
            with open(WATCHLIST_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"メイン": "7203, 7974, AAPL"}

def save_watchlists(data):
    with open(WATCHLIST_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def load_jquants_token():
    # 優先順: st.secrets → 環境変数 → セッション変数 → ローカルファイル
    # (1) Streamlit Cloud Secrets（最優先・永続的）
    try:
        key = st.secrets["JQUANTS_API_KEY"]
        if key:
            return key
    except Exception:
        pass
    # (2) 環境変数（GitHub Actions等）
    env_key = os.environ.get("JQUANTS_API_KEY", "")
    if env_key:
        return env_key
    # (3) セッション変数（サイドバーから入力 → Cloud上でもセッション中は有効）
    session_key = st.session_state.get("jquants_api_key_session", "")
    if session_key:
        return session_key
    # (4) ローカルファイル（ローカル環境用）
    if os.path.exists(JQUANTS_TOKEN_FILE):
        with open(JQUANTS_TOKEN_FILE, "r", encoding="utf-8") as f:
            return f.read().strip()
    return ""

def _get_jquants_key_source():
    """現在のAPIキーの取得元を返す（UI表示用）"""
    try:
        if st.secrets.get("JQUANTS_API_KEY"):
            return "☁️ Streamlit Secrets"
    except Exception:
        pass
    if os.environ.get("JQUANTS_API_KEY"):
        return "🔧 環境変数"
    if st.session_state.get("jquants_api_key_session"):
        return "💬 サイドバー入力（セッション中のみ有効）"
    if os.path.exists(JQUANTS_TOKEN_FILE):
        with open(JQUANTS_TOKEN_FILE, "r", encoding="utf-8") as f:
            if f.read().strip():
                return "📁 ローカルファイル"
    return None

def save_jquants_token(token):
    # セッション変数に保存（Cloud上でもセッション中は有効）
    st.session_state["jquants_api_key_session"] = token
    # ローカルファイルにも保存（ローカル環境用）
    try:
        with open(JQUANTS_TOKEN_FILE, "w", encoding="utf-8") as f:
            f.write(token)
    except Exception:
        pass  # Cloud上ではファイル書き込みに失敗しても問題ない

def get_jquants_company_name(api_key, code, retries=3):
    # J-Quants V2 APIは5桁コード（末尾0追加）が必要
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

def load_name_overrides():
    if os.path.exists(NAME_OVERRIDE_FILE):
        with open(NAME_OVERRIDE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_name_overrides(overrides):
    with open(NAME_OVERRIDE_FILE, "w", encoding="utf-8") as f:
        json.dump(overrides, f, ensure_ascii=False, indent=2)

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

def _score_to_label(score):
    """スコアを絵文字付きラベルに変換"""
    if score >= 85:
        return f"🔥🔥 {score}% (絶好機)"
    elif score >= 65:
        return f"🔥 {score}% (買い時)"
    elif score >= 40:
        return f"⭐ {score}% (中立)"
    else:
        return f"❄️ {score}% (様子見)"

def calculate_buy_timing_score(hist, raw=False):
    """V1: EMA + VWAP + RVOL + MACD + マイクロプルバック (最大100点)"""
    try:
        if hist.empty:
            return ("-", None) if not raw else (0, None)
        c_price = float(hist['Close'].iloc[-1])
        score = 0
        
        # 1. EMA (9, 20, 200) - トレンド (Max 30点)
        if len(hist) >= 200:
            ema9 = hist['Close'].ewm(span=9, adjust=False).mean().iloc[-1]
            ema20 = hist['Close'].ewm(span=20, adjust=False).mean().iloc[-1]
            ema200 = hist['Close'].ewm(span=200, adjust=False).mean().iloc[-1]
            if c_price > ema9: score += 10
            if c_price > ema20: score += 10
            if c_price > ema200: score += 10
        
        # 2. VWAP相当 (Max 20点)
        vwap_approx = (hist['High'].iloc[-1] + hist['Low'].iloc[-1] + hist['Close'].iloc[-1]) / 3
        if c_price > vwap_approx: score += 20
        
        # 3. RVOL (Max 20点)
        vol = hist['Volume'].iloc[-1]
        avg_vol = hist['Volume'].rolling(30).mean().iloc[-2] if len(hist) > 30 else 0
        if avg_vol > 0:
            rvol = vol / avg_vol
            if rvol >= 5.0: score += 20
            elif rvol >= 2.0: score += 10
            elif rvol >= 1.0: score += 5
        
        # 4. MACD (Max 15点)
        if len(hist) > 30:
            macd_line = hist['Close'].ewm(span=12, adjust=False).mean() - hist['Close'].ewm(span=26, adjust=False).mean()
            if macd_line.iloc[-1] > 0: score += 15
        
        # 5. マイクロ・プルバック (Max 15点)
        if len(hist) >= 2:
            prev_high = hist['High'].iloc[-2]
            if c_price > prev_high: score += 15
            
        return (score, c_price) if raw else (_score_to_label(score), c_price)
    except Exception:
        return ("-", None) if not raw else (0, None)

def calculate_buy_timing_score_v2(hist, raw=False):
    """V2: V1の5指標 + RSI + ボリンジャーバンド (最大100点に正規化)"""
    try:
        if hist.empty:
            return ("-", None) if not raw else (0, None)
        c_price = float(hist['Close'].iloc[-1])
        score = 0
        max_score = 0
        
        # 1. EMA (9, 20, 200) - トレンド (Max 30)
        max_score += 30
        if len(hist) >= 200:
            ema9 = hist['Close'].ewm(span=9, adjust=False).mean().iloc[-1]
            ema20 = hist['Close'].ewm(span=20, adjust=False).mean().iloc[-1]
            ema200 = hist['Close'].ewm(span=200, adjust=False).mean().iloc[-1]
            if c_price > ema9: score += 10
            if c_price > ema20: score += 10
            if c_price > ema200: score += 10
        
        # 2. VWAP相当 (Max 15)
        max_score += 15
        vwap_approx = (hist['High'].iloc[-1] + hist['Low'].iloc[-1] + hist['Close'].iloc[-1]) / 3
        if c_price > vwap_approx: score += 15
        
        # 3. RVOL (Max 15)
        max_score += 15
        vol = hist['Volume'].iloc[-1]
        avg_vol = hist['Volume'].rolling(30).mean().iloc[-2] if len(hist) > 30 else 0
        if avg_vol > 0:
            rvol = vol / avg_vol
            if rvol >= 5.0: score += 15
            elif rvol >= 2.0: score += 10
            elif rvol >= 1.0: score += 5
        
        # 4. MACD (Max 10)
        max_score += 10
        if len(hist) > 30:
            macd_line = hist['Close'].ewm(span=12, adjust=False).mean() - hist['Close'].ewm(span=26, adjust=False).mean()
            if macd_line.iloc[-1] > 0: score += 10
        
        # 5. マイクロ・プルバック (Max 10)
        max_score += 10
        if len(hist) >= 2:
            prev_high = hist['High'].iloc[-2]
            if c_price > prev_high: score += 10
        
        # 6. RSI (相対力指数) - 売られすぎ/買われすぎ判定 (Max 10)
        max_score += 10
        if len(hist) >= 15:
            delta = hist['Close'].diff()
            gain = delta.where(delta > 0, 0).rolling(14).mean().iloc[-1]
            loss = (-delta.where(delta < 0, 0)).rolling(14).mean().iloc[-1]
            if loss != 0:
                rs = gain / loss
                rsi = 100 - (100 / (1 + rs))
            else:
                rsi = 100
            # RSI 30以下 = 売られすぎ（買いチャンス）, 30-70 = 中立, 70以上 = 過熱（減点）
            if rsi <= 30: score += 10
            elif rsi <= 50: score += 7
            elif rsi <= 70: score += 3
            # 70以上は加点なし（過熱）
        
        # 7. ボリンジャーバンド - 割安度判定 (Max 10)
        max_score += 10
        if len(hist) >= 20:
            sma20 = hist['Close'].rolling(20).mean().iloc[-1]
            std20 = hist['Close'].rolling(20).std().iloc[-1]
            lower_band = sma20 - 2 * std20
            upper_band = sma20 + 2 * std20
            # 下部バンド付近 = 割安（買いチャンス）
            if c_price <= lower_band: score += 10
            elif c_price <= sma20: score += 5
            # 上部バンド付近 = 加点なし
        
        # スコアを100点満点に正規化
        normalized = int(score / max_score * 100) if max_score > 0 else 0
        return (normalized, c_price) if raw else (_score_to_label(normalized), c_price)
    except Exception:
        return ("-", None) if not raw else (0, None)

warnings.filterwarnings('ignore')

st.set_page_config(page_title="株価・企業情報収集ツール", layout="wide")

st.title("📈 株価・企業情報 収集ツール")


watchlists = load_watchlists()
watchlist_names = list(watchlists.keys())

# --- サイドバー: ウォッチリストの管理（設定系をまとめる） ---
with st.sidebar.expander("📝 ウォッチリスト管理", expanded=False):
    st.markdown("#### 銘柄コードの編集")
    selected_for_edit = st.selectbox("編集するリスト", watchlist_names, key="edit_wl")
    edit_tickers = watchlists.get(selected_for_edit, "")
    edited_tickers = st.text_area("銘柄コード（カンマ、改行、スペース区切り）", edit_tickers, height=100, key=f"edit_tickers_{selected_for_edit}")
    if st.button("💾 保存"):
        watchlists[selected_for_edit] = edited_tickers
        save_watchlists(watchlists)
        st.success(f"「{selected_for_edit}」を更新しました！")
        st.rerun()
    
    st.markdown("---")
    st.markdown("#### ➕ 新規リスト作成")
    new_wl_name = st.text_input("新しいウォッチリスト名")
    if st.button("作成"):
        if new_wl_name:
            if new_wl_name not in watchlists:
                watchlists[new_wl_name] = ""
                save_watchlists(watchlists)
                st.success(f"「{new_wl_name}」を作成しました！")
                st.rerun()
            else:
                st.error("その名前はすでに存在します")
    
    st.markdown("---")
    st.markdown("#### 🗑️ リスト削除")
    if len(watchlist_names) > 1:
        del_wl = st.selectbox("削除するリスト", watchlist_names, key="del_wl")
        if st.button("削除", type="secondary"):
            if del_wl in watchlists:
                del watchlists[del_wl]
                save_watchlists(watchlists)
                st.success(f"「{del_wl}」を削除しました")
                st.rerun()
    else:
        st.info("リストが1つしかないため削除できません")

with st.sidebar.expander("✏️ 企業名の修正", expanded=False):
    st.markdown("自動取得した企業名が間違っている場合に修正できます")
    fix_code = st.text_input("銘柄コード", key="fix_code", placeholder="例: 9696")
    fix_name = st.text_input("正しい企業名", key="fix_name", placeholder="例: ジェコス")
    if st.button("💾 企業名を保存"):
        if fix_code and fix_name:
            overrides = load_name_overrides()
            overrides[fix_code.strip()] = fix_name.strip()
            save_name_overrides(overrides)
            st.success(f"「{fix_code.strip()}」→「{fix_name.strip()}」を保存しました")
    # 登録済みの修正一覧と削除
    existing_overrides = load_name_overrides()
    if existing_overrides:
        st.markdown("**登録済み:**")
        for k, v in list(existing_overrides.items()):
            col_label, col_del = st.columns([3, 1])
            with col_label:
                st.caption(f"{k} → {v}")
            with col_del:
                if st.button("🗑", key=f"del_name_{k}"):
                    overrides = load_name_overrides()
                    overrides.pop(k, None)
                    save_name_overrides(overrides)
                    st.rerun()

# --- メインエリア: ウォッチリスト選択とアクションボタン ---
wl_col, btn1_col, btn2_col = st.columns([2, 1, 1])
with wl_col:
    selected_watchlist = st.selectbox("📂 ウォッチリスト", watchlist_names, label_visibility="collapsed")
with btn1_col:
    do_fetch = st.button("🔍 取得", use_container_width=True)
with btn2_col:
    do_refetch = st.button("🔄 再取得", use_container_width=True, disabled=("stock_df" not in st.session_state))

tickers_input = watchlists.get(selected_watchlist, "")

def fetch_stock_data(tickers_input):
    """銘柄データを取得してDataFrameを返す共通処理"""
    tickers_raw = re.split(r'[,\n\s]+', tickers_input)
    tickers = [t.strip() for t in tickers_raw if t.strip()]
    
    data_list = []
    
    # J-Quantsの準備 (V2 API) ─ キーの有効性はセッション内でキャッシュし、毎回のAPI呼び出しを防ぐ
    saved_jq_api_key = load_jquants_token()
    if saved_jq_api_key:
        if "jq_key_valid" not in st.session_state or st.session_state.get("jq_key_cache") != saved_jq_api_key:
            try:
                res_test = requests.get("https://api.jquants.com/v2/equities/master?code=72030", headers={"x-api-key": saved_jq_api_key}, timeout=10)
                if res_test.status_code in (401, 403):
                    st.session_state["jq_key_valid"] = False
                else:
                    # 200, 429 等はキー自体は有効（429はレート制限で一時的）
                    st.session_state["jq_key_valid"] = True
            except Exception:
                st.session_state["jq_key_valid"] = False
            st.session_state["jq_key_cache"] = saved_jq_api_key
        
        if not st.session_state.get("jq_key_valid", False):
            st.warning("⚠️ 入力されたJ-QuantsのAPI Keyが無効または期限切れです。最新のものを再登録するか、再度発行してください。")
            saved_jq_api_key = None
    
    # GoogleTranslator をループ外で1回だけ生成して使い回す
    translator = GoogleTranslator(source='auto', target='ja')
    
    progress_bar = st.progress(0, text="データを取得中...")
    
    for i, raw_ticker in enumerate(tickers):
        progress_bar.progress((i) / len(tickers), text=f"データを取得中... ({i+1}/{len(tickers)})")
        
        # 日本株の判定（4桁数字のみなら .T を付与）
        if raw_ticker.isdigit() and len(raw_ticker) == 4:
            query_ticker = raw_ticker + ".T"
            display_ticker = raw_ticker
            is_japan_stock = True
        elif raw_ticker.endswith(".T") and raw_ticker[:-2].isdigit() and len(raw_ticker[:-2]) == 4:
            query_ticker = raw_ticker
            display_ticker = raw_ticker[:-2]
            is_japan_stock = True
        else:
            query_ticker = raw_ticker
            display_ticker = raw_ticker
            is_japan_stock = False

        try:
            stock = yf.Ticker(query_ticker)
            info = stock.info
            
            # 企業名の取得（優先順: 手動修正辞書/キャッシュ → J-Quants → shortName → longName+翻訳）
            name = None
            name_from_cache = False
            
            # 1. 手動修正辞書 & 自動キャッシュ（最優先 - API不要で高速）
            name_overrides = load_name_overrides()
            cached = name_overrides.get(display_ticker, None)
            if cached:
                name = cached
                name_from_cache = True
            
            # 2. J-Quants API（キャッシュにない日本株のみ呼ぶ）
            if not name and is_japan_stock and saved_jq_api_key:
                name = get_jquants_company_name(saved_jq_api_key, display_ticker)
                if name:
                    # 取得成功 → 自動キャッシュに保存（次回からAPI不要）
                    name_overrides[display_ticker] = name
                    save_name_overrides(name_overrides)
                    print(f"  💾 キャッシュ保存: {display_ticker} → {name}")
                    time.sleep(12)  # Freeプラン: 1分5回制限（12秒間隔）
            
            # 3. yfinance shortName（日本語を含む場合のみ採用）
            if not name and is_japan_stock:
                short = info.get("shortName", None)
                if short and any('\u3000' <= c <= '\u9fff' or '\uff00' <= c <= '\uffef' for c in short):
                    name = short
            
            # 4. yfinance longName + Google翻訳（フォールバック）
            if not name:
                raw_name = info.get("longName", info.get("shortName", display_ticker))
                try:
                    name = translator.translate(raw_name)
                except Exception:
                    name = raw_name
            
            if not name:
                name = display_ticker
            
            # 「株式会社」を除去してすっきり表示
            if name:
                name = name.replace("株式会社", "").strip()
            
            # 株価の取得（見やすく整形）
            raw_price = info.get("currentPrice", info.get("regularMarketPrice", None))
            if raw_price is not None:
                cp = float(raw_price)
                current_price = f"{cp:,.0f}" if cp >= 100 else f"{cp:,.2f}"
            else:
                raw_price = None
                current_price = "-"
            
            # 配当金と配当利回り（見やすく整形）
            dividend_rate = info.get("dividendRate", "無配")
            if dividend_rate not in ("無配", "-", None):
                dr = float(dividend_rate)
                dividend_rate = f"{dr:,.0f}" if dr >= 100 else f"{dr:,.2f}"
            
            dividend_yield = info.get("dividendYield", "-")
            if dividend_yield != "-" and dividend_yield is not None:
                dividend_yield = f"{float(dividend_yield):.2f}%"
            
            # 決算関連
            ex_div_date = info.get("exDividendDate", None)
            if ex_div_date:
                ex_div_date = datetime.fromtimestamp(ex_div_date).strftime('%Y-%m-%d')
            else:
                ex_div_date = "-"

            earnings_date = get_earnings_date(stock)

            # 1か月後の予想株価の算出 と 買い時率（ロス・キャメロン流）の算出
            predicted_trend = "-"
            buy_timing_rate = "-"
            buy_timing_v2 = "-"
            
            try:
                hist = stock.history(period="1y")
                if not hist.empty and raw_price is not None:
                    c_price = float(raw_price)
                    
                    # -- 1か月予想株価の計算 --
                    daily_returns = hist['Close'].pct_change().dropna()
                    mean_return = daily_returns.mean()
                    estimated_1mo_return = (1 + mean_return) ** 21 - 1
                    predicted_price = c_price * (1 + estimated_1mo_return)
                    
                    if estimated_1mo_return >= 0:
                        predicted_trend = f"📈 {predicted_price:.2f} (+{estimated_1mo_return*100:.2f}%)"
                    else:
                        predicted_trend = f"📉 {predicted_price:.2f} ({estimated_1mo_return*100:.2f}%)"
                    
                    # -- 買い時率の計算 --
                    rate_now, _ = calculate_buy_timing_score(hist)
                    if rate_now: buy_timing_rate = rate_now
                    
                    rate_v2, _ = calculate_buy_timing_score_v2(hist)
                    if rate_v2: buy_timing_v2 = rate_v2
                    
            except Exception:
                pass

            # 出来高の取得
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

            data = {
                "銘柄コード": display_ticker,
                "企業名": name,
                "リンク": links_html,
                "現在株価": current_price,
                "チャート": "",  # 後でSVGをセット
                "V1トレンド": "",  
                "V2トレンド": "",
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
            # ミニチャート用SVGを生成
            try:
                if not hist.empty:
                    recent = hist['Close'].tail(30).tolist()
                    if len(recent) >= 2:
                        mn, mx = min(recent), max(recent)
                        rng = mx - mn if mx != mn else 1
                        w, h = 80, 24
                        points = []
                        for j, v in enumerate(recent):
                            x = j / (len(recent) - 1) * w
                            y = h - (v - mn) / rng * h
                            points.append(f"{x:.1f},{y:.1f}")
                        color = "#4caf50" if recent[-1] >= recent[0] else "#ff5252"
                        svg = f'<svg width="{w}" height="{h}" xmlns="http://www.w3.org/2000/svg"><polyline points="{" ".join(points)}" fill="none" stroke="{color}" stroke-width="1.5"/></svg>'
                        data["チャート"] = svg
                    
                    # V1推移チャート用SVGを生成
                    if len(hist) >= 40:
                        scores_20 = []
                        for i in range(20, 0, -1):
                            sub_hist = hist if i == 1 else hist.iloc[:-i+1]
                            score, _ = calculate_buy_timing_score(sub_hist, raw=True)
                            scores_20.append(score if score is not None and score != "-" else 0)
                        
                        min_score, max_score = 0, 100
                        svg_pts_v1 = []
                        w, h = 80, 24
                        for idx, val in enumerate(scores_20):
                            x = idx * (w / 19)
                            y = h - ((val - min_score) / (max_score - min_score) * h)
                            svg_pts_v1.append(f"{x:.1f},{y:.1f}")
                        
                        v1_color = "#ff5252" if scores_20[-1] < scores_20[0] else "#4caf50"
                        pts_str_v1 = " ".join(svg_pts_v1)
                        line_50 = f'<line x1="0" y1="{h/2}" x2="{w}" y2="{h/2}" stroke="#666666" stroke-width="1" stroke-dasharray="2,2"/>'
                        v1_svg = f'<svg width="{w}" height="{h}" xmlns="http://www.w3.org/2000/svg">{line_50}<polyline points="{pts_str_v1}" fill="none" stroke="{v1_color}" stroke-width="1.5"/></svg>'
                        data["V1トレンド"] = v1_svg
                        
                    # V2推移チャート用SVGを生成
                    if len(hist) >= 40:
                        scores_20_v2 = []
                        for i in range(20, 0, -1):
                            sub_hist = hist if i == 1 else hist.iloc[:-i+1]
                            score, _ = calculate_buy_timing_score_v2(sub_hist, raw=True)
                            scores_20_v2.append(score if score is not None and score != "-" else 0)
                        
                        min_score, max_score = 0, 100
                        svg_pts_v2 = []
                        w, h = 80, 24
                        for idx, val in enumerate(scores_20_v2):
                            x = idx * (w / 19)
                            y = h - ((val - min_score) / (max_score - min_score) * h)
                            svg_pts_v2.append(f"{x:.1f},{y:.1f}")
                        
                        v2_color = "#ff5252" if scores_20_v2[-1] < scores_20_v2[0] else "#4caf50"
                        pts_str_v2 = " ".join(svg_pts_v2)
                        v2_svg = f'<svg width="{w}" height="{h}" xmlns="http://www.w3.org/2000/svg">{line_50}<polyline points="{pts_str_v2}" fill="none" stroke="{v2_color}" stroke-width="1.5"/></svg>'
                        data["V2トレンド"] = v2_svg
                        
            except Exception:
                pass
            # ソート/フィルタ用のスコア数値を抽出
            try:
                import re as _re
                m1 = _re.search(r'(\d+)%', str(buy_timing_rate))
                if m1:
                    data["_score_v1"] = int(m1.group(1))
                m2 = _re.search(r'(\d+)%', str(buy_timing_v2))
                if m2:
                    data["_score_v2"] = int(m2.group(1))
            except Exception:
                pass
            data_list.append(data)
        except Exception as e:
            st.error(f"「{display_ticker}」 のデータ取得に失敗しました。銘柄が正しいか確認してください。")
    
    progress_bar.progress(1.0, text="取得完了！")
    
    if data_list:
        return pd.DataFrame(data_list)
    return None

def smart_refetch(existing_df, tickers_input, same_day=True):
    """スマート再取得：
    same_day=True → 株価・買い時率・変動率のみ更新（高速）
    same_day=False → 企業名・リンク以外を全更新
    """
    tickers_raw = re.split(r'[,\n\s]+', tickers_input)
    tickers = [t.strip() for t in tickers_raw if t.strip()]
    
    # 既存データを銘柄コードで辞書化
    existing = {}
    if existing_df is not None:
        for _, row in existing_df.iterrows():
            existing[str(row.get("銘柄コード", ""))] = row.to_dict()
    
    data_list = []
    progress_bar = st.progress(0, text="スマート更新中...")
    
    for i, raw_ticker in enumerate(tickers):
        progress_bar.progress(i / len(tickers), text=f"スマート更新中... ({i+1}/{len(tickers)})")
        
        if raw_ticker.isdigit() and len(raw_ticker) == 4:
            query_ticker = raw_ticker + ".T"
            display_ticker = raw_ticker
        elif raw_ticker.endswith(".T") and raw_ticker[:-2].isdigit() and len(raw_ticker[:-2]) == 4:
            query_ticker = raw_ticker
            display_ticker = raw_ticker[:-2]
        else:
            query_ticker = raw_ticker
            display_ticker = raw_ticker
        
        prev = existing.get(display_ticker, {})
        
        try:
            stock = yf.Ticker(query_ticker)
            info = stock.info
            
            # 株価取得
            raw_price = info.get("currentPrice", info.get("regularMarketPrice", None))
            if raw_price is not None:
                c_price = float(raw_price)
                current_price = f"{c_price:,.0f}" if c_price >= 100 else f"{c_price:,.2f}"
            else:
                c_price = None
                current_price = "-"
            
            # チャート・変動率の再計算
            predicted_trend = "-"
            buy_timing_rate = "-"
            buy_timing_v2 = "-"
            buy_timing_1w = "-"
            price_1w = "-"
            chg_1w = "-"
            buy_timing_2w = "-"
            price_2w = "-"
            chg_2w = "-"
            chart_svg = prev.get("チャート", "")
            
            if same_day:
                # 当日再取得: 買い時率V1/V2は前回データを再利用
                buy_timing_rate = prev.get("買い時率V1", "-")
                buy_timing_v2 = prev.get("買い時率V2", "-")
                buy_timing_1w = prev.get("1W前買い時率", "-")
                buy_timing_2w = prev.get("2W前買い時率", "-")
            
            try:
                hist = stock.history(period="1y")
                if not hist.empty and c_price is not None:
                    # 予想株価
                    daily_returns = hist['Close'].pct_change().dropna()
                    mean_return = daily_returns.mean()
                    estimated_1mo_return = (1 + mean_return) ** 21 - 1
                    predicted_price = c_price * (1 + estimated_1mo_return)
                    if estimated_1mo_return >= 0:
                        predicted_trend = f"📈 {predicted_price:.2f} (+{estimated_1mo_return*100:.2f}%)"
                    else:
                        predicted_trend = f"📉 {predicted_price:.2f} ({estimated_1mo_return*100:.2f}%)"
                    
                    if not same_day:
                        # 別日再取得: 買い時率を再計算
                        rate_now, _ = calculate_buy_timing_score(hist)
                        if rate_now: buy_timing_rate = rate_now
                        rate_v2, _ = calculate_buy_timing_score_v2(hist)
                        if rate_v2: buy_timing_v2 = rate_v2
                        
                        # 1W前
                        if len(hist) > 5:
                            hist_1w = hist.iloc[:-5]
                            rate_1w, p_1w = calculate_buy_timing_score(hist_1w)
                            if rate_1w: buy_timing_1w = rate_1w
                    
                    # 1W前株価・変動率（当日/別日共通）
                    if len(hist) > 5:
                        hist_1w_data = hist.iloc[:-5]
                        _, p_1w = calculate_buy_timing_score(hist_1w_data)
                        if p_1w:
                            price_1w = f"{p_1w:,.0f}" if p_1w >= 100 else f"{p_1w:,.2f}"
                            pct_chg = (c_price - p_1w) / p_1w * 100
                            chg_1w = f"📈 +{pct_chg:.2f}%" if pct_chg >= 0 else f"📉 {pct_chg:.2f}%"
                    
                    # 2W前株価・変動率（当日/別日共通）
                    if len(hist) > 10:
                        hist_2w_data = hist.iloc[:-10]
                        if not same_day:
                            rate_2w, p_2w = calculate_buy_timing_score(hist_2w_data)
                            if rate_2w: buy_timing_2w = rate_2w
                        else:
                            _, p_2w = calculate_buy_timing_score(hist_2w_data)
                        if p_2w:
                            price_2w = f"{p_2w:,.0f}" if p_2w >= 100 else f"{p_2w:,.2f}"
                            pct_chg = (c_price - p_2w) / p_2w * 100
                            chg_2w = f"📈 +{pct_chg:.2f}%" if pct_chg >= 0 else f"📉 {pct_chg:.2f}%"
                    
                    # チャート更新
                    recent = hist['Close'].tail(30).tolist()
                    if len(recent) >= 2:
                        mn, mx = min(recent), max(recent)
                        rng = mx - mn if mx != mn else 1
                        w, h = 80, 24
                        points = []
                        for j, v in enumerate(recent):
                            x = j / (len(recent) - 1) * w
                            y = h - (v - mn) / rng * h
                            points.append(f"{x:.1f},{y:.1f}")
                        color = "#2e7d32" if recent[-1] >= recent[0] else "#c62828"
                        chart_svg = f'<svg width="{w}" height="{h}" xmlns="http://www.w3.org/2000/svg"><polyline points="{" ".join(points)}" fill="none" stroke="{color}" stroke-width="1.5"/></svg>'
            except Exception:
                pass
            
            # 出来高の取得
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
            
            if same_day:
                # 当日再取得: 企業名・リンク・配当・決算は前回データを再利用
                data = {
                    "銘柄コード": display_ticker,
                    "企業名": prev.get("企業名", display_ticker),
                    "リンク": prev.get("リンク", ""),
                    "現在株価": current_price,
                    "チャート": chart_svg,
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
                    "配当金(年額)": prev.get("配当金(年額)", "-"),
                    "配当利回り": prev.get("配当利回り", "-"),
                    "配当落ち日": prev.get("配当落ち日", "-"),
                    "次回決算日": prev.get("次回決算日", "-"),
                    "_score_v1": 0,
                    "_score_v2": 0
                }
            else:
                # 別日再取得: 企業名・リンク・配当・決算は前回データを再利用
                data = {
                    "銘柄コード": display_ticker,
                    "企業名": prev.get("企業名", display_ticker),
                    "リンク": prev.get("リンク", ""),
                    "現在株価": current_price,
                    "チャート": chart_svg,
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
                    "配当金(年額)": prev.get("配当金(年額)", "-"),
                    "配当利回り": prev.get("配当利回り", "-"),
                    "配当落ち日": prev.get("配当落ち日", "-"),
                    "次回決算日": prev.get("次回決算日", "-"),
                    "_score_v1": 0,
                    "_score_v2": 0
                }
            
            # スコア数値抽出
            try:
                m1 = re.search(r'(\d+)%', str(buy_timing_rate))
                if m1: data["_score_v1"] = int(m1.group(1))
                m2 = re.search(r'(\d+)%', str(buy_timing_v2))
                if m2: data["_score_v2"] = int(m2.group(1))
            except Exception:
                pass
            data_list.append(data)
        except Exception:
            st.error(f"「{display_ticker}」 の更新に失敗しました。")
    
    progress_bar.progress(1.0, text="更新完了！")
    if data_list:
        return pd.DataFrame(data_list)
    return None

def save_to_file(df, fetch_time, wl_name):
    """データをファイルに永続化"""
    try:
        all_saved = {}
        if os.path.exists(LAST_DATA_FILE):
            with open(LAST_DATA_FILE, "r", encoding="utf-8") as f:
                all_saved = json.load(f)
        all_saved[wl_name] = {"fetch_time": fetch_time, "data": df.to_dict(orient="records")}
        with open(LAST_DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(all_saved, f, ensure_ascii=False, default=str)
    except Exception:
        pass

def save_buy_timing_history(df, fetch_time, wl_name):
    """買い時率の履歴を蓄積保存"""
    try:
        history = {}
        if os.path.exists(BUY_TIMING_HISTORY_FILE):
            with open(BUY_TIMING_HISTORY_FILE, "r", encoding="utf-8") as f:
                history = json.load(f)
        
        if wl_name not in history:
            history[wl_name] = []
        
        # 各銘柄のスコアを記録
        entry = {"time": fetch_time, "scores": {}}
        for _, row in df.iterrows():
            code = str(row.get("銘柄コード", ""))
            v1_str = str(row.get("買い時率V1", "-"))
            v2_str = str(row.get("買い時率V2", "-"))
            price_str = str(row.get("現在株価", "-"))
            # スコア数値を抽出
            v1_match = re.search(r'(\d+)%', v1_str)
            v2_match = re.search(r'(\d+)%', v2_str)
            entry["scores"][code] = {
                "v1": int(v1_match.group(1)) if v1_match else None,
                "v2": int(v2_match.group(1)) if v2_match else None,
                "price": price_str
            }
        
        history[wl_name].append(entry)
        
        # 最大100件まで保持
        if len(history[wl_name]) > 100:
            history[wl_name] = history[wl_name][-100:]
        
        with open(BUY_TIMING_HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, default=str)
    except Exception:
        pass

# --- ボタン処理 ---
if do_fetch:
    with st.spinner("全データを取得中..."):
        df = fetch_stock_data(tickers_input)
        if df is not None:
            fetch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            st.session_state["stock_df"] = df
            st.session_state["stock_fetch_time"] = fetch_time
            st.session_state["stock_wl_name"] = selected_watchlist
            save_to_file(df, fetch_time, selected_watchlist)
            save_buy_timing_history(df, fetch_time, selected_watchlist)

elif do_refetch and "stock_df" in st.session_state:
    prev_time = st.session_state.get("stock_fetch_time", "")
    today_str = datetime.now().strftime("%Y-%m-%d")
    same_day = prev_time.startswith(today_str)
    
    mode_label = "株価・指標のみ更新中..." if same_day else "データ更新中（企業名・リンク以外）..."
    with st.spinner(mode_label):
        df = smart_refetch(st.session_state["stock_df"], tickers_input, same_day=same_day)
        if df is not None:
            fetch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            st.session_state["stock_df"] = df
            st.session_state["stock_fetch_time"] = fetch_time
            st.session_state["stock_wl_name"] = selected_watchlist
            save_to_file(df, fetch_time, selected_watchlist)
            save_buy_timing_history(df, fetch_time, selected_watchlist)

# ウォッチリスト切替時 or 起動時にデータをファイルから復元
current_wl = st.session_state.get("stock_wl_name", None)
if current_wl != selected_watchlist or "stock_df" not in st.session_state:
    if os.path.exists(LAST_DATA_FILE):
        try:
            with open(LAST_DATA_FILE, "r", encoding="utf-8") as f:
                all_saved = json.load(f)
            if selected_watchlist in all_saved:
                wl_data = all_saved[selected_watchlist]
                st.session_state["stock_df"] = pd.DataFrame(wl_data["data"])
                st.session_state["stock_fetch_time"] = wl_data.get("fetch_time", "")
                st.session_state["stock_wl_name"] = selected_watchlist
            elif current_wl != selected_watchlist:
                # 切替先のウォッチリストにはまだデータがない場合はクリア
                st.session_state.pop("stock_df", None)
                st.session_state.pop("stock_fetch_time", None)
                st.session_state["stock_wl_name"] = selected_watchlist
        except Exception:
            pass

# session_stateに結果があれば常に表示
if "stock_df" in st.session_state:
    df = st.session_state["stock_df"]
    fetch_time = st.session_state.get("stock_fetch_time", "")
    
    st.success(f"データ取得完了！（最終取得: {fetch_time}）")
    
    # --- コントロールバー: ソート、フィルタ、表示切替 ---
    ctrl_c1, ctrl_c2, ctrl_c3 = st.columns([1, 1, 1])
    with ctrl_c1:
        sort_option = st.selectbox("↕️ ソート", ["なし", "買い時率V2 ⬇", "買い時率V2 ⬆", "買い時率V1 ⬇", "買い時率V1 ⬆", "1W変動 ⬇", "1W変動 ⬆"], label_visibility="collapsed")
    with ctrl_c2:
        filter_option = st.selectbox("🎯 フィルタ", ["すべて", "🔥 V2 買い時 (≥65%)", "🔥🔥 V2 絶好機 (≥85%)", "❄️ V2 様子見 (<40%)", "🔥 V1 買い時 (≥65%)", "🔥🔥 V1 絶好機 (≥85%)", "❄️ V1 様子見 (<40%)"], label_visibility="collapsed")
    with ctrl_c3:
        view_mode = st.selectbox("👁 表示", ["詳細表示", "簡易表示"], label_visibility="collapsed")
    
    # --- フィルタ適用 ---
    display_df = df.copy()
    if "V2" in filter_option and "_score_v2" in display_df.columns:
        if "≥65%" in filter_option:
            display_df = display_df[display_df["_score_v2"] >= 65]
        elif "≥85%" in filter_option:
            display_df = display_df[display_df["_score_v2"] >= 85]
        elif "<40%" in filter_option:
            display_df = display_df[display_df["_score_v2"] < 40]
    elif "V1" in filter_option and "_score_v1" in display_df.columns:
        if "≥65%" in filter_option:
            display_df = display_df[display_df["_score_v1"] >= 65]
        elif "≥85%" in filter_option:
            display_df = display_df[display_df["_score_v1"] >= 85]
        elif "<40%" in filter_option:
            display_df = display_df[display_df["_score_v1"] < 40]
    
    # --- ソート適用 ---
    if sort_option != "なし":
        if "買い時率V2" in sort_option and "_score_v2" in display_df.columns:
            asc = "⬆" in sort_option
            display_df = display_df.sort_values("_score_v2", ascending=asc)
        elif "買い時率V1" in sort_option and "_score_v1" in display_df.columns:
            asc = "⬆" in sort_option
            display_df = display_df.sort_values("_score_v1", ascending=asc)
    
    # --- 列表示切替 ---
    simple_cols = ["銘柄コード", "企業名", "リンク", "現在株価", "チャート", "V1トレンド", "V2トレンド", "買い時率V1", "買い時率V2", "配当利回り"]
    if view_mode == "簡易表示":
        cols_to_show = [c for c in simple_cols if c in display_df.columns]
    else:
        cols_to_show = [c for c in display_df.columns if not c.startswith("_")]
    
    show_df = display_df[cols_to_show]
    
    if show_df.empty:
        st.info("該当する銘柄がありません")
    else:
        # --- 色分けスタイルを適用 ---
        def style_table(styler):
            current_cols = ["現在株価", "買い時率V1", "買い時率V2"]
            for col in current_cols:
                if col in styler.columns:
                    styler = styler.set_properties(subset=[col], **{"background-color": "#1a3a22"})
            return styler
        
        styled_df = show_df.style.pipe(style_table)
        table_html = styled_df.to_html(escape=False)
        
        # Pandasが生成するインラインスタイルのcolor指定を白に強制変換
        # （CSSの!importantだけではStreamlit上で効かない場合があるため直接加工）
        table_html = table_html.replace('color: #e0e0e0', 'color: #ffffff')
        table_html = table_html.replace('color: #000000', 'color: #ffffff')
        # td要素にstyle属性がない場合にも対応するため、全tdにcolor追加
        table_html = table_html.replace('<td ', '<td style="color:#ffffff;" ')
        table_html = table_html.replace('<th ', '<th style="color:#ffffff;" ')
        # 変動列の色は保持（上で追加したstyleの後にPandasのstyleが来るのでOK）
        
        # テーブル表示用CSS
        st.markdown("""
        <style>
        .stock-table-wrapper table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.85em;
        }
        .stock-table-wrapper th {
            background-color: #1e2a3a;
            padding: 8px 10px;
            text-align: center;
            white-space: nowrap;
            position: sticky;
            top: 0;
            border-bottom: 2px solid #4a6fa5;
            font-weight: 700;
            font-size: 0.9em;
        }
        .stock-table-wrapper td {
            padding: 6px 10px;
            text-align: center;
            white-space: nowrap;
            border-bottom: 1px solid #2a2a3a;
            background-color: #131722;
        }
        /* 買い時率 現在（緑系）、1W前（青系）、2W前（紫系）の背景色はPandas Stylerで指定しています */
        .stock-table-wrapper tr:hover td {
            filter: brightness(1.25);
        }
        .stock-table-wrapper a {
            text-decoration: none;
            font-size: 1.2em;
            margin: 0 2px;
        }
        .stock-table-wrapper a:hover {
            transform: scale(1.3);
            display: inline-block;
        }
        </style>
        """, unsafe_allow_html=True)
        
        st.markdown(f'<div class="stock-table-wrapper" style="overflow-x:auto;">{table_html}</div>', unsafe_allow_html=True)
        
        # 凡例
        st.markdown("""
        <div style="display:flex; gap:16px; font-size:0.85em; margin-top:4px; margin-bottom:8px; flex-wrap:wrap;">
            <span>🟢 <b>緑</b>=現在</span>
            <span>🟦 <b>青</b>=1W前</span>
            <span>🟪 <b>紫</b>=2W前</span>
            <span>📘四季報 📗みんかぶ 📙かぶたん 📕BC</span>
        </div>
        """, unsafe_allow_html=True)
    
    # CSV出力
    df_csv = df.copy()
    # 内部列とHTMLを除去
    drop_cols = [c for c in df_csv.columns if c.startswith("_")]
    if drop_cols:
        df_csv = df_csv.drop(columns=drop_cols)
    if "リンク" in df_csv.columns:
        df_csv["リンク"] = df_csv["リンク"].str.replace(r'<[^>]+>', '', regex=True)
    if "チャート" in df_csv.columns:
        df_csv = df_csv.drop(columns=["チャート"])
    if "V1トレンド" in df_csv.columns:
        df_csv = df_csv.drop(columns=["V1トレンド"])
    if "V2トレンド" in df_csv.columns:
        df_csv = df_csv.drop(columns=["V2トレンド"])
    csv = df_csv.to_csv(index=False).encode('utf-8-sig')
    
    st.download_button(
        label="📥 CSVとして保存",
        data=csv,
        file_name=f'stock_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv',
        mime='text/csv',
    )
    
    # --- 買い時率推移グラフ ---
    with st.expander("📊 買い時率の推移グラフ", expanded=False):
        if os.path.exists(BUY_TIMING_HISTORY_FILE):
            try:
                with open(BUY_TIMING_HISTORY_FILE, "r", encoding="utf-8") as f:
                    hist_data = json.load(f)
                
                wl_history = hist_data.get(selected_watchlist, [])
                if wl_history and len(wl_history) >= 2:
                    # 銘柄リストを取得
                    all_codes = []
                    for entry in wl_history:
                        for code in entry.get("scores", {}).keys():
                            if code not in all_codes:
                                all_codes.append(code)
                    
                    # 企業名を付けて選択肢を作成
                    code_labels = {}
                    for code in all_codes:
                        name_display = code
                        if "stock_df" in st.session_state:
                            match = df[df["銘柄コード"] == code]
                            if not match.empty:
                                name_display = f"{code} ({match.iloc[0].get('企業名', code)})"
                        code_labels[code] = name_display
                    
                    selected_codes = st.multiselect(
                        "表示する銘柄を選択",
                        options=all_codes,
                        default=all_codes[:5],
                        format_func=lambda x: code_labels.get(x, x),
                        key="history_codes"
                    )
                    
                    if selected_codes:
                        # V1グラフ
                        st.markdown("#### 買い時率V1の推移")
                        chart_data_v1 = {}
                        times = []
                        for entry in wl_history:
                            t = entry["time"]
                            # 日時を短く表示
                            short_time = t[5:16] if len(t) >= 16 else t  # MM-DD HH:MM
                            times.append(short_time)
                            for code in selected_codes:
                                score = entry.get("scores", {}).get(code, {})
                                v1 = score.get("v1") if isinstance(score, dict) else None
                                if code not in chart_data_v1:
                                    chart_data_v1[code] = []
                                chart_data_v1[code].append(v1)
                        
                        chart_df_v1 = pd.DataFrame(chart_data_v1, index=times)
                        st.line_chart(chart_df_v1)
                        
                        # V2グラフ
                        st.markdown("#### 買い時率V2の推移")
                        chart_data_v2 = {}
                        for entry in wl_history:
                            for code in selected_codes:
                                score = entry.get("scores", {}).get(code, {})
                                v2 = score.get("v2") if isinstance(score, dict) else None
                                if code not in chart_data_v2:
                                    chart_data_v2[code] = []
                                chart_data_v2[code].append(v2)
                        
                        chart_df_v2 = pd.DataFrame(chart_data_v2, index=times)
                        st.line_chart(chart_df_v2)
                else:
                    st.info("📝 データを2回以上取得すると推移グラフが表示されます")
            except Exception:
                st.info("📝 データを2回以上取得すると推移グラフが表示されます")
        else:
            st.info("📝 データを取得すると推移グラフが表示されます")
