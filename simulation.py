import streamlit as st
import pandas as pd
import yfinance as yf
import itertools
from datetime import datetime, timedelta
import json
import os

SETTINGS_FILE = "sim_settings.json"
BEST_RESULTS_FILE = "sim_best_results.json"
CACHE_DIR = "yf_cache"

def get_historical_data(ticker):
    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_file = os.path.join(CACHE_DIR, f"{ticker}.pkl")
    
    if os.path.exists(cache_file):
        try:
            cached_df = pd.read_pickle(cache_file)
            if not cached_df.empty:
                last_date = cached_df.index[-1]
                start_str = (last_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                
                new_df = yf.Ticker(ticker).history(start=start_str)
                
                if not new_df.empty:
                    if new_df.index.tz is not None and cached_df.index.tz is None:
                        cached_df.index = cached_df.index.tz_localize(new_df.index.tz)
                    elif new_df.index.tz is None and cached_df.index.tz is not None:
                        new_df.index = new_df.index.tz_localize(cached_df.index.tz)
                        
                    df = pd.concat([cached_df, new_df])
                    df = df.loc[~df.index.duplicated(keep='last')]
                    df.to_pickle(cache_file)
                    return df
                else:
                    return cached_df
        except Exception:
            pass # フォールバックして全取得
            
    df = yf.Ticker(ticker).history(period="max")
    if not df.empty:
        df.to_pickle(cache_file)
    return df


def load_sim_settings():
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if data and "opt_mode" in data:  # Legacy single-profile format
                    return {"デフォルト": data}
                return data
        except:
            return {}
    return {}

def save_sim_settings(settings):
    try:
        with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(settings, f, ensure_ascii=False, indent=4)
    except:
        pass

def load_best_results():
    if os.path.exists(BEST_RESULTS_FILE):
        try:
            with open(BEST_RESULTS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return []
    return []

def save_best_results(results):
    try:
        with open(BEST_RESULTS_FILE, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=4)
    except:
        pass

def calculate_daily_v1_scores(df):
    score_df = pd.DataFrame(index=df.index)
    score_df['Close'] = df['Close']
    score_df['Score'] = 0
    if len(df) < 200: return score_df
    ema9 = df['Close'].ewm(span=9, adjust=False).mean()
    ema20 = df['Close'].ewm(span=20, adjust=False).mean()
    ema200 = df['Close'].ewm(span=200, adjust=False).mean()
    score_df['Score'] += (df['Close'] > ema9) * 10
    score_df['Score'] += (df['Close'] > ema20) * 10
    score_df['Score'] += (df['Close'] > ema200) * 10
    vwap_approx = (df['High'] + df['Low'] + df['Close']) / 3
    score_df['Score'] += (df['Close'] > vwap_approx) * 20
    avg_vol = df['Volume'].rolling(30).mean().shift(1)
    rvol = df['Volume'] / avg_vol
    rvol = rvol.fillna(0)
    score_df['Score'] += (rvol >= 5.0) * 20
    score_df.loc[(rvol >= 2.0) & (rvol < 5.0), 'Score'] += 10
    score_df.loc[(rvol >= 1.0) & (rvol < 2.0), 'Score'] += 5
    macd_line = df['Close'].ewm(span=12, adjust=False).mean() - df['Close'].ewm(span=26, adjust=False).mean()
    score_df['Score'] += (macd_line > 0) * 15
    prev_high = df['High'].shift(1)
    score_df['Score'] += (df['Close'] > prev_high) * 15
    return score_df

def calculate_daily_v2_scores(df):
    score_df = pd.DataFrame(index=df.index)
    score_df['Close'] = df['Close']
    score_df['Score'] = 0
    max_score = 100
    if len(df) < 200: return score_df
    ema9 = df['Close'].ewm(span=9, adjust=False).mean()
    ema20 = df['Close'].ewm(span=20, adjust=False).mean()
    ema200 = df['Close'].ewm(span=200, adjust=False).mean()
    score_df['Score'] += (df['Close'] > ema9) * 5
    score_df['Score'] += (df['Close'] > ema20) * 5
    score_df['Score'] += (df['Close'] > ema200) * 10
    vwap_approx = (df['High'] + df['Low'] + df['Close']) / 3
    score_df['Score'] += (df['Close'] > vwap_approx) * 20
    avg_vol = df['Volume'].rolling(30).mean().shift(1)
    rvol = df['Volume'] / avg_vol
    rvol = rvol.fillna(0)
    score_df['Score'] += (rvol >= 5.0) * 10
    score_df.loc[(rvol >= 2.0) & (rvol < 5.0), 'Score'] += 5
    score_df.loc[(rvol >= 1.0) & (rvol < 2.0), 'Score'] += 2
    macd_line = df['Close'].ewm(span=12, adjust=False).mean() - df['Close'].ewm(span=26, adjust=False).mean()
    score_df['Score'] += (macd_line > 0) * 10
    prev_high = df['High'].shift(1)
    score_df['Score'] += (df['Close'] > prev_high) * 10
    delta = df['Close'].diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = -delta.where(delta < 0, 0).rolling(14).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    score_df.loc[rsi < 30, 'Score'] += 10
    score_df.loc[(rsi >= 30) & (rsi <= 70), 'Score'] += 5
    rolling_mean = df['Close'].rolling(20).mean()
    rolling_std = df['Close'].rolling(20).std()
    lower_band = rolling_mean - (2 * rolling_std)
    score_df['Score'] += (df['Close'] <= lower_band) * 20
    score_df.loc[(df['Close'] > lower_band) & (df['Close'] <= rolling_mean), 'Score'] += 10
    score_df['Score'] = (score_df['Score'] / max_score * 100).astype(int)
    return score_df

def calculate_daily_v3_scores(df):
    """V3 (マルチタイムフレーム・ボラティリティ・VWAP等の加味)の過去スコアを一括計算"""
    score_df = pd.DataFrame(index=df.index)
    score_df['Score'] = 0
    import numpy as np
    
    # 全期間をループで処理するのは遅いかもしれないが、
    # V3は内部で過去のヒストグラムやATR等を動的計算するため、iterrowsで1日ずつapp.py側の関数を呼ぶか、
    # ベクトル化して計算するかの2択。ここでは保守的なベクトル化の代用として簡易版V3ベクトル化を実装。
    
    close = df['Close']
    high = df['High']
    low = df['Low']
    vol = df['Volume']
    
    # 1. 過熱感 (RSI)
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = -delta.where(delta < 0, 0).rolling(14).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    
    # 2. マルチタイムフレーム (SMA20, SMA60)
    sma20 = close.rolling(20).mean()
    sma60 = close.rolling(60).mean()
    
    # 3. ボラティリティ ATR
    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(14).mean()
    recent_high = high.rolling(10).max()
    drawdown = recent_high - close
    dd_atr_ratio = drawdown / atr
    
    # 4. RVOL
    avg_vol = vol.rolling(30).mean().shift(1)
    rvol = vol / avg_vol
    
    # 5. MACD
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    macd_sig = macd_line.ewm(span=9, adjust=False).mean()
    macd_hist = macd_line - macd_sig
    
    # スコア加算
    
    # マルチタイムフレーム (20)
    score_df.loc[sma20 > sma60, 'Score'] += 20
    score_df.loc[(sma20 <= sma60) & (close > sma60), 'Score'] += 10
    
    # ATR押し目 (25)
    score_df.loc[dd_atr_ratio >= 1.5, 'Score'] += 25
    score_df.loc[(dd_atr_ratio >= 0.5) & (dd_atr_ratio < 1.5), 'Score'] += 15
    score_df.loc[dd_atr_ratio < 0.5, 'Score'] += 5
    
    # RVOL (15)
    score_df.loc[rvol >= 3.0, 'Score'] += 15
    score_df.loc[(rvol >= 1.5) & (rvol < 3.0), 'Score'] += 10
    score_df.loc[(rvol >= 0.8) & (rvol < 1.5), 'Score'] += 5
    
    # MACD (15)
    score_df.loc[(macd_line > 0) & (macd_hist > 0), 'Score'] += 15
    score_df.loc[(macd_line <= 0) & (macd_hist > 0), 'Score'] += 10
    
    # VPVR簡易サポート (25) -> ベクトル化が難しいため、簡易的にVWAPからの乖離で代用または過去20日の最頻値価格を利用
    vwap = (high + low + close) / 3
    # 現在価格がVWAPの直上（0〜5%）にいる場合を強いサポートとする簡易表現
    vwap_ratio = (close - vwap) / vwap
    score_df.loc[(vwap_ratio >= 0) & (vwap_ratio <= 0.05), 'Score'] += 25
    score_df.loc[(vwap_ratio > 0.05) & (vwap_ratio <= 0.15), 'Score'] += 15
    score_df.loc[vwap_ratio < 0, 'Score'] += 5
    
    # 過熱感リセット (RSI >= 80 なら 0点にする)
    score_df.loc[rsi >= 80, 'Score'] = 0
    
    # 丸め処理
    score_df['Score'] = score_df['Score'].clip(upper=100).fillna(0).astype(int)
    
    # 元のdfのカラムを含める（バックテストでClose等が必要なため）
    for col in df.columns:
        if col != 'Score':
            score_df[col] = df[col]
            
    return score_df

def calculate_daily_v4_scores(df):
    """V4 (環境認識ハイブリッド型)の過去スコアを一括計算"""
    import numpy as np
    import pandas as pd
    
    close = df['Close']
    high = df['High']
    low = df['Low']
    
    # 1. 長期トレンド (200日または60日SMA)
    sma_long = close.rolling(200).mean().fillna(close.rolling(60).mean())
    
    # 2. ボラティリティ (ATR%)
    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(14).mean()
    atr_pct = atr / close
    
    # 条件判定: SMAより上で、1日の平均変動幅(ATR)が1%以上ある銘柄は「強い上昇トレンド」
    is_strong_trend = (close > sma_long) & (atr_pct >= 0.01)
    
    # 個別のスコアを一括計算
    df_v2 = calculate_daily_v2_scores(df)
    df_v3 = calculate_daily_v3_scores(df)
    
    score_df = pd.DataFrame(index=df.index)
    # 強いトレンド時はV2、それ以外はV3を採用
    score_df['Score'] = np.where(is_strong_trend, df_v2['Score'], df_v3['Score'])
    
    for col in df.columns:
        if col != 'Score':
            score_df[col] = df[col]
            
    return score_df

def run_backtest(df, initial_capital, buy_score, sell_score, take_profit, stop_loss):
    cash = initial_capital
    position = 0
    entry_price = 0
    trades = []
    dates = df.index
    scores = df['Score'].values
    closes = df['Close'].values
    opens = df['Open'].values if 'Open' in df.columns else closes
    highs = df['High'].values if 'High' in df.columns else closes
    lows = df['Low'].values if 'Low' in df.columns else closes
    daily_equity = []
    
    for i in range(len(df)):
        date = dates[i]
        score = scores[i]
        price = closes[i]
        
        if position > 0:
            open_p = opens[i]
            high_p = highs[i]
            low_p = lows[i]
            close_p = closes[i]
            
            # 各価格帯での利益・損失率
            open_pct = (open_p - entry_price) / entry_price
            high_pct = (high_p - entry_price) / entry_price
            low_pct = (low_p - entry_price) / entry_price
            close_pct = (close_p - entry_price) / entry_price
            
            sold = False
            sell_price = 0
            reason = ""
            
            # 1. 窓開けギャップダウンで朝イチで損切りラインを下回った場合（寄り付きで強制損切り）
            if open_pct <= stop_loss:
                sold = True
                sell_price = open_p
                reason = "損切り (窓開け)"
            # 2. 日中に損切りラインに引っかかった場合（設定した損切り価格ピッタリか、Low価格のうち高い方で約定したとみなす）
            elif low_pct <= stop_loss:
                sold = True
                sell_price = min(open_p, entry_price * (1 + stop_loss)) # 始値より上で損切り設定があれば設定価格、ギャップダウンなら始値
                reason = "損切り"
            # 3. 窓開けギャップアップで利確ラインを超えていた場合（寄り付きで利確）
            elif open_pct >= take_profit:
                sold = True
                sell_price = open_p
                reason = "利確 (窓開け)"
            # 4. 日中に利確ラインに引っかかった場合
            elif high_pct >= take_profit:
                sold = True
                sell_price = max(open_p, entry_price * (1 + take_profit))
                reason = "利確"
            # 5. 引け時点でスコア条件で売り
            elif score <= sell_score:
                sold = True
                sell_price = close_p
                reason = "スコア下落"
                
            if sold:
                cash += position * sell_price
                actual_profit_pct = (sell_price - entry_price) / entry_price
                trades.append({
                    "日付": date,
                    "取引": "🔵 売",
                    "約定価格": sell_price,
                    "株数": position,
                    "損益(%)": actual_profit_pct * 100,
                    "判定理由": reason
                })
                position = 0
                entry_price = 0
        
        # 買い判定（当日の終値で判定するためClose価格を使う）
        if position == 0 and score >= buy_score:
            price = closes[i]
            shares = int(cash // (price * 100)) * 100
            if shares > 0:
                cost = shares * price
                cash -= cost
                position = shares
                entry_price = price
                trades.append({
                    "日付": date,
                    "取引": "🔴 買",
                    "約定価格": price,
                    "株数": shares,
                    "損益(%)": 0,
                    "判定理由": f"スコア >= {buy_score}"
                })
                
        equity = cash + (position * price)
        daily_equity.append(equity)
        
    if position > 0:
        price = closes[-1]
        profit_pct = (price - entry_price) / entry_price
        cash += position * price
        trades.append({
            "日付": dates[-1],
            "取引": "🔵 売 (期間終了)",
            "約定価格": price,
            "株数": position,
            "損益(%)": profit_pct * 100,
            "判定理由": "期間終了に伴う強制決済"
        })
        position = 0
        daily_equity[-1] = cash
        
    return cash, daily_equity, trades


def run_bnh_backtest(df, initial_capital):
    """
    余剰資金での追加購入型ガチホ（Buy & Hold & Accumulate）
    初日に資金の範囲内で最大株数（100株単位）を買い、
    その後、残った現金でさらに100株買えるほど株価が下がった日があれば追加購入（ナンピン）する。
    """
    cash = initial_capital
    position = 0
    trades = []
    dates = df.index
    closes = df['Close'].values
    daily_equity = []
    
    # 全ての購入の総費用を追跡（平均取得単価などを出すため）
    total_cost = 0
    
    for i in range(len(df)):
        date = dates[i]
        price = closes[i]
        
        # 現金で買える最大株数 (100株単位)
        shares = int(cash // (price * 100)) * 100
        
        if shares > 0:
            cost = shares * price
            cash -= cost
            position += shares
            total_cost += cost
            
            # 最初の購入か追加購入かを判定
            if len(trades) == 0:
                reason = "初期一括購入"
            else:
                reason = "余力による買い増し"
                
            trades.append({
                "日付": date,
                "取引": "🟢 積立買",
                "約定価格": price,
                "株数": shares,
                "消費資金": cost,
                "残現金": cash,
                "判定理由": reason
            })
            
        equity = cash + (position * price)
        daily_equity.append(equity)
        
    if position > 0:
        price = closes[-1]
        avg_price = total_cost / position if position > 0 else 0
        profit_pct = (price - avg_price) / avg_price if avg_price > 0 else 0
        cash += position * price
        
        trades.append({
            "日付": dates[-1],
            "取引": "🔵 売 (期間終了)",
            "約定価格": price,
            "株数": position,
            "損益(%)": profit_pct * 100,
            "判定理由": "期間終了に伴う評価決済"
        })
        position = 0
        daily_equity[-1] = cash
        
    return cash, daily_equity, trades


def run_portfolio_backtest(df_dict, initial_capital, buy_score, sell_score, take_profit, stop_loss):
    cash = initial_capital
    positions = {ticker: 0 for ticker in df_dict}
    entry_prices = {ticker: 0 for ticker in df_dict}
    trades = []
    
    all_dates = pd.DatetimeIndex([])
    for df in df_dict.values():
        all_dates = all_dates.union(df.index)
    all_dates = all_dates.sort_values()
    
    daily_equity = []
    
    for date in all_dates:
        # 1. 保有銘柄の売却判定
        for ticker, df in df_dict.items():
            if date not in df.index: continue
            
            row = df.loc[date]
            score = row['Score']
            
            if positions[ticker] > 0:
                open_p = row.get('Open', row['Close'])
                high_p = row.get('High', row['Close'])
                low_p = row.get('Low', row['Close'])
                close_p = row['Close']
                entry_price = entry_prices[ticker]
                
                open_pct = (open_p - entry_price) / entry_price
                high_pct = (high_p - entry_price) / entry_price
                low_pct = (low_p - entry_price) / entry_price
                
                sold = False
                sell_price = 0
                reason = ""
                
                if open_pct <= stop_loss:
                    sold = True; sell_price = open_p; reason = "損切り (窓開け)"
                elif low_pct <= stop_loss:
                    sold = True; sell_price = min(open_p, entry_price * (1 + stop_loss)); reason = "損切り"
                elif open_pct >= take_profit:
                    sold = True; sell_price = open_p; reason = "利確 (窓開け)"
                elif high_pct >= take_profit:
                    sold = True; sell_price = max(open_p, entry_price * (1 + take_profit)); reason = "利確"
                elif score <= sell_score:
                    sold = True; sell_price = close_p; reason = "スコア下落"
                    
                if sold:
                    cash += positions[ticker] * sell_price
                    actual_profit_pct = (sell_price - entry_price) / entry_price
                    trades.append({
                        "日付": date, "銘柄": ticker, "取引": "🔵 売", "約定価格": sell_price,
                        "株数": positions[ticker], "損益(%)": actual_profit_pct * 100, "判定理由": reason
                    })
                    positions[ticker] = 0; entry_prices[ticker] = 0
                    
        # 2. 新規購入判定（キャッシュがある限り順番に買う）
        for ticker, df in df_dict.items():
            if date not in df.index: continue
            
            row = df.loc[date]
            score = row['Score']
            
            if positions[ticker] == 0 and score >= buy_score:
                price = row['Close']
                shares = int(cash // (price * 100)) * 100
                if shares > 0:
                    cost = shares * price
                    cash -= cost
                    positions[ticker] = shares
                    entry_prices[ticker] = price
                    trades.append({
                        "日付": date, "銘柄": ticker, "取引": "🔴 買", "約定価格": price,
                        "株数": shares, "損益(%)": 0, "判定理由": f"スコア >= {buy_score}"
                    })
                    
        # 3. 毎日の資産評価
        equity = cash
        for ticker, df in df_dict.items():
            if positions[ticker] > 0:
                if date in df.index:
                    equity += positions[ticker] * df.loc[date, 'Close']
                else:
                    try:
                        last_price = df.loc[:date, 'Close'].iloc[-1]
                        equity += positions[ticker] * last_price
                    except:
                        equity += positions[ticker] * entry_prices[ticker]
        daily_equity.append(equity)
        
    # 最終日の強制決済
    last_date = all_dates[-1] if len(all_dates) > 0 else None
    if last_date is not None:
        for ticker, pos in positions.items():
            if pos > 0:
                df = df_dict[ticker]
                price = df['Close'].iloc[-1] if not df.empty else entry_prices[ticker]
                profit_pct = (price - entry_prices[ticker]) / entry_prices[ticker]
                cash += pos * price
                trades.append({
                    "日付": last_date, "銘柄": ticker, "取引": "🔵 売 (期間終了)", "約定価格": price,
                    "株数": pos, "損益(%)": profit_pct * 100, "判定理由": "期間終了に伴う強制決済"
                })
                positions[ticker] = 0
                
    if daily_equity: daily_equity[-1] = cash
    return cash, daily_equity, trades, all_dates

def run_portfolio_bnh_backtest(df_dict, initial_capital):
    cash = initial_capital
    positions = {ticker: 0 for ticker in df_dict}
    trades = []
    total_costs = {ticker: 0 for ticker in df_dict}
    
    all_dates = pd.DatetimeIndex([])
    for df in df_dict.values():
        all_dates = all_dates.union(df.index)
    all_dates = all_dates.sort_values()
    
    daily_equity = []
    
    for date in all_dates:
        for ticker, df in df_dict.items():
            if date not in df.index: continue
                
            price = df.loc[date, 'Close']
            shares = int(cash // (price * 100)) * 100
            if shares > 0:
                cost = shares * price
                cash -= cost
                positions[ticker] += shares
                total_costs[ticker] += cost
                trades.append({
                    "日付": date, "銘柄": ticker, "取引": "🟢 積立買", "約定価格": price,
                    "株数": shares, "消費資金": cost, "残現金": cash, "判定理由": "余力による買い増し"
                })
                
        equity = cash
        for ticker, df in df_dict.items():
            if positions[ticker] > 0:
                if date in df.index:
                    equity += positions[ticker] * df.loc[date, 'Close']
                else:
                    try:
                        last_price = df.loc[:date, 'Close'].iloc[-1]
                        equity += positions[ticker] * last_price
                    except:
                        pass
        daily_equity.append(equity)
        
    last_date = all_dates[-1] if len(all_dates) > 0 else None
    if last_date is not None:
        for ticker, pos in positions.items():
            if pos > 0:
                df = df_dict[ticker]
                price = df['Close'].iloc[-1] if not df.empty else 0
                avg_price = total_costs[ticker] / pos if pos > 0 else 0
                profit_pct = (price - avg_price) / avg_price if avg_price > 0 else 0
                cash += pos * price
                trades.append({
                    "日付": last_date, "銘柄": ticker, "取引": "🔵 売 (期間終了)", "約定価格": price,
                    "株数": pos, "損益(%)": profit_pct * 100, "判定理由": "期間終了に伴う評価決済"
                })
                positions[ticker] = 0
                
    if daily_equity: daily_equity[-1] = cash
    return cash, daily_equity, trades, all_dates


def render_simulation_page(watchlists, name_overrides):
    st.title("🧪 最適化シミュレーション (Backtesting Optimizer)")
    st.markdown("""
        過去データを用いてあらゆる「買い・売り・利確・損切り」ルールの組み合わせを総当たりでテストし、最強のルールを見つけます。<br/>
        単なる「ガチホ（余力資金での積立買い増し）」とも自動比較され、その銘柄での歴代ベスト設定は自動で保存されます。
    """, unsafe_allow_html=True)
    
    all_codes = []
    for wl, codes_str in watchlists.items():
        if codes_str:
            codes = [c.strip() for c in codes_str.replace('\n', ',').replace(' ', ',').split(',') if c.strip()]
            all_codes.extend(codes)
    all_codes = list(set(all_codes))
    
    options = []
    ticker_to_name = {}
    for code in sorted(all_codes):
        name = name_overrides.get(code, "")
        label = f"{code} ({name})" if name else code
        options.append(label)
        ticker_to_name[code] = name
        
    profiles = load_sim_settings()
    if not profiles:
        profiles = {"デフォルト": {}}
        
    profile_names = list(profiles.keys())
    
    if "current_sim_profile" not in st.session_state:
        st.session_state.current_sim_profile = profile_names[0] if profile_names else ""
        
    # Profile selection UI
    col_p1, col_p2, col_p3 = st.columns([2, 1, 1])
    with col_p1:
        selected_prof_idx = profile_names.index(st.session_state.current_sim_profile) if st.session_state.current_sim_profile in profile_names else 0
        selected_prof_name = st.selectbox("📂 保存済みシミュレーション設定", profile_names, index=selected_prof_idx)
        if selected_prof_name != st.session_state.current_sim_profile:
            st.session_state.current_sim_profile = selected_prof_name
            st.rerun()
            
    with col_p2:
        st.write("")
        st.write("")
        if st.button("🗑️ 現在の設定を削除", use_container_width=True, disabled=len(profile_names) <= 1):
            if selected_prof_name in profiles:
                del profiles[selected_prof_name]
                save_sim_settings(profiles)
                st.session_state.current_sim_profile = list(profiles.keys())[0]  # Fallback to the first available settings
                st.success(f"「{selected_prof_name}」を削除しました。")
                st.rerun()
                
    s = profiles.get(st.session_state.current_sim_profile, {})
    
    def get_index(options_list, val, default_idx=0):
        try: return options_list.index(val)
        except ValueError: return default_idx

    period_options = ["3mo", "6mo", "1y", "2y", "3y", "5y", "10y", "max"]
    trend_options = ["V1 トレンド", "V2 トレンド", "V3 トレンド", "V4 トレンド"]

    with st.expander("⚙️ シミュレーション設定パラメーター", expanded=True):
        st.markdown("**最適化モードの選択**")
        prof_key = st.session_state.current_sim_profile
        opt_mode = st.radio("最適化モード", ["個別銘柄 (単一)", "ウォッチリスト全体 (ポートフォリオ)"], 
                            index=0 if s.get("opt_mode", "個別銘柄 (単一)") == "個別銘柄 (単一)" else 1,
                            horizontal=True, label_visibility="collapsed", key=f"opt_mode_{prof_key}")
        
        c1, c2, c3 = st.columns(3)
        with c1:
            if opt_mode == "個別銘柄 (単一)":
                if options:
                    idx = get_index(options, s.get("ticker", ""), 0) if s.get("ticker") else 0
                    selected_ticker = st.selectbox("対象銘柄", options, index=idx, key=f"ticker_sel_{prof_key}")
                else:
                    st.warning("ウォッチリストに銘柄が登録されていません。手動入力してください。")
                    selected_ticker = st.text_input("対象銘柄コード (例: 7203)", value=s.get("ticker", "7203"), key=f"ticker_txt_{prof_key}")
                selected_wl = None
            else:
                wl_options = [wl for wl, codes in watchlists.items() if codes]
                if wl_options or watchlists:
                    # 'すべて' を追加
                    if "🌟 すべて" not in wl_options:
                        wl_options.insert(0, "🌟 すべて")
                    idx = get_index(wl_options, s.get("watchlist", ""), 0) if s.get("watchlist") else 0
                    selected_wl = st.selectbox("対象ウォッチリスト", wl_options, index=idx, key=f"wl_sel_{prof_key}")
                else:
                    st.warning("有効なウォッチリストがありません。")
                    selected_wl = None
                selected_ticker = None
                
        with c2:
            period = st.selectbox("分析期間", period_options, index=get_index(period_options, s.get("period", "1y")), key=f"period_{prof_key}")
        with c3:
            trend_type = st.radio("使用するスコア指標", trend_options, index=get_index(trend_options, s.get("trend_type", "V1 トレンド")), key=f"trend_{prof_key}")
            
        initial_cap = st.number_input("初期資金 (円)", value=s.get("initial_cap", 1000000), step=100000, key=f"ic_{prof_key}")
            
        st.markdown("---")
        st.markdown("#### 🔍 探索するパラメータの範囲指定")
        st.markdown("細かくしすぎると計算に時間がかかります。まずは粗く探索して、良さそうな範囲を絞り込むのがおすすめです。")
        
        r1, r2, r3, r4 = st.columns(4)
        with r1:
            st.markdown("**買いスコア (以上で買う)**")
            buy_start = st.number_input("開始", value=s.get("buy_start", 50), key=f"b1_{prof_key}")
            buy_end = st.number_input("終了", value=s.get("buy_end", 80), key=f"b2_{prof_key}")
            buy_step = st.number_input("間隔", value=s.get("buy_step", 10), key=f"b3_{prof_key}")
        with r2:
            st.markdown("**売りスコア (以下で売る)**")
            sell_start = st.number_input("開始", value=s.get("sell_start", 10), key=f"s1_{prof_key}")
            sell_end = st.number_input("終了", value=s.get("sell_end", 40), key=f"s2_{prof_key}")
            sell_step = st.number_input("間隔", value=s.get("sell_step", 10), key=f"s3_{prof_key}")
        with r3:
            st.markdown("**利益確定 %**")
            tp_start = st.number_input("開始", value=s.get("tp_start", 5), key=f"tp1_{prof_key}")
            tp_end = st.number_input("終了", value=s.get("tp_end", 20), key=f"tp2_{prof_key}")
            tp_step = st.number_input("間隔", value=s.get("tp_step", 5), key=f"tp3_{prof_key}")
        with r4:
            st.markdown("**損切り %**")
            sl_start = st.number_input("開始", value=s.get("sl_start", -20), key=f"sl1_{prof_key}")
            sl_end = st.number_input("終了", value=s.get("sl_end", -5), key=f"sl2_{prof_key}")
            sl_step = st.number_input("間隔", value=s.get("sl_step", 5), key=f"sl3_{prof_key}")
            
        st.markdown("---")
        col_s1, col_s2, col_s3 = st.columns([2, 1.5, 3])
        with col_s1:
            save_name_input = st.text_input("📝 設定の保存名", value=st.session_state.current_sim_profile)
        with col_s2:
            st.write("")
            st.write("")
            if st.button("💾 この設定で保存", use_container_width=True):
                if not save_name_input.strip():
                    st.error("保存名を入力してください。")
                else:
                    new_s = {
                        "opt_mode": opt_mode, "watchlist": selected_wl, "ticker": selected_ticker, 
                        "period": period, "trend_type": trend_type, "initial_cap": initial_cap,
                        "buy_start": buy_start, "buy_end": buy_end, "buy_step": buy_step,
                        "sell_start": sell_start, "sell_end": sell_end, "sell_step": sell_step,
                        "tp_start": tp_start, "tp_end": tp_end, "tp_step": tp_step,
                        "sl_start": sl_start, "sl_end": sl_end, "sl_step": sl_step
                    }
                    profiles[save_name_input.strip()] = new_s
                    save_sim_settings(profiles)
                    st.session_state.current_sim_profile = save_name_input.strip()
                    st.success(f"「{save_name_input.strip()}」として保存しました！")
                    st.rerun()
                    
        with col_s3:
            st.write("")
            st.write("")
            submitted = st.button("🚀 一括シミュレーション開始", use_container_width=True, type="primary")
        
    if submitted:
        def make_range(start, end, step, is_pct):
            vals = []
            val = start
            if step == 0:
                return [val / 100 if is_pct else val]
            while (val <= end + 0.0001) if step > 0 else (val >= end - 0.0001):
                vals.append(val / 100 if is_pct else val)
                val += step
            return vals
            
        b_list = make_range(buy_start, buy_end, buy_step, False)
        s_list = make_range(sell_start, sell_end, sell_step, False)
        tp_list = make_range(tp_start, tp_end, tp_step, True)
        sl_list = make_range(sl_start, sl_end, sl_step, True)
        
        if not all([b_list, s_list, tp_list, sl_list]):
            st.error("パラメータの設定が間違っています。(終了値が開始値より大きいのに間隔がマイナスなど)")
            return
            
        if opt_mode == "個別銘柄 (単一)":
            code_only = selected_ticker.split(' ')[0] if ' ' in selected_ticker else selected_ticker
            ticker_name = ticker_to_name.get(code_only, "")
            target_codes = [code_only]
            display_name = f"{code_only} {ticker_name}".strip()
        else:
            if not selected_wl:
                st.error("有効なウォッチリストが選択されていません。")
                return
            
            if selected_wl == "🌟 すべて":
                all_tickers = []
                for wl, codes in watchlists.items():
                    if not codes: continue
                    clist = [c.strip() for c in codes.replace('\n', ',').replace(' ', ',').split(',') if c.strip()]
                    for c in clist:
                        if c not in all_tickers:
                            all_tickers.append(c)
                target_codes = all_tickers
                display_name = "ポートフォリオ (すべて)"
            else:
                codes_str = watchlists.get(selected_wl, "")
                target_codes = [c.strip() for c in codes_str.replace('\n', ',').replace(' ', ',').split(',') if c.strip()]
                display_name = f"ポートフォリオ ({selected_wl})"
            
        fetch_period_map = {"3mo": "2y", "6mo": "2y", "1y": "2y", "2y": "3y", "3y": "5y", "5y": "10y", "10y": "max", "max": "max"}
        fetch_period = fetch_period_map.get(period, "5y")
        
        df_dict = {}
        with st.spinner(f"対象銘柄のデータを取得・スコア計算中 ({len(target_codes)}銘柄)..."):
            for tc in target_codes:
                ticker_yf = tc + ".T" if tc.isdigit() else tc
                hist = get_historical_data(ticker_yf)
                if hist.empty or len(hist) < 200:
                    if opt_mode == "個別銘柄 (単一)":
                        st.error("データが存在しないか、計算に必要な期間のデータが不足しています。")
                        return
                    continue
                for c in ['Close', 'High', 'Low', 'Volume']:
                    if c in hist.columns:
                        hist[c] = hist[c].ffill().bfill()
                        
                if trend_type == "V1 トレンド":
                    scored_df = calculate_daily_v1_scores(hist)
                elif trend_type == "V3 トレンド":
                    scored_df = calculate_daily_v3_scores(hist)
                elif trend_type == "V4 トレンド":
                    scored_df = calculate_daily_v4_scores(hist)
                else:
                    scored_df = calculate_daily_v2_scores(hist)
                    
                now = pd.Timestamp.now(tz=hist.index.tz) if hist.index.tz is not None else datetime.now()
                p_map = {"3mo": pd.DateOffset(months=3), "6mo": pd.DateOffset(months=6), "1y": pd.DateOffset(years=1),
                         "2y": pd.DateOffset(years=2), "3y": pd.DateOffset(years=3), "5y": pd.DateOffset(years=5),
                         "10y": pd.DateOffset(years=10)}
                
                if period in p_map:
                    start_date = now - p_map[period]
                else:
                    start_date = hist.index[200]
                    
                test_df = scored_df[scored_df.index >= start_date].copy()
                min_valid_date = scored_df.index[200]
                if len(test_df) > 0 and test_df.index[0] < min_valid_date:
                    test_df = scored_df[scored_df.index >= min_valid_date].copy()
                    
                if len(test_df) > 0:
                    df_dict[tc] = test_df
                    
        if not df_dict:
            st.error("評価可能なデータ期間がありません。")
            return
            
        with st.spinner("一括最適化パラメーターを探索中 (数秒〜数分かかります)..."):
            combinations = list(itertools.product(b_list, s_list, tp_list, sl_list))
            total_combs = len(combinations)
            
            all_best_results = []
            
            # Progress tracking
            ticker_progress = st.progress(0, text="銘柄ごとの最適化準備中...")
            comb_progress = st.progress(0, text="パラメーター探索準備中...")
            
            total_tickers = len(df_dict)
            
            for t_idx, (tc, single_df) in enumerate(df_dict.items()):
                ticker_name = ticker_to_name.get(tc, "")
                display_tc_name = f"{tc} {ticker_name}".strip()
                ticker_progress.progress((t_idx) / total_tickers, text=f"処理中: {display_tc_name} ({t_idx+1}/{total_tickers})")
                
                bnh_final_equity, bnh_daily_equity, bnh_trades = run_bnh_backtest(single_df, initial_cap)
                
                best_final_equity = -1
                best_trades = None
                best_curve = None
                best_params = None
                best_all_dates = None
                best_results_list = []
                
                for i, (b, s, tp, sl) in enumerate(combinations):
                    if i % max(1, total_combs // 100) == 0 or i == total_combs - 1:
                        comb_progress.progress((i+1)/total_combs, text=f"パラメーター探索中: {i+1}/{total_combs}")
                        
                    if b <= s:
                        continue
                        
                    fe, curve, trds = run_backtest(single_df, initial_cap, b, s, tp, sl)
                    run_dates = single_df.index
                        
                    profit = fe - initial_cap
                    pct_return = profit / initial_cap * 100
                    best_results_list.append({
                        "買い≧": b, "売り≦": s, "利確%": round(tp*100, 1), "損切%": round(sl*100, 1),
                        "取引回数": len([x for x in trds if x['取引'].startswith("🔵")]),
                        "最終資産": fe, "リターン": round(pct_return, 2)
                    })
                    if fe > best_final_equity:
                        best_final_equity = fe
                        best_trades = trds
                        best_curve = curve
                        best_params = (b, s, tp, sl)
                        best_all_dates = run_dates
                
                if best_params is not None:
                    best_return_pct = round((best_final_equity-initial_cap)/initial_cap*100, 2)
                    bnh_return_pct = round((bnh_final_equity-initial_cap)/initial_cap*100, 2)
                    eval_start = best_all_dates[0].strftime('%Y-%m-%d') if best_all_dates is not None and len(best_all_dates)>0 else "-"
                    eval_end = best_all_dates[-1].strftime('%Y-%m-%d') if best_all_dates is not None and len(best_all_dates)>0 else "-"
                    
                    all_best_results.append({
                        "銘柄": display_tc_name,
                        "期間": period,
                        "指標": trend_type,
                        "買い≧": best_params[0],
                        "売り≦": best_params[1],
                        "利確%": round(best_params[2]*100, 1),
                        "損切%": round(best_params[3]*100, 1),
                        "取引回数": len([x for x in best_trades if x['取引'].startswith("🔵")]),
                        "最終資産": round(best_final_equity, 0),
                        "リターン": best_return_pct,
                        "ガチホ比較": f"{bnh_return_pct:+.2f}%",
                        "評価開始": eval_start,
                        "評価終了": eval_end,
                        # For single mode detail chart
                        "_best_results_list": best_results_list,
                        "_best_trades": best_trades,
                        "_bnh_trades": bnh_trades,
                        "_best_curve": best_curve,
                        "_best_all_dates": best_all_dates,
                        "_bnh_final_equity": bnh_final_equity,
                        "_bnh_return_pct": bnh_return_pct
                    })
                    
            ticker_progress.empty()
            comb_progress.empty()
            
        if not all_best_results:
            st.warning("有効な評価結果が得られませんでした。")
            return
            
        # UI rendering for results
        st.success(f"✨ 全 {len(all_best_results)} 銘柄、各 {total_combs} 通りの最適化が完了しました！")
        
        # Save Best Results to File
        saved_results = load_best_results()
        for res in all_best_results:
            new_result = {k: v for k, v in res.items() if not k.startswith('_')}
            updated = False
            for i, r in enumerate(saved_results):
                if r.get("銘柄") == new_result["銘柄"] and r.get("期間") == new_result["期間"] and r.get("指標", "") == new_result.get("指標", ""):
                    saved_results[i] = new_result
                    updated = True
                    break
            if not updated:
                saved_results.append(new_result)
        save_best_results(saved_results)
        st.success(f"✨ 全 {total_combs} 通りの最適化が完了しました！（評価期間: {eval_start} 〜 {eval_end}）")
        
        st.markdown("### 🏆 ウォッチリスト最適化結果 一覧")
        st.markdown("各銘柄ごとに最も成績が良かったルールと結果です。詳細グラフを確認するには下の「過去のベスト設定一覧」から行をクリックしてください。")
        
        display_results = [{k: v for k, v in r.items() if not k.startswith('_')} for r in all_best_results]
        res_df = pd.DataFrame(display_results).sort_values("リターン", ascending=False).reset_index(drop=True)
        st.dataframe(res_df.style.format({"最終資産": "¥{:,.0f}", "リターン": "{:+.2f}%"}), use_container_width=True)
        
        st.markdown("---")
        
        # If Single Ticker Mode, show the detailed result chart immediately just as before
        if opt_mode == "個別銘柄 (単一)" and len(all_best_results) > 0:
            res = all_best_results[0]
            st.markdown(f"### 🥇 第1位ルール 詳細分析 ({res['銘柄']})")
            
            best_return_pct = res['リターン']
            best_final_equity = res['_best_results_list'][0]['最終資産'] if res.get('_best_results_list') else res['最終資産']
            # Search actual max
            for r in res.get('_best_results_list', []):
                if r['リターン'] == best_return_pct:
                    best_final_equity = r['最終資産']
                    break
                    
            bnh_return_pct = res['_bnh_return_pct']
            bnh_final_equity = res['_bnh_final_equity']
            diff_pct = best_return_pct - bnh_return_pct
            
            best_trades = res['_best_trades']
            bnh_trades = res['_bnh_trades']
            best_params = [res['買い≧'], res['売り≦'], res['利確%']/100, res['損切%']/100]
            trend_type = res['指標']
            
            col_res1, col_res2 = st.columns(2)
            with col_res1:
                first_info = ""
                if len(best_trades) > 0 and '買' in best_trades[0]['取引']:
                    first_info = f"\n\n*(初回購入: {int(best_trades[0]['株数'])}株)*"
                st.info(f"**🤖 最適化ルール結果:**\n\nReturn: **{best_return_pct:+.2f}%**\n\n最終資産: ¥{best_final_equity:,.0f}{first_info}")
                
            with col_res2:
                bnh_info = ""
                if len(bnh_trades) > 0 and '積立買' in bnh_trades[0]['取引']:
                    bnh_info = f"\n\n*(初回一括購入: {int(bnh_trades[0]['株数'])}株)*"
                st.warning(f"**📦 そのまま持ち続けた場合 (積立・ガチホ):**\n\nReturn: **{bnh_return_pct:+.2f}%**\n\n最終資産: ¥{bnh_final_equity:,.0f}{bnh_info}")
                
            st.markdown(f"**📊 比較**: 最適化ツールを使った方が、ナンピン・ガチホよりも **{diff_pct:+.2f}%** 得でした！")
        
            st.write(f"**使用指標**: {trend_type}")
            st.write(f"**ルール**: 買いスコア >= {best_params[0]} / 売りスコア <= {best_params[1]} / 利確 {round(best_params[2]*100,1)}% / 損切り {round(best_params[3]*100,1)}%")
            
            import plotly.graph_objects as go
            from plotly.subplots import make_subplots

            fig = make_subplots(specs=[[{"secondary_y": True}]])

            # 1. 最適化ツールの資産推移 (左軸)
            fig.add_trace(
                go.Scatter(
                    x=res['_best_all_dates'],
                    y=res['_best_curve'],
                    name="最適化ツール (左軸:資産)",
                    line=dict(color="#00AEEF", width=2),
                    hovertemplate="<b>日付</b>: %{x|%Y-%m-%d}<br><b>資産</b>: ¥%{y:,.0f}<extra></extra>"
                ),
                secondary_y=False,
            )

            # 2. ガチホの資産推移 (左軸)
            fig.add_trace(
                go.Scatter(
                    x=res['_best_all_dates'],
                    y=res['_bnh_daily_equity'] if '_bnh_daily_equity' in res else bnh_daily_equity,
                    name="積立ガチホ (左軸:資産)",
                    line=dict(color="#FF9900", width=2, dash='dot'),
                    hovertemplate="<b>日付</b>: %{x|%Y-%m-%d}<br><b>資産</b>: ¥%{y:,.0f}<extra></extra>"
                ),
                secondary_y=False,
            )

            # 3. 実際の株価 (右軸) - 個別銘柄の場合のみ表示
            if opt_mode == "個別銘柄 (単一)":
                fig.add_trace(
                    go.Scatter(
                        x=single_df.index,
                        y=single_df['Close'],
                        name="株価 (右軸)",
                        line=dict(color="rgba(255, 255, 255, 0.2)", width=1),
                        hovertemplate="<b>日付</b>: %{x|%Y-%m-%d}<br><b>株価</b>: ¥%{y:,.0f}<extra></extra>"
                    ),
                    secondary_y=True,
                )

            # --- 購入・売却・利確・損切りのマーカーを追加 ---
            buy_x, buy_y, buy_text = [], [], []
            sell_x, sell_y, sell_text = [], [], []
            tp_x, tp_y, tp_text = [], [], []
            sl_x, sl_y, sl_text = [], [], []
            
            for trd in best_trades:
                # find closest date in run_dates or test_df.index
                match_dates = [d for d in res['_best_all_dates'] if d.strftime('%Y-%m-%d') == trd['日付']]
                if match_dates:
                    d_obj = match_dates[0]
                    # We need the equity value at this date
                    idx = list(res['_best_all_dates']).index(d_obj)
                    eq = res['_best_curve'][idx]
                    
                    if "🔵" in trd['取引']:
                        buy_x.append(d_obj)
                        buy_y.append(eq)
                        buy_text.append(f"買: {trd['株数']}株<br>¥{trd['株価']:,.0f}")
                    elif "🔴 売却" in trd['取引']:
                        sell_x.append(d_obj)
                        sell_y.append(eq)
                        sell_text.append(f"売: {trd['株数']}株<br>¥{trd['株価']:,.0f}<br>損益: {trd['損益']}")
                    elif "🟢 利確" in trd['取引']:
                        tp_x.append(d_obj)
                        tp_y.append(eq)
                        tp_text.append(f"利確: {trd['株数']}株<br>¥{trd['株価']:,.0f}<br>損益: {trd['損益']}")
                    elif "🟣 損切" in trd['取引']:
                        sl_x.append(d_obj)
                        sl_y.append(eq)
                        sl_text.append(f"損切: {trd['株数']}株<br>¥{trd['株価']:,.0f}<br>損益: {trd['損益']}")

            # マーカーの追加 (左軸の資産グラフ上に配置)
            if buy_x:
                fig.add_trace(go.Scatter(x=buy_x, y=buy_y, mode='markers', name='購入', marker=dict(color='blue', size=8, symbol='triangle-up'), text=buy_text, hoverinfo='text'), secondary_y=False)
            if sell_x:
                fig.add_trace(go.Scatter(x=sell_x, y=sell_y, mode='markers', name='売却 (スコア)', marker=dict(color='red', size=8, symbol='triangle-down'), text=sell_text, hoverinfo='text'), secondary_y=False)
            if tp_x:
                fig.add_trace(go.Scatter(x=tp_x, y=tp_y, mode='markers', name='利益確定', marker=dict(color='#00FF00', size=8, symbol='star'), text=tp_text, hoverinfo='text'), secondary_y=False)
            if sl_x:
                fig.add_trace(go.Scatter(x=sl_x, y=sl_y, mode='markers', name='損切り', marker=dict(color='#FF00FF', size=8, symbol='x'), text=sl_text, hoverinfo='text'), secondary_y=False)

            fig.update_layout(
                title="資産推移の比較と取引タイミング",
                xaxis_title="日付",
                yaxis_title="合計資産 (円)",
                yaxis2_title="株価 (円)",
                hovermode="x unified",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                template="plotly_dark",
                height=500,
                margin=dict(l=40, r=40, t=60, b=40)
            )
            fig.update_yaxes(tickformat="¥,.0f", secondary_y=False)
            fig.update_yaxes(tickformat="¥,.0f", secondary_y=True, showgrid=False)
            
            st.plotly_chart(fig, use_container_width=True)
            
            tab_opt, tab_bnh = st.tabs(["📜 最適化ルールの売買履歴", "📥 ナンピン・ガチホ側の追加購入履歴"])
            with tab_opt:
                if best_trades:
                    tdf = pd.DataFrame(best_trades)
                    tdf['日付'] = tdf['日付'].dt.strftime('%Y-%m-%d')
                    if '銘柄' in tdf.columns:
                        tdf['銘柄'] = tdf['銘柄'].apply(lambda x: f"{x} {ticker_to_name.get(x, '')}".strip())
                    if '損益(%)' in tdf.columns:
                        tdf['損益(%)'] = tdf['損益(%)'].apply(lambda x: f"{x:+.1f}%" if x != 0 else "-")
                    st.dataframe(tdf, use_container_width=True)
                else:
                    st.info("この期間での取引は発生しませんでした。")
                    
            with tab_bnh:
                if bnh_trades:
                    bdf = pd.DataFrame(bnh_trades)
                    bdf['日付'] = bdf['日付'].dt.strftime('%Y-%m-%d')
                    if '銘柄' in bdf.columns:
                        bdf['銘柄'] = bdf['銘柄'].apply(lambda x: f"{x} {ticker_to_name.get(x, '')}".strip())
                    if '損益(%)' in bdf.columns:
                        bdf['損益(%)'] = bdf['損益(%)'].apply(lambda x: f"{x:+.1f}%" if pd.notnull(x) and x != 0 else "-")
                    if '消費資金' in bdf.columns:
                        bdf['消費資金'] = bdf['消費資金'].apply(lambda x: f"¥{x:,.0f}" if pd.notnull(x) else "-")
                    if '残現金' in bdf.columns:
                        bdf['残現金'] = bdf['残現金'].apply(lambda x: f"¥{x:,.0f}" if pd.notnull(x) else "-")
                    st.dataframe(bdf.fillna("-"), use_container_width=True)
                else:
                    st.info("この期間での取引は発生しませんでした。")

    # --- 保存された一覧の表示 ---
    st.markdown("---")
    res_list = load_best_results()
    col_hist1, col_hist2 = st.columns([3, 1])
    with col_hist1:
        st.subheader("🏆 過去のベスト設定一覧")
    with col_hist2:
        if len(res_list) > 0:
            if st.button("🗑️ 履歴をすべてリセット", type="secondary", use_container_width=True):
                save_best_results([])
                st.rerun()
                
    if len(res_list) > 0:
        hist_df = pd.DataFrame(res_list)
        
        if '期間' not in hist_df.columns: hist_df['期間'] = "未指定"
        if '指標' not in hist_df.columns: hist_df['指標'] = "未指定"
        if '銘柄' not in hist_df.columns: hist_df['銘柄'] = "未指定"
        
        hist_df['期間'] = hist_df['期間'].fillna("未指定")
        hist_df['指標'] = hist_df['指標'].fillna("未指定")
        hist_df['銘柄'] = hist_df['銘柄'].fillna("未指定")
        
        display_mode = st.radio("表示形式", ["指標ごとに比較", "銘柄ごとに比較"], horizontal=True)
        
        if display_mode == "指標ごとに比較":
            grouped = hist_df.groupby(['期間', '指標'])
            group_keys = ['期間', '指標']
        else:
            grouped = hist_df.groupby(['期間', '銘柄'])
            group_keys = ['期間', '銘柄']
        
        for keys, group_df in grouped:
            if display_mode == "指標ごとに比較":
                st.markdown(f"#### 📅 期間: {keys[0]} ｜ 📈 指標: {keys[1]}")
            else:
                st.markdown(f"#### 📅 期間: {keys[0]} ｜ 🏢 銘柄: {keys[1]}")
                
            # 降順ソート
            group_df = group_df.sort_values("リターン", ascending=False).reset_index(drop=True)
            
            event = st.dataframe(group_df.style.format({
                "リターン": "{:+.2f}%", 
                "最終資産": "¥{:,.0f}",
                "利確%": "{:.1f}%",
                "損切%": "{:.1f}%"
            }), use_container_width=True, on_select="rerun", selection_mode="single-row", key=f"best_res_{keys[0]}_{keys[1]}")
            
            if hasattr(event, "selection") and len(event.selection.rows) > 0:
                selected_idx = event.selection.rows[0]
                selected_row = group_df.iloc[selected_idx]
                show_historical_details(selected_row, watchlists, ticker_to_name)
    else:
        st.info("履歴はまだありません。シミュレーションを実行すると自動で保存されます。")

def show_historical_details(row, watchlists, ticker_to_name):
    st.markdown("---")
    st.markdown(f"### 🔍 選択した設定の詳細結果: {row['銘柄']}")
    
    target_name = row["銘柄"]
    fetch_period = row["期間"]
    trend_type = row["指標"]
    b = int(row["買い≧"])
    s = int(row["売り≦"])
    tp = float(row["利確%"]) / 100.0
    sl = float(row["損切%"]) / 100.0
    
    # attempt to load previous form config for init cap, default 1M
    all_profiles = load_sim_settings()
    current_prof = st.session_state.get("current_sim_profile", "")
    sim_config = all_profiles.get(current_prof, {}) if current_prof in all_profiles else (list(all_profiles.values())[0] if all_profiles else {})
    initial_cap = sim_config.get("initial_cap", 1000000)
    
    opt_mode = "ウォッチリスト全体 (ポートフォリオ)" if target_name.startswith("ポートフォリオ (") else "個別銘柄 (単一)"
    target_codes = []
    
    if opt_mode == "個別銘柄 (単一)":
        code_str = target_name.split(" ")[0]
        target_codes = [code_str]
    else:
        import re
        m = re.search(r'\((.*?)\)', target_name)
        wl_name = m.group(1) if m else target_name
        
        codes_str = watchlists.get(wl_name, "")
        if codes_str:
            target_codes = [c.strip() for c in codes_str.replace('\n', ',').replace(' ', ',').split(',') if c.strip()]
        else:
            st.error("設定されていたウォッチリストが見つからないため、詳細を表示できません。")
            return
            
    df_dict = {}
    with st.spinner("詳細データをローカルキャッシュから復元して計算中..."):
        for tc in target_codes:
            ticker_yf = tc + ".T" if tc.isdigit() else tc
            hist = get_historical_data(ticker_yf)
            if hist.empty or len(hist) < 200: continue
            for c in ['Close', 'High', 'Low', 'Volume']:
                if c in hist.columns: hist[c] = hist[c].ffill().bfill()
                    
            if "V1" in trend_type:
                scored_df = calculate_daily_v1_scores(hist)
            else:
                scored_df = calculate_daily_v2_scores(hist)
                
            now = pd.Timestamp.now(tz=hist.index.tz) if hist.index.tz is not None else datetime.now()
            p_map = {"3mo": pd.DateOffset(months=3), "6mo": pd.DateOffset(months=6), "1y": pd.DateOffset(years=1),
                     "2y": pd.DateOffset(years=2), "3y": pd.DateOffset(years=3), "5y": pd.DateOffset(years=5),
                     "10y": pd.DateOffset(years=10)}
            
            if fetch_period in p_map:
                start_date = now - p_map[fetch_period]
            else:
                start_date = hist.index[200]
                
            test_df = scored_df[scored_df.index >= start_date].copy()
            if len(test_df) > 0:
                df_dict[tc] = test_df
                
        if not df_dict:
            st.error("評価可能なデータ期間がありません。")
            return
            
        if opt_mode == "個別銘柄 (単一)":
            single_tc = list(df_dict.keys())[0]
            single_df = df_dict[single_tc]
            best_final_equity, best_curve, best_trades = run_backtest(single_df, initial_cap, b, s, tp, sl)
            best_all_dates = single_df.index
            bnh_final_equity, bnh_daily_equity, bnh_trades = run_bnh_backtest(single_df, initial_cap)
        else:
            best_final_equity, best_curve, best_trades, best_all_dates = run_portfolio_backtest(df_dict, initial_cap, b, s, tp, sl)
            bnh_final_equity, bnh_daily_equity, bnh_trades, _ = run_portfolio_bnh_backtest(df_dict, initial_cap)
            single_df = None
            
        best_return_pct = round((best_final_equity-initial_cap)/initial_cap*100, 2)
        bnh_return_pct = round((bnh_final_equity-initial_cap)/initial_cap*100, 2)
        diff_pct = best_return_pct - bnh_return_pct
        
        col_res1, col_res2 = st.columns(2)
        with col_res1:
            if opt_mode == "個別銘柄 (単一)" and len(best_trades) > 0 and '買' in best_trades[0]['取引']:
                first_info = f"\\n\\n*(初回購入: {int(best_trades[0]['株数'])}株)*"
            elif opt_mode != "個別銘柄 (単一)" and len(best_trades) > 0:
                first_info = f"\\n\\n*(総取引回数: {len(best_trades)}回)*"
            else:
                first_info = ""
            st.info(f"**🤖 最適化ルール結果:**\\n\\nReturn: **{best_return_pct:+.2f}%**\\n\\n最終資産: ¥{best_final_equity:,.0f}{first_info}")
            
        with col_res2:
            if opt_mode == "個別銘柄 (単一)" and len(bnh_trades) > 0 and '積立買' in bnh_trades[0]['取引']:
                bnh_info = f"\\n\\n*(初回一括購入: {int(bnh_trades[0]['株数'])}株)*"
            elif opt_mode != "個別銘柄 (単一)" and len(bnh_trades) > 0:
                bnh_info = f"\\n\\n*(総取引回数: {len(bnh_trades)}回)*"
            else:
                bnh_info = ""
            st.warning(f"**📦 そのまま持ち続けた場合 (積立・ガチホ):**\\n\\nReturn: **{bnh_return_pct:+.2f}%**\\n\\n最終資産: ¥{bnh_final_equity:,.0f}{bnh_info}")
            
        st.markdown(f"**📊 比較**: 最適化ルールの方が、ナンピン・ガチホよりも **{diff_pct:+.2f}%** 最終成績が良いです！")
        
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots

        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Scatter(x=best_all_dates, y=best_curve, name="最適化ルール (左軸:資産)", line=dict(color="#00AEEF", width=2)), secondary_y=False)
        fig.add_trace(go.Scatter(x=best_all_dates, y=bnh_daily_equity, name="積立ガチホ (左軸:資産)", line=dict(color="#FF9900", width=2, dash='dot')), secondary_y=False)

        if opt_mode == "個別銘柄 (単一)" and single_df is not None:
            fig.add_trace(go.Scatter(x=single_df.index, y=single_df['Close'], name="実際の株価 (右軸:株価)", line=dict(color="rgba(255, 255, 255, 0.3)", width=1.5), fill='tozeroy', fillcolor='rgba(255, 255, 255, 0.05)'), secondary_y=True)

        fig.update_layout(title="📈 資産推移と実際の株価の比較", xaxis_title="日付", hovermode="x unified", margin=dict(l=0, r=0, t=50, b=0), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        fig.update_yaxes(title_text="資産額 (円)", secondary_y=False, tickformat=",")
        fig.update_yaxes(title_text="株価 (円)", secondary_y=True, showgrid=False, tickformat=",")
        st.plotly_chart(fig, use_container_width=True)
        
        tab_opt, tab_bnh = st.tabs(["📜 最適化ルールの売買履歴", "📥 ナンピン・ガチホ側の追加購入履歴"])
        with tab_opt:
            if best_trades:
                tdf = pd.DataFrame(best_trades)
                tdf['日付'] = tdf['日付'].dt.strftime('%Y-%m-%d')
                if '銘柄' in tdf.columns: tdf['銘柄'] = tdf['銘柄'].apply(lambda x: f"{x} {ticker_to_name.get(x, '')}".strip())
                if '損益(%)' in tdf.columns: tdf['損益(%)'] = tdf['損益(%)'].apply(lambda x: f"{x:+.1f}%" if x != 0 else "-")
                st.dataframe(tdf, use_container_width=True)
            else:
                st.info("取引は発生しませんでした。")
                
        with tab_bnh:
            if bnh_trades:
                bdf = pd.DataFrame(bnh_trades)
                bdf['日付'] = bdf['日付'].dt.strftime('%Y-%m-%d')
                if '銘柄' in bdf.columns: bdf['銘柄'] = bdf['銘柄'].apply(lambda x: f"{x} {ticker_to_name.get(x, '')}".strip())
                if '損益(%)' in bdf.columns: bdf['損益(%)'] = bdf['損益(%)'].apply(lambda x: f"{x:+.1f}%" if pd.notnull(x) and x != 0 else "-")
                if '消費資金' in bdf.columns: bdf['消費資金'] = bdf['消費資金'].apply(lambda x: f"¥{x:,.0f}" if pd.notnull(x) else "-")
                if '残現金' in bdf.columns: bdf['残現金'] = bdf['残現金'].apply(lambda x: f"¥{x:,.0f}" if pd.notnull(x) else "-")
                st.dataframe(bdf.fillna("-"), use_container_width=True)
            else:
                st.info("取引は発生しませんでした。")
