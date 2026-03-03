"""
買い時率スコア計算の共通モジュール
app.py と auto_fetch.py で共有される統一ロジック
"""
import numpy as np
import pandas as pd


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
            if rsi <= 30: score += 10
            elif rsi <= 50: score += 7
            elif rsi <= 70: score += 3

        # 7. ボリンジャーバンド - 割安度判定 (Max 10)
        max_score += 10
        if len(hist) >= 20:
            sma20 = hist['Close'].rolling(20).mean().iloc[-1]
            std20 = hist['Close'].rolling(20).std().iloc[-1]
            lower_band = sma20 - 2 * std20
            if c_price <= lower_band: score += 10
            elif c_price <= sma20: score += 5

        # スコアを100点満点に正規化
        normalized = int(score / max_score * 100) if max_score > 0 else 0
        return (normalized, c_price) if raw else (_score_to_label(normalized), c_price)
    except Exception:
        return ("-", None) if not raw else (0, None)


def calculate_buy_timing_score_v3(hist, raw=False):
    """V3: マルチタイム・ボラティリティ適応・過熱感排除型の高度ロジック (最大100点)"""
    try:
        if hist.empty or len(hist) < 30:
            return ("-", None) if not raw else (0, None)

        c_price = float(hist['Close'].iloc[-1])
        score = 0

        # 準備: 基本指標の計算
        close_series = hist['Close']
        high_series = hist['High']
        low_series = hist['Low']
        vol_series = hist['Volume']

        # --- 1. 過熱感フィルター (RSI) ---
        delta = close_series.diff()
        gain = delta.where(delta > 0, 0).rolling(14).mean().iloc[-1]
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean().iloc[-1]
        rsi = 100 - (100 / (1 + gain / loss)) if loss != 0 else 100

        if rsi >= 80:
            return (0, c_price) if raw else (_score_to_label(0), c_price)

        # --- 2. マルチタイムフレーム分析 [配分: 20点] ---
        if len(hist) >= 100:
            sma20 = close_series.rolling(20).mean().iloc[-1]
            sma60 = close_series.rolling(60).mean().iloc[-1]
            if sma20 > sma60:
                score += 20
            elif c_price > sma60:
                score += 10
        else:
            score += 10

        # --- 3. ボラティリティ適応型押し目判定 (ATR考慮) [配分: 25点] ---
        atr_period = 14
        tr1 = high_series - low_series
        tr2 = (high_series - close_series.shift()).abs()
        tr3 = (low_series - close_series.shift()).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(atr_period).mean().iloc[-1]

        recent_high = high_series.iloc[-10:].max()
        drawdown = recent_high - c_price

        if atr > 0:
            dd_atr_ratio = drawdown / atr
            if dd_atr_ratio >= 1.5:
                score += 25
            elif dd_atr_ratio >= 0.5:
                score += 15
            else:
                score += 5

        # --- 4. RVOL (相対出来高) [配分: 15点] ---
        vol = vol_series.iloc[-1]
        avg_vol = vol_series.rolling(30).mean().iloc[-2] if len(hist) > 30 else 0
        if avg_vol > 0:
            rvol = vol / avg_vol
            if rvol >= 3.0:
                score += 15
            elif rvol >= 1.5:
                score += 10
            elif rvol >= 0.8:
                score += 5

        # --- 5. 簡易VPVR [配分: 25点] ---
        hist_60 = hist.tail(60)
        min_p = hist_60['Low'].min()
        max_p = hist_60['High'].max()
        if max_p > min_p:
            bins = np.linspace(min_p, max_p, 11)
            vol_profile = np.zeros(10)

            for i in range(len(hist_60)):
                h_price = hist_60['Close'].iloc[i]
                h_vol = hist_60['Volume'].iloc[i]
                idx = np.digitize(h_price, bins) - 1
                idx = min(max(idx, 0), 9)
                vol_profile[idx] += h_vol

            poc_idx = np.argmax(vol_profile)
            poc_price_low = bins[poc_idx]
            poc_price_high = bins[poc_idx + 1]
            poc_center = (poc_price_low + poc_price_high) / 2

            pct_from_poc = (c_price - poc_center) / poc_center
            if 0 <= pct_from_poc <= 0.05:
                score += 25
            elif 0.05 < pct_from_poc <= 0.15:
                score += 15
            elif pct_from_poc < 0:
                score += 5

        # --- 6. 短期モメンタム (MACD) [配分: 15点] ---
        ema12 = close_series.ewm(span=12, adjust=False).mean()
        ema26 = close_series.ewm(span=26, adjust=False).mean()
        macd_line = ema12 - ema26
        macd_sig = macd_line.ewm(span=9, adjust=False).mean()
        macd_hist = macd_line - macd_sig

        if macd_line.iloc[-1] > 0 and macd_hist.iloc[-1] > 0:
            score += 15
        elif macd_hist.iloc[-1] > 0:
            score += 10

        normalized = min(int(score), 100)
        return (normalized, c_price) if raw else (_score_to_label(normalized), c_price)

    except Exception:
        return ("-", None) if not raw else (0, None)


def calculate_buy_timing_score_v4(hist, raw=False):
    """V4: 環境認識ハイブリッド型 (V2とV3の自動切り替え)"""
    try:
        if hist.empty or len(hist) < 60:
            return ("-", None) if not raw else (0, None)

        c_price = float(hist['Close'].iloc[-1])

        # --- 環境判定 ---
        period = 200 if len(hist) >= 200 else 60
        sma_long = hist['Close'].rolling(period).mean().iloc[-1]

        # ボラティリティ (ATR%を計算)
        high_series = hist['High']
        low_series = hist['Low']
        close_series = hist['Close']
        tr1 = high_series - low_series
        tr2 = (high_series - close_series.shift()).abs()
        tr3 = (low_series - close_series.shift()).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(14).mean().iloc[-1]
        atr_pct = atr / c_price if c_price > 0 else 0

        is_strong_trend = (c_price > sma_long) and (atr_pct >= 0.01)

        if is_strong_trend:
            return calculate_buy_timing_score_v2(hist, raw=raw)
        else:
            return calculate_buy_timing_score_v3(hist, raw=raw)

    except Exception:
        return ("-", None) if not raw else (0, None)
