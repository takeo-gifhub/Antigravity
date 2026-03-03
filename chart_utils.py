"""
SVGチャート生成の共通ユーティリティ
"""


def generate_price_chart_svg(close_data, width=80, height=24):
    """株価推移のミニチャートSVGを生成する"""
    try:
        recent = list(close_data)
        if len(recent) < 2:
            return ""
        mn, mx = min(recent), max(recent)
        rng = mx - mn if mx != mn else 1
        points = []
        for j, v in enumerate(recent):
            x = j / (len(recent) - 1) * width
            y = height - (v - mn) / rng * height
            points.append(f"{x:.1f},{y:.1f}")
        color = "#4caf50" if recent[-1] >= recent[0] else "#ff5252"
        return f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg"><polyline points="{" ".join(points)}" fill="none" stroke="{color}" stroke-width="1.5"/></svg>'
    except Exception:
        return ""


def generate_score_trend_svg(hist, score_func, width=80, height=24, num_points=20):
    """スコア推移のトレンドチャートSVGを生成する

    Args:
        hist: yfinanceの履歴DataFrame
        score_func: calculate_buy_timing_score_vN (raw=True で呼ぶ)
        width: SVG幅
        height: SVG高さ
        num_points: 計算するポイント数
    """
    try:
        if len(hist) < num_points * 2:
            return ""

        scores = []
        for i in range(num_points, 0, -1):
            sub_hist = hist if i == 1 else hist.iloc[:-i + 1]
            score, _ = score_func(sub_hist, raw=True)
            scores.append(score if score is not None and score != "-" else 0)

        min_score, max_score = 0, 100
        svg_pts = []
        for idx, val in enumerate(scores):
            x = idx * (width / (num_points - 1))
            y = height - ((val - min_score) / (max_score - min_score) * height)
            svg_pts.append(f"{x:.1f},{y:.1f}")

        color = "#ff5252" if scores[-1] < scores[0] else "#4caf50"
        pts_str = " ".join(svg_pts)
        line_50 = f'<line x1="0" y1="{height / 2}" x2="{width}" y2="{height / 2}" stroke="#666666" stroke-width="1" stroke-dasharray="2,2"/>'
        return f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">{line_50}<polyline points="{pts_str}" fill="none" stroke="{color}" stroke-width="1.5"/></svg>'
    except Exception:
        return ""
