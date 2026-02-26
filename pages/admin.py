import streamlit as st
import json
import os

st.set_page_config(page_title="管理画面", layout="wide")

st.title("⚙️ 管理画面")

# --- 自動取得スケジュール ---
st.header("⏰ 自動取得スケジュール")

st.info("""
**現在のスケジュール（GitHub Actions）**

| 時刻（JST） | 曜日 | 状態 |
|---|---|---|
| 9:00 | 月〜金 | ✅ 有効 |
| 12:00 | 月〜金 | ✅ 有効 |
| 15:30 | 月〜金 | ✅ 有効 |

スケジュールを変更する場合は、GitHubリポジトリの  
`.github/workflows/auto_fetch.yml` を編集してください。

**手動で即座に取得する方法**: GitHubリポジトリ → Actions → 「自動データ取得」 → 「Run workflow」ボタン
""")

st.markdown("---")

# --- 取得ログ ---
st.header("📋 取得ログ")

LAST_DATA_FILE = "last_stock_data.json"
BUY_TIMING_HISTORY_FILE = "buy_timing_history.json"

if os.path.exists(LAST_DATA_FILE):
    try:
        with open(LAST_DATA_FILE, "r", encoding="utf-8") as f:
            all_saved = json.load(f)
        
        for wl_name, wl_data in all_saved.items():
            if isinstance(wl_data, dict):
                fetch_time = wl_data.get("fetch_time", "不明")
                data = wl_data.get("data", [])
                st.success(f"📂 **{wl_name}** — 最終取得: {fetch_time} （{len(data)}銘柄）")
    except Exception:
        st.warning("データファイルの読み込みに失敗しました")
else:
    st.warning("まだデータが取得されていません")

st.markdown("---")

# --- 履歴統計 ---
st.header("📊 取得履歴の統計")

if os.path.exists(BUY_TIMING_HISTORY_FILE):
    try:
        with open(BUY_TIMING_HISTORY_FILE, "r", encoding="utf-8") as f:
            history = json.load(f)
        
        for wl_name, entries in history.items():
            st.metric(f"📂 {wl_name}", f"{len(entries)} 件の取得履歴")
            if entries:
                st.caption(f"最初の取得: {entries[0]['time']}　→　最新の取得: {entries[-1]['time']}")
    except Exception:
        st.warning("履歴ファイルの読み込みに失敗しました")
else:
    st.info("まだ取得履歴がありません")

st.markdown("---")

# --- データファイルの管理 ---
st.header("🗄️ データファイル")

col1, col2 = st.columns(2)

with col1:
    if os.path.exists(LAST_DATA_FILE):
        size = os.path.getsize(LAST_DATA_FILE)
        st.metric("last_stock_data.json", f"{size / 1024:.1f} KB")
    else:
        st.metric("last_stock_data.json", "未作成")

with col2:
    if os.path.exists(BUY_TIMING_HISTORY_FILE):
        size = os.path.getsize(BUY_TIMING_HISTORY_FILE)
        st.metric("buy_timing_history.json", f"{size / 1024:.1f} KB")
    else:
        st.metric("buy_timing_history.json", "未作成")

st.markdown("---")

# --- 環境情報 ---
with st.expander("🔧 環境情報"):
    jq_key = os.environ.get("JQUANTS_API_KEY", "")
    try:
        jq_secret = st.secrets.get("JQUANTS_API_KEY", "")
    except Exception:
        jq_secret = ""
    
    if jq_key or jq_secret:
        st.success("✅ J-Quants APIキーが設定されています")
    else:
        if os.path.exists("jquants_token.txt"):
            st.success("✅ J-Quants APIキーがローカルファイルに設定されています")
        else:
            st.warning("⚠️ J-Quants APIキーが未設定です")
    
    st.markdown("""
    **APIキーの設定場所（優先順）:**
    1. Streamlit Cloud → Settings → Secrets → `JQUANTS_API_KEY = "your_key"`
    2. 環境変数 `JQUANTS_API_KEY`
    3. ローカルファイル `jquants_token.txt`
    """)
