import pandas as pd
import requests
from io import StringIO
import urllib3

# 關閉 SSL 警告（只影響顯示，不影響功能）
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# -------------------------
# 資料前處理
# -------------------------
df = pd.read_html("data.xls")[0]
df['成交日期'] = pd.to_datetime(df['成交日期'], errors='coerce')
df['成交價'] = pd.to_numeric(df['成交價'], errors='coerce')

# -------------------------
# TWSE 價格快取
# -------------------------
price_cache = {}

def parse_twse_date(date_str):
    try:
        parts = date_str.split('/')
        year = int(parts[0]) + 1911  # 民國年轉西元
        month = int(parts[1])
        day = int(parts[2])
        return pd.Timestamp(year, month, day)
    except:
        return pd.NaT

def get_avg_price_1_to_10(stock_no: str, year: int, month: int):
    """
    使用 TWSE 官方 CSV API
    回傳指定股票該年月 1~10 號的平均收盤價
    """
    cache_key = (stock_no, year, month)
    if cache_key in price_cache:
        return price_cache[cache_key]

    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
    params = {
        "response": "csv",
        "date": f"{year}{month:02d}01",
        "stockNo": stock_no
    }

    try:
        r = requests.get(
            url,
            params=params,
            timeout=10,
            verify=False,           # ⭐ 關鍵：避開 TWSE SSL 問題
            headers={"User-Agent": "Mozilla/5.0"}
        )
        r.raise_for_status()
    except Exception as e:
        print(f"TWSE 讀取失敗: {stock_no} {year}-{month:02d} → {e}")
        price_cache[cache_key] = None
        return None

    df_twse = pd.read_csv(StringIO(r.text), header=1)
    df_twse['日期'] = df_twse['日期'].apply(parse_twse_date)
    df_twse['收盤價'] = (df_twse['收盤價'].str.replace(',', '', regex=False).astype(float))
    avg_price = df_twse.loc[
        df_twse['日期'].dt.day.between(1, 10),
        '收盤價'
    ].mean()

    price_cache[cache_key] = avg_price
    return avg_price

# -------------------------
# 補值主流程
# -------------------------
mask = df['成交價'] == 0

for idx, row in df.loc[mask].iterrows():
    trade_date = row['成交日期']
    if pd.isna(trade_date):
        continue

    avg_price = get_avg_price_1_to_10(
        stock_no="2330",   # 台積電
        year=trade_date.year,
        month=trade_date.month
    )

    if pd.notna(avg_price):
        df.at[idx, '成交價'] = round(avg_price, 2)

# -------------------------
# 檢查結果
# -------------------------
print(df.loc[mask, ['成交日期', '成交價']])
