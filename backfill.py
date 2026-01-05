import requests
import csv
import time
import os
from datetime import datetime, timedelta

# --- 設定 ---
START_DATE = datetime(2025, 3, 22)
END_DATE = datetime(2026, 1, 3)
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

DOW_30 = {
    "AAPL", "AMGN", "AMZN", "AXP", "BA", "CAT", "CRM", "CSCO", "CVX", "DIS", 
    "GS", "HD", "HON", "IBM", "INTC", "JNJ", "JPM", "KO", "MCD", "MMM", 
    "MRK", "MSFT", "NKE", "NVDA", "PG", "SHW", "TRV", "UNH", "V", "VZ", "WMT"
}

fieldnames = ['date', 'T', 'c', 'h', 'l', 'n', 'o', 't', 'v', 'vw']
output_csv = 'dow30.csv'

def is_trading_day(dt):
    """檢查是否為週一至週五 (0=週一, 5=週六, 6=週日)"""
    return dt.weekday() < 5

def fetch_daily_data(current_date):
    date_str = current_date.strftime('%Y-%m-%d')
    url = f'hhttps://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/{date_str}?adjusted=true&apiKey={POLYGON_API_KEY}'
    
    
    try:
        response = requests.get(url)
        # 處理 429 頻率限制
        if response.status_code == 429:
            print("達到 API 頻率限制，等待 60 秒...")
            time.sleep(60)
            return fetch_daily_data(current_date)
            
        data = response.json()
        if "results" in data:
            return [t for t in data['results'] if t.get('T') in DOW_30], date_str
        else:
            return [], date_str
    except Exception as e:
        print(f"{date_str}: 請求失敗 - {e}")
        return [], date_str

# --- 開始執行 ---
file_exists = os.path.isfile(output_csv)

with open(output_csv, mode='a', newline='', encoding='UTF-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    if not file_exists:
        writer.writeheader()

    current = START_DATE
    while current <= END_DATE:
        # 核心優化：如果不是交易日，直接跳過 API 調用
        if not is_trading_day(current):
            print(f"{current.strftime('%Y-%m-%d')}: 跳過 (週末)")
            current += timedelta(days=1)
            continue

        results, date_str = fetch_daily_data(current)
        
        if results:
            for t in results:
                row = {key: t.get(key, '') for key in fieldnames}
                row['date'] = date_str
                writer.writerow(row)
            print(f"成功補回 {date_str}，共 {len(results)} 筆數據")
            # 只有真正調用 API 時才需要等待 (避免觸發 429 限制)
            time.sleep(13) 
        else:
            print(f"{date_str}: API 無結果 (可能是股市假日)")
            # 沒數據但調用了 API，仍建議稍微等待
            time.sleep(2)

        current += timedelta(days=1)

print("--- 補數據任務完成！ ---")