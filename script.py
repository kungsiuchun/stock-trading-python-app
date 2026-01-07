import requests
import csv
import os
from datetime import datetime, timedelta

# 1. 自動獲取日期 (預設抓取昨天)
target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

# 2. 定義 Dow 30 名單
DOW_30 = {
    "AAPL", "AMGN", "AMZN", "AXP", "BA", "CAT", "CRM", "CSCO", "CVX", "DIS", 
    "GS", "HD", "HON", "IBM", "INTC", "JNJ", "JPM", "KO", "MCD", "MMM", 
    "MRK", "MSFT", "NKE", "NVDA", "PG", "SHW", "TRV", "UNH", "V", "VZ", "WMT"
}

output_csv = 'dow30.csv'

# --- 檢查日期是否重複 ---
if os.path.exists(output_csv):
    with open(output_csv, mode='r', encoding='UTF-8') as f:
        # 讀取最後幾行或整個檔案內容來檢查日期
        existing_data = f.read()
        if target_date in existing_data:
            print(f"跳過執行：{target_date} 的數據已經存在於 {output_csv} 中。")
            exit() # 直接結束程式，不調用 API

# --- 如果日期不重複，則調用 API ---
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
url = f'https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/{target_date}?adjusted=true&apiKey={POLYGON_API_KEY}'

try:
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    if "results" not in data:
        print(f"日期 {target_date} 沒有找到數據 (可能是週末或假期)")
        exit()

    filtered_tickers = [t for t in data['results'] if t.get('T') in DOW_30]

    # 3. 準備寫入 CSV
    fieldnames = ['date', 'T', 'c', 'h', 'l', 'n', 'o', 't', 'v', 'vw']
    file_exists = os.path.isfile(output_csv)

    # 使用 'a' (append) 模式附加數據
    with open(output_csv, mode='a', newline='', encoding='UTF-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        # 如果檔案是新建立的，才寫入標題 (Header)
        if not file_exists:
            writer.writeheader()
        
        for t in filtered_tickers:
            row = {key: t.get(key, '') for key in fieldnames}
            row['date'] = target_date 
            writer.writerow(row)
    
    print(f'成功抓取 {target_date} 數據，並附加至 {output_csv}')

except Exception as e:
    print(f"執行出錯: {e}")
