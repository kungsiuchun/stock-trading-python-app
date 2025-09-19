import requests
import csv
import time
import os
from dotenv import load_dotenv
load_dotenv()

# Variable
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
LIMIT = 1000

url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}'

response = requests.get(url)
tickers = []

data = response.json()

# print(data)
# print(data.keys())
# print(data['next_url'])
for ticker in data['results']:
    tickers.append(ticker)

# for next page result
while 'next_url' in data:
    print('requesting next page', data['next_url'])
    
    # adding time 15s stop here
    time.sleep(12)

    response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
    data = response.json()
    print(data)
    for ticker in data['results']:
        tickers.append(ticker)
        
example_ticker = \
{
    'ticker': 'PEB', 
    'name': 'Pebblebrook Hotel Trust', 
    'market': 'stocks', 'locale': 'us', 
    'primary_exchange': 'XNYS', 
    'type': 'CS', 
    'active': True, 
    'currency_name': 'usd', 
    'cik': '0001474098', 
    'composite_figi': 'BBG000PNBZF5', 
    'share_class_figi': 'BBG001T5S1M7', 
    'last_updated_utc': '2025-09-18T06:05:34.657589804Z'
}

# Write tickers to CSV with example_ticker schema
fieldnames = list(example_ticker.keys())
output_csv = 'tickers.csv'

with open(output_csv, mode = 'w', newline='', encoding='UTF-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    
    for t in tickers:
        row = {key: t.get(key, '') for key in fieldnames}
        writer.writerow(row)
    
    print(f'Wrote {len(tickers)} rows to {output_csv}')
