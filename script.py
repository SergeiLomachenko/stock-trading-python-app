import requests 
import os 
import csv 
import time 
from dotenv import load_dotenv 
load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

print(POLYGON_API_KEY)

LIMIT = 1000

url = f'https://api.massive.com/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}'

response = requests.get(url)

tickers = []

data = response.json()

data_example = {
    'ticker': 'A', 
    'name': 'Agilent Technologies Inc.', 
    'market': 'stocks', 
    'locale': 'us', 
    'primary_exchange': 'XNYS', 
    'type': 'CS', 'active': True, 
    'currency_name': 'usd', 
    'cik': '0001090872', 
    'composite_figi': 'BBG000C2V3D6', 
    'share_class_figi': 'BBG001SCTQY4', 
    'last_updated_utc': '2025-10-31T06:15:40.047781683Z'}

for i in data['results']:
    tickers.append(i)

while 'next_url' in data: 
    print('requesting next page', data['next_url'])
    response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
    data = response.json()
    if 'results' in data:
        for i in data['results']:
            tickers.append(i)
    else:
        print("key 'results' not found in response :", data)
        break
    time.sleep(5)

print(len(tickers))

filename = "ticker_data.csv"

fields = data_example.keys() 

with open(filename, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
    writer.writeheader()
    for row in tickers:        
        writer.writerow(row)

print(f"Data saved to {filename}, rows: {len(tickers)}")
