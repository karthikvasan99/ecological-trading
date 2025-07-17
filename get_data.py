import requests
import pandas as pd
import yfinance as yf
from bs4 import BeautifulSoup
from time import sleep
from tqdm import tqdm

# Step 1: Scrape S&P 100 tickers from Wikipedia
def get_sp100_tickers():
    url = "https://en.wikipedia.org/wiki/S%26P_100"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", {"id": "constituents"})
    df = pd.read_html(str(table))[0]
    tickers = df["Symbol"].tolist()

    # Fix tickers with dots (e.g., BRK.B → BRK-B for Yahoo Finance)
    tickers = [t.replace(".", "-") for t in tickers]
    return tickers

# Step 2: Download historical data
def download_data(tickers, start="2015-01-01", end="2025-01-01"):
    all_data = {}
    for ticker in tqdm(tickers):
        try:
            df = yf.download(ticker, start=start, end=end, interval="1d", auto_adjust=True)
            df = df[["Close"]].rename(columns={"Close": ticker})
            all_data[ticker] = df
            sleep(1)  # Be polite to avoid rate limits
        except Exception as e:
            print(f"Failed for {ticker}: {e}")
    return all_data

# Step 3: Align and combine all tickers into one DataFrame
def combine_data(all_data):
    return pd.concat(all_data.values(), axis=1).dropna(how='all')

# Main execution
if __name__ == "__main__":
    tickers = get_sp100_tickers()
    print(f"Found {len(tickers)} tickers")

    stock_data = download_data(tickers)
    combined = combine_data(stock_data)

    combined.to_csv("sp100_daily_close.csv")
    print("Saved data to sp100_daily_close.csv")
