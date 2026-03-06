import yfinance as yf
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_stock_data(symbol: str) -> pd.DataFrame:
    """
    Fetches the last 6 months of daily stock data using yfinance.
    Automatically appends '.NS' for Indian stocks if not provided.
    """
    # 5. Automatically append ".NS"
    symbol = symbol.strip().upper()
    if not symbol.endswith(".NS"):
        symbol += ".NS"
        
    logging.info(f"Fetching 6-month daily stock data for symbol: {symbol}")
    
    try:
        # 1. Use yfinance library
        ticker = yf.Ticker(symbol)
        
        # 2 & 3. Fetch last 6 months of daily data
        df = ticker.history(period="6mo", interval="1d")
        
        # 6. Validate symbol: If no data returned
        if df.empty:
            logging.error(f"No data returned for symbol: {symbol}")
            raise ValueError("Invalid NSE symbol or no data found.")
            
        # 7. Reset index so 'date' becomes a column
        df = df.reset_index()
        
        # 8 & 10. Ensure lowercase columns and proper types
        # yfinance columns are usually ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits']
        df.rename(columns={
            'Date': 'date',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume'
        }, inplace=True)
        
        # Convert date to datetime (yfinance dates are usually timezone-aware, making them naive for SQLite)
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
        
        # 9. Remove unnecessary columns
        expected_cols = ['date', 'open', 'high', 'low', 'close', 'volume']
        df = df[expected_cols]
        
        # Ensure numeric columns are float
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_cols:
            df[col] = df[col].astype(float)
            
        logging.info("Successfully fetched and formatted stock data.")
        return df
        
    except Exception as e:
        logging.error(f"Error fetching data for {symbol}: {e}")
        raise

if __name__ == "__main__":
    # Quick test
    try:
        sample_df = fetch_stock_data("RELIANCE")
        print("\nTest Extracted Data:")
        print(sample_df.head())
        print("\nData Types:")
        print(sample_df.dtypes)
    except Exception as e:
        print(f"Failed to fetch data module test: {e}")
