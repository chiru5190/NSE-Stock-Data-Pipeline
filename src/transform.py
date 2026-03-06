import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def clean_stock_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes a pandas DataFrame directly from yfinance extract.py and adds derived columns.
    Assumes standard columns: date, open, high, low, close, volume exist.
    """
    if df is None or df.empty:
        logging.error("Input DataFrame is empty or None.")
        raise ValueError("No data found to transform.")

    logging.info("Starting data transformation...")
    
    # Defensive check for expected columns
    expected_cols = ['date', 'open', 'high', 'low', 'close', 'volume']
    if not all(col in df.columns for col in expected_cols):
        missing = [col for col in expected_cols if col not in df.columns]
        raise ValueError(f"Missing expected columns from yfinance data: {missing}")
        
    # Data from yfinance is already sorted and typed mostly, but we add derived columns
    
    # 1. Add daily_price_change
    df['daily_price_change'] = df['close'] - df['open']
    
    # 2. Add percentage_change 
    # Avoid division by zero if 'open' is exactly 0.0
    df['percentage_change'] = (df['daily_price_change'] / df['open'].replace(0, pd.NA)) * 100
    df['percentage_change'] = df['percentage_change'].fillna(0.0)

    # Note: Sorting is generally guaranteed chronologically from yfinance history,
    # but defensive sorting is good practice.
    df = df.sort_values(by='date', ascending=True)
    df = df.reset_index(drop=True)

    logging.info(f"Successfully added derived columns. Dimensions: {df.shape}")
    return df

if __name__ == "__main__":
    # Test block
    sample_df = pd.DataFrame({
        'date': pd.to_datetime(['2023-10-01', '2023-10-02']),
        'open': [100.0, 105.0],
        'high': [105.0, 110.0],
        'low': [99.0, 101.0],
        'close': [104.0, 108.0],
        'volume': [1000, 1200]
    })
    
    try:
        transformed_df = clean_stock_data(sample_df)
        print("\nTransformed DataFrame:")
        print(transformed_df)
    except Exception as e:
        print(f"Test failed: {e}")
