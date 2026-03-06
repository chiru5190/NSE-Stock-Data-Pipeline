import streamlit as st
import pandas as pd
import sqlite3
import yfinance as yf
import plotly.express as px
from src.extract import fetch_stock_data
from src.transform import clean_stock_data
from src.load import setup_database, load_data_to_db

st.set_page_config(page_title="NSE Stock Analytics Dashboard", page_icon="📈", layout="wide")

def load_data_for_symbol(symbol: str, db_path: str = "stock_data.db") -> pd.DataFrame:
    """
    Connects to SQLite and loads daily_prices using parameterized query WHERE symbol = ?
    """
    try:
        setup_database(db_path)
        conn = sqlite3.connect(db_path)
        query = "SELECT * FROM daily_prices WHERE symbol = ?"
        df = pd.read_sql_query(query, conn, params=(symbol,))
        conn.close()
        
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
            
        return df
    except sqlite3.Error as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()

def run_etl_pipeline(symbol: str):
    """
    Triggers extract -> transform -> load for a specified symbol with loading spinner.
    """
    db_path = "stock_data.db"
    table_name = "daily_prices"
    
    with st.spinner(f"Extracting, Transforming, and Loading latest data for {symbol}..."):
        try:
            # Extract
            raw_df = fetch_stock_data(symbol)
            # Transform
            clean_df = clean_stock_data(raw_df)
            # Load
            setup_database(db_path)
            load_data_to_db(clean_df, db_path, table_name, symbol)
            
            st.success(f"Successfully updated local database with latest 6 months of {symbol} data!")
        except Exception as e:
            st.error(f"Pipeline Failed: {e}")

@st.cache_data(ttl=86400)
def get_nse_symbols():
    """
    Downloads the master list of NSE symbols dynamically.
    Caches the list for 24 hours.
    """
    url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
    try:
        storage_options = {'User-Agent': 'Mozilla/5.0'}
        df = pd.read_csv(url, storage_options=storage_options)
        
        options = {}
        for _, row in df.iterrows():
            sym = str(row['SYMBOL']).strip()
            name = str(row['NAME OF COMPANY']).strip()
            options[sym] = f"{sym}.NS ({name})"
            
        return options
    except Exception as e:
        fallback = {
            "RELIANCE": "RELIANCE.NS (Reliance Industries)", "TCS": "TCS.NS (Tata Consultancy Services)",
            "HDFCBANK": "HDFCBANK.NS (HDFC Bank)", "INFY": "INFY.NS (Infosys)",
            "ICICIBANK": "ICICIBANK.NS (ICICI Bank)", "HUL": "HINDUNILVR.NS (Hindustan Unilever)",
            "ITC": "ITC.NS (ITC Limited)", "SBIN": "SBIN.NS (State Bank of India)",
            "BHARTIARTL": "BHARTIARTL.NS (Bharti Airtel)", "BAJFINANCE": "BAJFINANCE.NS (Bajaj Finance)"
        }
        return fallback

def format_inr(value):
    """Formats float into a traditional INR string with commas: ₹1,23,456.78"""
    try:
        # Handling the Indian numbering system cleanly
        s, *d = str(round(value, 2)).partition(".")
        r = ",".join([s[x-2:x] for x in range(-3, -len(s), -2)][::-1] + [s[-3:]])
        return f"₹ {r}{d[0]}{d[1].ljust(2, '0')}"
    except Exception:
        return f"₹ {value:,.2f}"

def main():
    # Header Section
    st.title("📈 NSE Stock Analytics Dashboard")
    st.markdown("#### 6-Month Performance Overview")
    st.divider()
    
    # Selection Controls
    stock_dict = get_nse_symbols()
    options = list(stock_dict.keys())
    
    col1, col2 = st.columns([3, 1])
    with col1:
        raw_symbol = st.selectbox(
            "Search or select an NSE Stock Symbol:", 
            options=options,
            index=options.index("RELIANCE") if "RELIANCE" in options else 0,
            format_func=lambda x: stock_dict.get(x, x)
        )
        
    symbol = raw_symbol.upper().strip()
    if not symbol.endswith(".NS"):
        symbol += ".NS"

    with col2:
        st.write("")
        st.write("")
        if st.button("Fetch Latest Data", use_container_width=True, type="primary"):
            valid_symbol = False
            with st.spinner(f"Validating symbol {symbol}..."):
                ticker = yf.Ticker(symbol)
                test_df = ticker.history(period="1d")
                
                if test_df.empty:
                    st.error(f"Invalid symbol '{symbol}' or no data available. Please try another symbol.")
                else:
                    valid_symbol = True
                    
            if valid_symbol:
                run_etl_pipeline(symbol)
                st.rerun() 
                
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Main Dashboard Area
    display_df = load_data_for_symbol(symbol)
    
    if display_df.empty:
        st.warning(f"No local data found for symbol: **{symbol}**. Click 'Fetch Latest Data' to extract 6 months of historical data.")
        return
        
    try:
        ticker_info = yf.Ticker(symbol).info
        long_name = ticker_info.get('longName', symbol)
        st.subheader(f"Data Overview: {long_name} ({symbol})")
    except Exception:
        st.subheader(f"Data Overview: {symbol}")
        
    st.write("")
    
    # Group KPI metrics inside a container box
    with st.container(border=True):
        avg_close = display_df['close'].mean()
        max_close = display_df['close'].max()
        total_records = len(display_df)
        
        metric_cols = st.columns(3)
        with metric_cols[0]:
            st.metric("Average Close Price", format_inr(avg_close))
        with metric_cols[1]:
            st.metric("Max Close Price", format_inr(max_close))
        with metric_cols[2]:
            st.metric("Total Records", f"{total_records} Trading Days")
        
    st.divider()

    # Plotly Line Chart for better UX
    st.subheader("Closing Price Over Time")
    fig = px.line(
        display_df.sort_values(by="date"), 
        x="date", 
        y="close", 
        labels={"close": "Closing Price (₹)", "date": "Date"},
        template="plotly_dark",
        hover_data={"date": "|%B %d, %Y"}
    )
    
    fig.update_layout(
        xaxis_title=None,
        margin=dict(l=0, r=0, t=30, b=0),
        hovermode="x unified"
    )
    fig.update_traces(line=dict(width=2.5, color='#00a1f1'))
    st.plotly_chart(fig, use_container_width=True)
    
    st.divider()

    # Top 5 Percentage Change Table Styling
    st.subheader("Top 5 Highest Growth Days")
    if 'percentage_change' in display_df.columns:
        top_5_change = display_df.nlargest(5, 'percentage_change')[['date', 'open', 'close', 'daily_price_change', 'percentage_change']].copy()
        
        # Clean formatting
        top_5_change['date'] = top_5_change['date'].dt.strftime('%Y-%m-%d')
        top_5_change['open'] = top_5_change['open'].apply(format_inr)
        top_5_change['close'] = top_5_change['close'].apply(format_inr)
        
        # Using lambda explicitly instead of simple map to respect signage cleanly
        top_5_change['daily_price_change'] = top_5_change['daily_price_change'].apply(lambda x: format_inr(x) if x >= 0 else f"-{format_inr(abs(x))}")
        top_5_change['percentage_change'] = top_5_change['percentage_change'].apply(lambda x: f"{x:+.2f}%")
        
        # Rename columns for the end-user rendering
        top_5_change.columns = ["Date", "Open Price", "Close Price", "Daily Variance", "Change (%)"]
        
        st.dataframe(top_5_change, use_container_width=True, hide_index=True)
    else:
        st.info("Percentage change data is not available.")

if __name__ == "__main__":
    main()
