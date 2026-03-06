import sqlite3
import pandas as pd
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_analytics(db_path: str, output_dir: str, table_name: str) -> None:
    """
    Connects to SQLite, executes 5 analytical SQL queries, and exports results to CSV.
    """
    try:
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        conn = sqlite3.connect(db_path)
        
        # 1. Basic Aggregation: Global max, min, avg closing price
        query_1 = f"""
        SELECT 
            MAX(close) AS max_close,
            MIN(close) AS min_close,
            AVG(close) AS avg_close
        FROM {table_name}
        """
        df_1 = pd.read_sql_query(query_1, conn)
        df_1.to_csv(f"{output_dir}/1_basic_aggregation.csv", index=False)
        logging.info("Exported 1_basic_aggregation.csv")

        # 2. Group By + Aggregation: Average trading volume grouped by month/year
        query_2 = f"""
        SELECT 
            strftime('%Y-%m', date) AS year_month,
            AVG(volume) AS avg_volume
        FROM {table_name}
        GROUP BY year_month
        ORDER BY year_month
        """
        df_2 = pd.read_sql_query(query_2, conn)
        df_2.to_csv(f"{output_dir}/2_monthly_avg_volume.csv", index=False)
        logging.info("Exported 2_monthly_avg_volume.csv")
        
        # 3. Filtering & Logic: Count of "up days" vs "down days"
        query_3 = f"""
        SELECT 
            CASE 
                WHEN close > open THEN 'Up Day'
                WHEN close < open THEN 'Down Day'
                ELSE 'Flat Day'
            END AS day_type,
            COUNT(*) AS day_count
        FROM {table_name}
        GROUP BY day_type
        """
        df_3 = pd.read_sql_query(query_3, conn)
        df_3.to_csv(f"{output_dir}/3_up_vs_down_days.csv", index=False)
        logging.info("Exported 3_up_vs_down_days.csv")

        # 4. Window Function 1 (Moving Average): 7-day moving average of closing price
        # Note: SQLite supports window functions starting from version 3.25.0
        query_4 = f"""
        SELECT 
            date,
            close,
            AVG(close) OVER (
                ORDER BY date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) AS moving_avg_7day
        FROM {table_name}
        ORDER BY date DESC
        """
        df_4 = pd.read_sql_query(query_4, conn)
        df_4.to_csv(f"{output_dir}/4_moving_average.csv", index=False)
        logging.info("Exported 4_moving_average.csv")
        
        # 5. Window Function 2 (Ranking): Rank the top 5 days by highest trading volume
        query_5 = f"""
        WITH RankedDays AS (
            SELECT 
                date,
                volume,
                RANK() OVER(ORDER BY volume DESC) AS volume_rank
            FROM {table_name}
        )
        SELECT *
        FROM RankedDays
        WHERE volume_rank <= 5
        ORDER BY volume_rank
        """
        df_5 = pd.read_sql_query(query_5, conn)
        df_5.to_csv(f"{output_dir}/5_top_volume_days.csv", index=False)
        logging.info("Exported 5_top_volume_days.csv")

        conn.close()
        logging.info("Successfully ran all analytics and exported results.")
        
    except sqlite3.OperationalError as e:
        logging.error(f"SQL Execution Error (Ensure your SQLite version supports Window Functions & data is loaded): {e}")
        raise
    except Exception as e:
        logging.error(f"Error during analytics generation: {e}")
        raise

if __name__ == "__main__":
    # Test logic can be added here, but requires an active DB with data.
    pass
