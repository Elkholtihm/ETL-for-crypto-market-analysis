import snowflake.connector
import os
from dotenv import load_dotenv


# Snowflake Connection
load_dotenv()
def create_snowflake_schema():
    conn = snowflake.connector.connect(
        user=os.getenv('user_snowflake'),
        password=os.getenv('password_snowflake'),
        account=os.getenv('snowflake_account')
    )

    cursor = conn.cursor()

    # Set up your Snowflake context
    cursor.execute("USE WAREHOUSE COMPUTE_WH;")
    cursor.execute("USE DATABASE CRYPTODW;")
    cursor.execute("USE SCHEMA CRYPTOS;")

    # Define Schema Creation Queries
    schema_queries = [
        """
        CREATE TABLE IF NOT EXISTS date_dim (
            timestamp STRING,
            day INT,
            month INT,
            year INT,
            hour INT,
            day_of_week STRING,
            week INT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS crypto_info (
            symbol STRING,
            launch_date DATE,
            ath_price FLOAT,
            ath_date DATE,
            total_supply BIGINT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS blockchain_info (
            timestamp STRING,
            symbol STRING,
            hashrate FLOAT,
            pts INT,
            total_trans BIGINT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS technical_indicators (
            timestamp STRING,
            symbol STRING,
            rsi FLOAT,
            sma FLOAT,
            ema FLOAT,
            macd FLOAT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS correlations_dim (
            day STRING,
            gold_price FLOAT,
            interest_rate FLOAT,
            stocks_price FLOAT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS sentiment_dim (
            timestamp STRING,
            sentiment_score FLOAT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_table (
            timestamp STRING,
            symbol STRING,
            price FLOAT,
            dominance FLOAT,
            exchangerate FLOAT
        );
        """,
        """ 
        CREATE TABLE IF NOT EXISTS crypto_dim (
            timestamp STRING,
            symbol STRING,
            open FLOAT,
            close FLOAT,
            high FLOAT,
            volum FLOAT,
            market_cap FLOAT,
            price_change FLOAT,
            price_change_perc FLOAT,
            market_cap_change FLOAT,
            market_cap_change_perc FLOAT
        );
        """
    ]

    # Execute Queries
    for query in schema_queries:
        cursor.execute(query)
        print(f"Executed: {query}")

    cursor.close()
    conn.close()
    print("Data warehouse schema created successfully in Snowflake!")

if __name__ == "__main__":
    create_snowflake_schema()