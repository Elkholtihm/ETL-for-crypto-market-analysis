import time
from pyhive import hive
from datetime import datetime
import snowflake.connector
import os
from dotenv import load_dotenv
import mysql.connector


# Import functions from the provided modules
from extract_transform import (
    get_sentiment_data, 
    get_correlation_data, 
    get_coins_data,
    get_last_timestamp,
    get_technical_indicators, 
    get_crypto_info, 
    cryptoinfo,
    blockchaininf,
    apply_sentiment_analysis, 
    transforme_date_dimensions
    )


from load import (
    insert_sentiment_data_to_hive,
    insert_correlation_data_to_hive,
    insert_coins_data_to_hive,
    insert_technical_indicators_to_hive,
    insert_results_into_fact_table,
    insert_metadata_into_hive,
    insert_blockchain_info_into_hive,
    insert_date_dimensions_to_hive
)


def perform_etl_cycle(DWcursor, mysqlCursor):
    """
    Perform a complete ETL cycle for different data sources
    """
    try:
        # 1. Sentiment Analysis
        print("loading sentiment dim".center(160, '='))
        sentiment_data = get_sentiment_data(mysqlCursor)
        if sentiment_data:
            processed_sentiment = apply_sentiment_analysis(sentiment_data)
            insert_sentiment_data_to_hive(processed_sentiment,DWcursor)
    except Exception as e:
        print(f"Error in ETL Cycle:there is a probleme when trying to handle sentiment dim")
    try:
        # 2. Correlation Data
        print("loading correlation dim".center(160, '='))
        correlation_data = get_correlation_data(mysqlCursor)
        if correlation_data:
            insert_correlation_data_to_hive(correlation_data,DWcursor)
    except Exception as e:
        print(f"Error in ETL Cycle:there is a probleme when trying to handle correlation dim")
    try: 
        # 3. Coins Data
        print("loading coins dim".center(160, '='))
        coins_data = get_coins_data(mysqlCursor)
        if coins_data:
            insert_coins_data_to_hive(coins_data,DWcursor)
    except Exception as e:
        
        print(f"Error in ETL Cycle:there is a probleme when trying to handle coins dim")
    try:
        # 4. Technical Indicators
        print("loading indicators dim".center(160, '='))
        indicators_data = get_technical_indicators(mysqlCursor)
        if not indicators_data.empty:
            insert_technical_indicators_to_hive(indicators_data,DWcursor)
    except Exception as e:
        print(f"Error in ETL Cycle:there is a probleme when trying to handle indicators dim")
    try:
        # 5. Last Timestamp and Date Dimensions
        print("loading date dim".center(160, '='))
        last_timestamp = get_last_timestamp(mysqlCursor)
        if last_timestamp:
            transformed_timestamp = transforme_date_dimensions(last_timestamp)
            insert_date_dimensions_to_hive(transformed_timestamp,DWcursor)
    except Exception as e:
        print(f"Error in ETL Cycle:there is a probleme when trying to handle date dim")
    try:
        # 6. Blockchain Information
        # Note: You might need to modify blockchaininf function to match your exact requirements
        print("loading blockchain dim".center(160, '='))
        blockchain_data = blockchaininf()
        if blockchain_data:
            insert_blockchain_info_into_hive(blockchain_data,DWcursor)
    except Exception as e:
        print(f"Error in ETL Cycle:there is a probleme when trying to handle blockchain dim")
    try:
        # 7. Crypto Info Fact Table
        print("loading fact table".center(160, '='))
        fact_data = get_crypto_info()
        if fact_data:
            insert_results_into_fact_table(fact_data,DWcursor)
    except Exception as e:
        print(f"Error in ETL Cycle:there is a probleme when trying to handle fact table")
    try:
        # 8. Metadata
        print("loading metadata dim".center(160, '='))
        metadata = cryptoinfo()
        if metadata:
            insert_metadata_into_hive(metadata,DWcursor)
    
    except Exception as e:
        print(f"Error in ETL Cycle:there is a probleme when trying to handle metadata dim")

current_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(current_dir, '../../.env')
load_dotenv(env_path)

def main():
    """
    Main function to run continuous ETL process
    """
    # Configuration for ETL cycle
    cycle_interval = 4 * 60  # 4 minutes 
    max_runtime_hours = 24  # Run for 24 hours maximum
    load_dotenv()
    DW = input('please provide the data warehouse used *snowflake* or *hive* :')
    print("Starting Continuous ETL Process")
    start_time = datetime.now()
    counter = 1
    try:
        while True:
            if DW == 'snowflake':
                # Establish connection to datawarehouse
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

            elif DW == 'hive':
                conn = hive.connect(
                    host=os.getenv('hive_host'),
                    port=os.getenv('hive_port'), 
                    database=os.getenv('hive_database'))
                cursor = conn.cursor()
            
            if bool(cursor):
                print("Connected to Hive Server")
            # Perform ETL cycle
            print(f"Starting ETL cycle {counter}".center(160, '|'))
            counter += 1

            # Establish connection to the MySQL database
            mysqlConn = mysql.connector.connect(
                host=os.getenv('SQLhost'),
                user=os.getenv('SQLuser'),
                password=os.getenv('SQLpassword'),
                database=os.getenv('SQLdatabase')
            )
            mysqlCursor = mysqlConn.cursor()
            perform_etl_cycle(cursor, mysqlCursor)

            # Check runtime
            current_runtime = datetime.now() - start_time
            if current_runtime.total_seconds() >= max_runtime_hours * 3600:
                print(f"Maximum runtime of {max_runtime_hours} hours reached. Stopping ETL process.")
                break

            # Wait before next cycle
            print(f"Waiting {cycle_interval} seconds before next ETL cycle")
            time.sleep(cycle_interval)
            conn.commit()

    except KeyboardInterrupt:
        print("ETL Process manually stopped by user")
    except Exception as e:
        print("Unexpected error in main ETL loop")
    finally:
        print("ETL Process Terminated")

if __name__ == "__main__":
    main()