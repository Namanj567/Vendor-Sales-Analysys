import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

# Get the location of current directory
print("Current Directory:", os.getcwd())

# Setup directories
logs_dir = 'logs'
data_dir_name = 'data1'

# Create logs directory if it doesn't exist
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

# Create data directory if it doesn't exist
if not os.path.exists(data_dir_name):
    os.makedirs(data_dir_name)

# Setup logging
logging.basicConfig(
    filename=os.path.join(logs_dir, 'ingestion_db1.log'),
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a'
)

# Create database connection
engine = create_engine('sqlite:///inventory.db')

# Set dynamic path for data directory
base_dir = os.path.abspath(os.getcwd())
data_dir = os.path.join(base_dir, data_dir_name)

# Function to ingest data into the database
def ingest_db(df, table_name, engine, chunksize=10000):
    """Ingest a dataframe into the database, replacing the existing table if present."""
    try:
        df.to_sql(table_name, con=engine, if_exists="replace", index=False)
        logging.info(f"Table '{table_name}' ingested successfully.")
    except Exception as e:
        logging.error(f"Error ingesting {table_name}: {e}")

# Function to load and ingest all CSV files from the data directory
def load_raw_data():
    start_time = time.time()
    files_ingested = 0
    for file in os.listdir(data_dir):
        if file.endswith('.csv'):
            file_path = os.path.join(data_dir, file)
            try:
                df = pd.read_csv(file_path)
                logging.info(f"Ingesting {file} into database.")
                ingest_db(df, file[:-4], engine)
                files_ingested += 1
            except Exception as e:
                logging.error(f"Failed to process {file}: {e}")
    end_time = time.time()
    total_time = (end_time - start_time) / 60  # Convert seconds to minutes
    logging.info(f"Ingestion completed for {files_ingested} files.")
    logging.info(f"Total time taken: {total_time:.2f} minutes.")

if __name__ == "__main__":
    load_raw_data()
