import sqlite3
import pandas as pd
import logging
from ingestion_db1 import ingest_db  # assuming this is defined elsewhere

# Setup logging to track the entire process
logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"  # 'a' stands for append
)

def create_vendor_summary(conn):
    """
    This function merges different tables to create an overall vendor summary
    and adds aggregated columns to the resulting data.
    """
    vendor_sales_summary1 = pd.read_sql_query("""
    WITH freight_cost_summary AS (
        SELECT VendorNumber, SUM(Freight) AS freight_Cost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),
    Purchase_summary AS (
        SELECT 
            pr.VendorNumber,
            pr.VendorName,
            pr.Brand,
            pr.PurchasePrice,
            pp.Price AS Actual_sellingPrice,
            pp.Volume,
            pp.Description,
            SUM(pr.Quantity) AS total_purchase_quantity,
            SUM(pr.Dollars) AS total_purchases_amount_in_dollars
        FROM purchases AS pr 
        JOIN purchase_prices AS pp
            ON pr.Brand = pp.Brand 
        WHERE pr.PurchasePrice > 0
        GROUP BY pr.VendorNumber, pr.VendorName, pr.Brand, pr.PurchasePrice, pp.Price, pp.Volume, pp.Description
        ORDER BY total_purchases_amount_in_dollars
    ),
    Sales_summary AS (
        SELECT 
            VendorNo, 
            Brand, 
            SUM(SalesPrice) AS TOTAL_SALES_PRICE, 
            SUM(SalesDollars) AS TOTAL_SALES_DOLLARS,
            SUM(ExciseTax) AS TOTALSALES_EXCISE_TAX, 
            SUM(SalesQuantity) AS TOTAL_SALES_QUANTITY
        FROM sales 
        GROUP BY VendorNo, Brand
    )
    SELECT 
        ps.VendorNumber, 
        ps.VendorName, 
        ps.Brand, 
        ps.PurchasePrice, 
        ps.Actual_sellingPrice, 
        ps.Volume,
        ps.Description,
        ps.total_purchase_quantity, 
        ps.total_purchases_amount_in_dollars,
        COALESCE(ss.TOTAL_SALES_QUANTITY, 0) AS TOTAL_SALES_QUANTITY,
        COALESCE(ss.TOTAL_SALES_PRICE, 0) AS TOTAL_SALES_PRICE, 
        COALESCE(ss.TOTAL_SALES_DOLLARS, 0) AS TOTAL_SALES_DOLLARS, 
        COALESCE(ss.TOTALSALES_EXCISE_TAX, 0) AS TOTALSALES_EXCISE_TAX,
        COALESCE(fcs.freight_Cost, 0) AS freight_Cost
    FROM Purchase_summary AS ps 
    LEFT JOIN Sales_summary AS ss 
        ON ps.VendorNumber = ss.VendorNo
        AND ps.Brand = ss.Brand 
    LEFT JOIN freight_cost_summary AS fcs 
        ON ps.VendorNumber = fcs.VendorNumber 
    ORDER BY ps.total_purchases_amount_in_dollars;
    """, conn)

    return vendor_sales_summary1

def clean_data(df):
    """
    Cleans the data by fixing types, filling missing values,
    trimming strings, and creating new calculated columns.
    """
    # Convert 'Volume' to float64 (remove trailing space if present)
    df['Volume'] = df['Volume'].astype('float64')

    # Fill missing values with 0
    df.fillna(0, inplace=True)

    # Trim strings in categorical columns
    df['Description'] = df['Description'].str.strip()
    df['VendorName'] = df['VendorName'].str.strip()

    # Create new calculated columns
    df['GrossProfit'] = df['TOTAL_SALES_DOLLARS'] - df['total_purchases_amount_in_dollars']
    df['Stockturnover'] = df.apply(lambda row: row['TOTAL_SALES_QUANTITY'] / row['total_purchase_quantity'] 
                                   if row['total_purchase_quantity'] != 0 else 0, axis=1)
    df['ProfitMargin'] = df.apply(lambda row: (row['GrossProfit'] / row['TOTAL_SALES_DOLLARS'] * 100) 
                                 if row['TOTAL_SALES_DOLLARS'] != 0 else 0, axis=1)
    df['SalesToPurchaseRatio'] = df.apply(lambda row: row['TOTAL_SALES_DOLLARS'] / row['total_purchases_amount_in_dollars'] 
                                         if row['total_purchases_amount_in_dollars'] != 0 else 0, axis=1)

    return df

if __name__ == '__main__':
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect('inventory.db')
        logging.info('Database connection established.')

        # Step 1: Create vendor summary
        logging.info('Creating vendor summary...')
        summary_df = create_vendor_summary(conn)
        logging.info(f'Vendor summary created with {len(summary_df)} rows.')
        logging.debug(summary_df.head().to_string())

        # Step 2: Clean data
        logging.info('Cleaning data...')
        clean_df = clean_data(summary_df)
        logging.info('Data cleaning completed.')
        logging.debug(clean_df.head().to_string())

        # Step 3: Ingest cleaned data into the database
        logging.info('Ingesting data into the database...')
        ingest_db(clean_df, 'vendor_sales_summary1', conn)
        logging.info('Data ingestion completed.')

    except Exception as e:
        logging.error(f'An error occurred: {e}')
    finally:
        if conn:
            conn.close()
            logging.info('Database connection closed.')
