import sqlite3
import pandas as pd
import os
import requests
import argparse

# File paths
file_paths = {
    "naics_co2": "https://pasteur.epa.gov/uploads/10.23719/1531143/SupplyChainGHGEmissionFactors_v1.3.0_NAICS_CO2e_USD2022.csv",
    "naics_ghg": "https://pasteur.epa.gov/uploads/10.23719/1531143/SupplyChainGHGEmissionFactors_v1.3.0_NAICS_byGHG_USD2022.csv",
    "us_industries": "https://pasteur.epa.gov/uploads/10.23719/1517796/SupplyChainEmissionFactorsforUSIndustriesCommodities.xlsx",
    "table_4_44": "https://www.bts.gov/sites/bts.dot.gov/files/2024-06/table_04_44_062724.xlsx"
}

# Directory to store the downloaded files
DATA_DIR = 'data'

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

# Download files function
def download_file(url, local_path):
    print(f"Checking for {local_path}...")
    if os.path.exists(local_path):
        print(f"File {local_path} already exists. Skipping download.")
        return
    response = requests.get(url)
    with open(local_path, 'wb') as file:
        file.write(response.content)
    print(f"Downloaded {url} to {local_path}")

# Function to process CSV and Excel files
def transform_csv_to_df(file_path):
    return pd.read_csv(file_path)

def transform_excel_to_df(file_path, sheet_name):
    return pd.read_excel(file_path, sheet_name=sheet_name)

# SQLite connection
def connect_db(db_name=f'{DATA_DIR}/emission.db'):
    return sqlite3.connect(db_name)

def connect_insights_db():
    return sqlite3.connect(f'{DATA_DIR}/insights.db')

# Save DataFrame to SQLite
def save_to_sqlite(df, table_name, conn):
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    print(f"Saved {table_name} data to SQLite.")

# Process CSV files and save with '2017' prefix
def process_csv_files(conn, use_cache, insights_conn):
    file_paths_local = {
        "naics_co2": f"{DATA_DIR}/naics_co2.csv",
        "naics_ghg": f"{DATA_DIR}/naics_ghg.csv"
    }
    
    for key, url in file_paths.items():
        if key in file_paths_local:
            local_path = file_paths_local[key]
            if not use_cache:
                download_file(url, local_path)
            df = transform_csv_to_df(local_path)
            # Add '2017' prefix to table name and store in insights.db
            save_to_sqlite(df, f"{key}_2017", insights_conn)

# Process US Industries files and create combined versions for each category
def process_us_industries_files(insights_conn, use_cache):
    local_path = f"{DATA_DIR}/us_industries.xlsx"
    if not use_cache:
        download_file(file_paths["us_industries"], local_path)
    
    # Categories to be processed
    categories = ['Summary_Commodity', 'Summary_Industry', 'Detail_Commodity', 'Detail_Industry']
    years = [str(year) for year in range(2010, 2017)]  # 2010-2016

    # Iterate over each category and combine data
    for category in categories:
        combined_df = pd.DataFrame()
        for year in years:
            sheet_name = f'{year}_{category}'
            try:
                df = transform_excel_to_df(local_path, sheet_name)
                combined_df = pd.concat([combined_df, df], ignore_index=True)
            except ValueError:
                print(f"Sheet {sheet_name} not found in the Excel file.")
        
        # After combining data for a category, save it into insights.db
        table_name = f"{category}_2010_2016"
        save_to_sqlite(combined_df, table_name, insights_conn)

# Process Table 4-44
def process_table_4_44(insights_conn, use_cache):
    local_path = f"{DATA_DIR}/table_4_44.xlsx"
    if not use_cache:
        download_file(file_paths["table_4_44"], local_path)
    try:
        df = transform_excel_to_df(local_path, sheet_name="4-44")
        # Clean the data if necessary before saving to insights.db
        df = df.dropna(axis=1, how='all')  # Remove columns with all NaN values
        save_to_sqlite(df, 'categorywise_ghg_Emissions_1990_2022', insights_conn)
    except ValueError:
        print(f"Sheet 4-44 not found in the Excel file.")

# Main pipeline function
def main():
    # Argument parsing for cache usage
    parser = argparse.ArgumentParser(description="Run the data pipeline with optional caching.")
    parser.add_argument("--use-cache", action="store_true", help="Use cached data instead of downloading.")
    args = parser.parse_args()
    use_cache = args.use_cache

    # Connect to both emission.db and insights.db
    conn = connect_db()
    insights_conn = connect_insights_db()
    
    # Process CSV files and store them with '2017' prefix in insights.db
    process_csv_files(conn, use_cache, insights_conn)
    
    # Process US Industries data (2010-2016) and combine them into single tables for each category
    process_us_industries_files(insights_conn, use_cache)
    
    # Process Table 4-44 and store it in insights.db
    process_table_4_44(insights_conn, use_cache)

    # Close database connections
    conn.close()
    insights_conn.close()
    
    print("Data pipeline executed successfully.")

if __name__ == '__main__':
    main()
