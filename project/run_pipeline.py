import sqlite3
import pandas as pd
import os
import requests

# File paths
file_paths = {
    "naics_co2": "https://pasteur.epa.gov/uploads/10.23719/1531143/SupplyChainGHGEmissionFactors_v1.3.0_NAICS_CO2e_USD2022.csv",
    "naics_ghg": "https://pasteur.epa.gov/uploads/10.23719/1531143/SupplyChainGHGEmissionFactors_v1.3.0_NAICS_byGHG_USD2022.csv",
    "us_industries": "https://pasteur.epa.gov/uploads/10.23719/1517796/SupplyChainEmissionFactorsforUSIndustriesCommodities.xlsx",
    "table_4_44": "https://www.bts.gov/sites/bts.dot.gov/files/2024-06/table_04_44_062724.xlsx"
}

# Create a directory to store the downloaded files
if not os.path.exists('data'):
    os.makedirs('data')

# Download files
def download_file(url, local_path):
    response = requests.get(url)
    with open(local_path, 'wb') as file:
        file.write(response.content)
    print(f"Downloaded {url} to {local_path}")

for key, url in file_paths.items():
    download_file(url, f"data/{key}.csv" if key != 'us_industries' and key != 'table_4_44' else f"data/{key}.xlsx")

# Function to transform CSV to DataFrame
def transform_csv_to_df(file_path):
    return pd.read_csv(file_path)

# Function to transform Excel to DataFrame for specific sheet
def transform_excel_to_df(file_path, sheet_name):
    return pd.read_excel(file_path, sheet_name=sheet_name)

# Create or connect to SQLite database
def connect_db(db_name='data/emission.db'):
    conn = sqlite3.connect(db_name)
    return conn

# Save DataFrame to SQLite table
def save_to_sqlite(df, table_name, conn):
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    print(f"Saved {table_name} data to SQLite.")

# Processing and save data for CSV files (NAICS CO2 and GHG)
def process_csv_files(conn):
    naics_co2_df = transform_csv_to_df('data/naics_co2.csv')
    save_to_sqlite(naics_co2_df, 'naics_co2', conn)
    naics_ghg_df = transform_csv_to_df('data/naics_ghg.csv')
    save_to_sqlite(naics_ghg_df, 'naics_ghg', conn)

# Processing and save data for US Industries (2010-2016)
def process_us_industries_files(conn):
    years = [str(year) for year in range(2010, 2017)]
    categories = ['Summary_Commodity', 'Summary_Industry', 'Detail_Commodity', 'Detail_Industry']
    
    for year in years:
        for category in categories:
            sheet_name = f'{year}_{category}'
            try:
                df = transform_excel_to_df('data/us_industries.xlsx', sheet_name)
                save_to_sqlite(df, sheet_name, conn)
            except ValueError:
                print(f"Sheet {sheet_name} not found in the Excel file.")

# Processing and save data for Table 4-44 (from the table_4_44.xlsx file)
def process_table_4_44(conn):
    try:
        df_table_4_44 = transform_excel_to_df('data/table_4_44.xlsx', sheet_name="4-44")
        save_to_sqlite(df_table_4_44, 'categorywise_ghg_Emissions', conn)
    except ValueError:
        print(f"Sheet 4-44 not found in the Excel file.")

# Main pipeline function
def main():
    conn = connect_db()
    
    # Processing CSV files (naics_co2, naics_ghg)
    process_csv_files(conn)
    
    # Processing US Industries data (2010-2016)
    process_us_industries_files(conn)
    
    # Processing specific Table 4-44
    process_table_4_44(conn)

    # Closing db connection
    conn.close()

if __name__ == '__main__':
    main()
