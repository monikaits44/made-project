import os
import sqlite3
import pandas as pd
import subprocess
import pytest
import time
import logging

# Setup logging
log_file = './project/test_pipeline_execution.log'
logging.basicConfig(
    filename=log_file, 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()


# Test Case 1: Validate pipeline execution and output file generation
def test_etl_pipeline_end_to_end():
    print("Running the data pipeline...")
    
    # Run the pipeline using subprocess
    result = subprocess.run([r'./.venv/Scripts/python.exe', './project/run_pipeline.py'], capture_output=True, text=True)

    
    # Ensure the pipeline completes successfully
    assert result.returncode == 0, f"Pipeline failed with error: {result.stderr}"
    print("Pipeline ran successfully.")

    # Validate output files exist
    output_files = [
        './data/emission.db',
        './data/insights.db'
    ]
    
    for file in output_files:
        assert os.path.isfile(file), f"{file} does not exist."
        print(f"Success: {file} exists.")


# Test Case 2: Validate database tables' existence
def test_database_tables():
    print("Validating database tables...")

    # Connect to insights.db
    conn = sqlite3.connect('./data/insights.db')
    cursor = conn.cursor()

    # List of expected tables
    expected_tables = [
        "naics_co2_2017",
        "naics_ghg_2017",
        "Summary_Commodity_2010_2016",
        "Summary_Industry_2010_2016",
        "Detail_Commodity_2010_2016",
        "Detail_Industry_2010_2016",
        "categorywise_ghg_Emissions_1990_2022"
    ]

    # Fetch actual tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    actual_tables = {row[0] for row in cursor.fetchall()}

    conn.close()

    # Assert all expected tables exist
    missing_tables = [table for table in expected_tables if table not in actual_tables]
    assert not missing_tables, f"Missing tables in insights.db: {missing_tables}"
    print("All expected tables are present in insights.db.")

# Test Case 3: Validate data quality in database tables
def test_table_content_naics_co2_2017():
    print("Checking if table naics_co2_2017 contains data...")

    # Connect to insights.db
    conn = sqlite3.connect('./data/insights.db')
    cursor = conn.cursor()

    # Check if table has data
    cursor.execute("SELECT COUNT(*) FROM naics_co2_2017")
    count = cursor.fetchone()[0]
    conn.close()

    assert count > 0, "Table naics_co2_2017 is empty."
    print("Table naics_co2_2017 contains data.")

# Test Case 5: Check for missing values in cleaned data
def test_no_missing_values():
    print("Checking for missing values in naics_co2_2017 table...")

    # Connect to insights.db
    conn = sqlite3.connect('./data/insights.db')

    # Load the table into a DataFrame
    df = pd.read_sql_query("SELECT * FROM naics_co2_2017", conn)
    conn.close()

    # Assert no missing values
    assert df.isnull().sum().sum() == 0, "Missing values found in naics_co2_2017 table."
    print("No missing values in naics_co2_2017 table.")

    # Test Case 6: Validate that the pipeline completes within a reasonable time
def test_pipeline_execution_time():
    logger.info("Testing pipeline execution time...")

    start_time = time.time()

    # Run the pipeline using subprocess
    result = subprocess.run([r'./.venv/Scripts/python.exe', './project/run_pipeline.py'], capture_output=True, text=True)

    end_time = time.time()
    execution_time = end_time - start_time

    if result.returncode == 0:
        logger.info(f"Pipeline executed successfully in {execution_time:.2f} seconds.")
    else:
        logger.error(f"Pipeline failed with error: {result.stderr}")
    
    # Assert that the pipeline runs within a reasonable time and takes time for execution
    assert execution_time < 60, f"Pipeline took too long to execute: {execution_time:.2f} seconds."

# Run all tests
if __name__ == "__main__":
    pytest.main()

