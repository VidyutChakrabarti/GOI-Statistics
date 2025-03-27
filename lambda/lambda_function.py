import os
import boto3
import csv
import psycopg2

def lambda_handler(event, context):
    # Retrieve connection and bucket details from environment variables
    db_host     = os.environ['DB_HOST']
    db_name     = os.environ['DB_NAME']
    db_user     = os.environ['DB_USER']
    db_password = os.environ['DB_PASSWORD']
    s3_bucket   = os.environ['S3_BUCKET']
    
    # Connect to the RDS PostgreSQL instance
    try:
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password
        )
    except Exception as e:
        print("ERROR: Could not connect to RDS instance", e)
        raise e

    cur = conn.cursor()
    
    # Create dimension table (item groups) from CSV "item_group_srl_no_household_consumption.csv"
    create_dim_item_group = """
    CREATE TABLE IF NOT EXISTS dim_item_group (
        item_group_srl_no INTEGER PRIMARY KEY,
        description TEXT
    );
    """
    cur.execute(create_dim_item_group)
    
    # Create fact table for household consumption (from filtered_dataset_household_consumption.csv)
    create_fact_household = """
    CREATE TABLE IF NOT EXISTS fact_household_consumption (
        id SERIAL PRIMARY KEY,
        sector INTEGER,
        state_region INTEGER,
        district INTEGER,
        item_group_srl_no INTEGER,
        state INTEGER,
        district_code INTEGER,
        hhid VARCHAR(50),
        value_consumption_last_30_day NUMERIC,
        value_consumption_last_365_days NUMERIC,
        FOREIGN KEY (item_group_srl_no) REFERENCES dim_item_group(item_group_srl_no)
    );
    """
    cur.execute(create_fact_household)
    
    # Create table for CPI data (from clean_cpi_data.csv)
    create_cpi_data = """
    CREATE TABLE IF NOT EXISTS cpi_data (
        id SERIAL PRIMARY KEY,
        base_year INTEGER,
        year INTEGER,
        month VARCHAR(20),
        state VARCHAR(50),
        sector VARCHAR(50),
        group_name VARCHAR(50),
        sub_group VARCHAR(50),
        index_value NUMERIC,
        inflation NUMERIC
    );
    """
    cur.execute(create_cpi_data)
    conn.commit()
    
    s3_client = boto3.client('s3')
    
    # Process each record from the S3 event
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key    = record['s3']['object']['key']
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8').splitlines()
        reader = csv.DictReader(content)
        
        # Check file name to decide which table to populate
        if 'item_group_srl_no' in key.lower():
            # Process item group dimension data
            for row in reader:
                try:
                    item_group = int(row.get("Value", 0))
                except Exception as e:
                    item_group = None
                description = row.get("Label", "")
                cur.execute("""
                    INSERT INTO dim_item_group (item_group_srl_no, description)
                    VALUES (%s, %s)
                    ON CONFLICT (item_group_srl_no)
                    DO UPDATE SET description = EXCLUDED.description;
                """, (item_group, description))
        elif 'filtered_dataset_household_consumption' in key.lower():
            # Process household consumption fact data
            for row in reader:
                try:
                    sector = int(row.get("Sector", 0))
                except:
                    sector = None
                try:
                    state_region = int(row.get("State_Region", 0))
                except:
                    state_region = None
                try:
                    district = int(row.get("District", 0))
                except:
                    district = None
                try:
                    item_group_srl_no = int(row.get("Item_Group_Srl_No", 0))
                except:
                    item_group_srl_no = None
                try:
                    state_val = int(row.get("State", 0))
                except:
                    state_val = None
                try:
                    district_code = int(row.get("District_Code", 0))
                except:
                    district_code = None
                hhid = row.get("HHID", "")
                try:
                    consumption_30 = float(row.get("Value_of_Consumption_Last_30_Day", 0))
                except:
                    consumption_30 = None
                try:
                    consumption_365 = float(row.get("Value_Consumption_Last_365_Days", 0))
                except:
                    consumption_365 = None

                cur.execute("""
                    INSERT INTO fact_household_consumption (
                        sector, state_region, district, item_group_srl_no, state, district_code, hhid,
                        value_consumption_last_30_day, value_consumption_last_365_days
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, (
                    sector, state_region, district, item_group_srl_no,
                    state_val, district_code, hhid, consumption_30, consumption_365
                ))
        elif 'clean_cpi_data' in key.lower():
            # Process CPI data
            for row in reader:
                try:
                    base_year = int(row.get("BaseYear", 0))
                except:
                    base_year = None
                try:
                    year = int(row.get("Year", 0))
                except:
                    year = None
                month = row.get("Month", "")
                state = row.get("State", "")
                sector = row.get("Sector", "")
                group_name = row.get("Group", "")
                sub_group = row.get("SubGroup", "")
                try:
                    index_value = float(row.get("Index", 0))
                except:
                    index_value = None
                try:
                    inflation = float(row.get("Inflation (%)", 0))
                except:
                    inflation = None

                cur.execute("""
                    INSERT INTO cpi_data (
                        base_year, year, month, state, sector, group_name, sub_group, index_value, inflation
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, (
                    base_year, year, month, state, sector, group_name, sub_group, index_value, inflation
                ))
        conn.commit()
    
    cur.close()
    conn.close()
    
    return {
        'statusCode': 200,
        'body': 'CSV data processed and loaded into RDS successfully!'
    }
