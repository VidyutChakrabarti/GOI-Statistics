import os
import boto3
import csv
import psycopg2

def safe_float(value):
    """Convert a value to float, returning None for empty strings or 'NaN'."""
    try:
        if value is None or str(value).strip() == "" or str(value).strip().lower() == "nan":
            return None
        return float(value)
    except Exception:
        return None

def safe_int(value):
    """Convert a value to int, treating empty strings or 'NaN' as None."""
    try:
        if value is None or str(value).strip() == "" or str(value).strip().lower() == "nan":
            return None
        return int(float(value))
    except Exception:
        return None

def lambda_handler(event, context):
    # Retrieve database and bucket details from environment variables
    db_host     = os.environ['DB_HOST']
    db_name     = os.environ['DB_NAME']
    db_user     = os.environ['DB_USER']
    db_password = os.environ['DB_PASSWORD']
    s3_bucket   = os.environ['S3_BUCKET']
    
    # Connect to the PostgreSQL RDS instance
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
    
    # ----------------------------------------
    # Create tables for Household Consumption Star Schema
    # ----------------------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_household (
        hhid VARCHAR(50) PRIMARY KEY
    );
    """)
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_geography (
        geography_id SERIAL PRIMARY KEY,
        state VARCHAR(50),
        district VARCHAR(50),
        district_code VARCHAR(50),
        state_region VARCHAR(50),
        UNIQUE(state, district, district_code, state_region)
    );
    """)
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_sector (
        sector INTEGER PRIMARY KEY,
        description TEXT
    );
    """)
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_item_group (
        item_group_srl_no INTEGER PRIMARY KEY,
        description TEXT
    );
    """)
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_household_consumption (
        consumption_id SERIAL PRIMARY KEY,
        hhid VARCHAR(50) REFERENCES dim_household(hhid),
        geography_id INTEGER REFERENCES dim_geography(geography_id),
        sector INTEGER REFERENCES dim_sector(sector),
        item_group_srl_no INTEGER REFERENCES dim_item_group(item_group_srl_no),
        value_consumption_last_30_day NUMERIC,
        value_consumption_last_365_days NUMERIC
    );
    """)
    
    # ----------------------------------------
    # Create tables for CPI Star Schema
    # ----------------------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_time_cpi (
        time_id SERIAL PRIMARY KEY,
        base_year INTEGER,
        year INTEGER,
        month VARCHAR(50),
        UNIQUE(base_year, year, month)
    );
    """)
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_cpi_context (
        context_id SERIAL PRIMARY KEY,
        state VARCHAR(100),
        sector VARCHAR(100),
        group_name VARCHAR(100),
        sub_group VARCHAR(100),
        UNIQUE(state, sector, group_name, sub_group)
    );
    """)
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_cpi (
        cpi_id SERIAL PRIMARY KEY,
        time_id INTEGER REFERENCES dim_time_cpi(time_id),
        context_id INTEGER REFERENCES dim_cpi_context(context_id),
        index_value NUMERIC,
        inflation NUMERIC
    );
    """)
    
    conn.commit()
    
    s3_client = boto3.client('s3')
    
    # Process the S3 records
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key    = record['s3']['object']['key']
        
        if "item_group_srl_no_household_consumption" in key.lower():
            # Process the item group dimension file.
            try:
                response = s3_client.get_object(Bucket=bucket, Key=key)
            except Exception as e:
                print("Error retrieving Item Group CSV from S3:", e)
                continue
            
            content = response['Body'].read().decode('utf-8').splitlines()
            reader = csv.DictReader(content)
            for row in reader:
                item_group = safe_int(row.get("Value"))
                description = row.get("Label", "").strip()
                if item_group is not None:
                    cur.execute("""
                        INSERT INTO dim_item_group (item_group_srl_no, description)
                        VALUES (%s, %s)
                        ON CONFLICT (item_group_srl_no)
                        DO UPDATE SET description = EXCLUDED.description;
                    """, (item_group, description))
            conn.commit()
        
        elif "filtered_dataset_household_consumption" in key.lower():
            # Process the household consumption file.
            try:
                response = s3_client.get_object(Bucket=bucket, Key=key)
            except Exception as e:
                print("Error retrieving Household Consumption CSV from S3:", e)
                continue
            
            content = response['Body'].read().decode('utf-8').splitlines()
            reader = csv.DictReader(content)
            for row in reader:
                sector = safe_int(row.get("Sector"))
                state_region = row.get("State_Region", "").strip()
                district = row.get("District", "").strip()
                item_group_srl_no = safe_int(row.get("Item_Group_Srl_No"))
                state = row.get("State", "").strip()
                district_code = row.get("District_Code", "").strip()
                hhid = row.get("HHID", "").strip()
                consumption_30 = safe_float(row.get("Value_of_Consumption_Last_30_Day"))
                consumption_365 = safe_float(row.get("Value_Consumption_Last_365_Days"))
                
                # Insert Household dimension
                if hhid:
                    cur.execute("""
                        INSERT INTO dim_household (hhid)
                        VALUES (%s)
                        ON CONFLICT (hhid) DO NOTHING;
                    """, (hhid,))
                
                # Insert Geography dimension
                cur.execute("""
                    INSERT INTO dim_geography (state, district, district_code, state_region)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (state, district, district_code, state_region) DO NOTHING;
                """, (state, district, district_code, state_region))
                cur.execute("""
                    SELECT geography_id FROM dim_geography
                    WHERE state = %s AND district = %s AND district_code = %s AND state_region = %s;
                """, (state, district, district_code, state_region))
                geo_row = cur.fetchone()
                geography_id = geo_row[0] if geo_row else None
                
                # Insert Sector dimension
                if sector is not None:
                    cur.execute("""
                        INSERT INTO dim_sector (sector, description)
                        VALUES (%s, %s)
                        ON CONFLICT (sector) DO NOTHING;
                    """, (sector, f"Sector {sector}"))
                
                # Insert Item Group dimension (if not already inserted/updated)
                if item_group_srl_no is not None:
                    cur.execute("""
                        INSERT INTO dim_item_group (item_group_srl_no, description)
                        VALUES (%s, %s)
                        ON CONFLICT (item_group_srl_no) DO NOTHING;
                    """, (item_group_srl_no, f"Item Group {item_group_srl_no}"))
                
                # Insert into Fact Table for household consumption
                cur.execute("""
                    INSERT INTO fact_household_consumption (
                        hhid, geography_id, sector, item_group_srl_no,
                        value_consumption_last_30_day, value_consumption_last_365_days
                    )
                    VALUES (%s, %s, %s, %s, %s, %s);
                """, (hhid, geography_id, sector, item_group_srl_no, consumption_30, consumption_365))
            conn.commit()
        
        elif "clean_cpi_data" in key.lower():
            # Process the CPI CSV file.
            try:
                response = s3_client.get_object(Bucket=bucket, Key=key)
            except Exception as e:
                print("Error retrieving CPI CSV from S3:", e)
                continue
            
            content = response['Body'].read().decode('utf-8').splitlines()
            reader = csv.DictReader(content)
            for row in reader:
                base_year  = safe_int(row.get("BaseYear"))
                year_val   = safe_int(row.get("Year"))
                month_val  = row.get("Month", "").strip()
                state_val  = row.get("State", "").strip()
                sector_val = row.get("Sector", "").strip()
                group_val  = row.get("Group", "").strip()
                sub_group_val = row.get("SubGroup", "").strip()
                index_value   = safe_float(row.get("Index"))
                inflation     = safe_float(row.get("Inflation (%)"))
                
                # Insert into Time dimension for CPI
                cur.execute("""
                    INSERT INTO dim_time_cpi (base_year, year, month)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (base_year, year, month) DO NOTHING;
                """, (base_year, year_val, month_val))
                cur.execute("""
                    SELECT time_id FROM dim_time_cpi
                    WHERE base_year = %s AND year = %s AND month = %s;
                """, (base_year, year_val, month_val))
                time_row = cur.fetchone()
                time_id = time_row[0] if time_row else None
                
                # Insert into CPI Context dimension
                cur.execute("""
                    INSERT INTO dim_cpi_context (state, sector, group_name, sub_group)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (state, sector, group_name, sub_group) DO NOTHING;
                """, (state_val, sector_val, group_val, sub_group_val))
                cur.execute("""
                    SELECT context_id FROM dim_cpi_context
                    WHERE state = %s AND sector = %s AND group_name = %s AND sub_group = %s;
                """, (state_val, sector_val, group_val, sub_group_val))
                context_row = cur.fetchone()
                context_id = context_row[0] if context_row else None
                
                # Insert record into CPI fact table
                cur.execute("""
                    INSERT INTO fact_cpi (time_id, context_id, index_value, inflation)
                    VALUES (%s, %s, %s, %s);
                """, (time_id, context_id, index_value, inflation))
            conn.commit()
    
    cur.close()
    conn.close()
    
    return {
        'statusCode': 200,
        'body': 'CSV data processed and loaded into the star schema successfully!'
    }
