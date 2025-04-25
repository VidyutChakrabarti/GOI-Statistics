import os
import boto3
import csv
import psycopg2
def safe_float(value):
    try:
        if value is None or str(value).strip() == "" or str(value).strip().lower() == "nan":
            return None
        return float(value)
    except Exception as e:
        print("Error converting value '{}' to float: {}".format(value, e))
        return None
def safe_int(value):
    try:
        if value is None or str(value).strip() == "" or str(value).strip().lower() == "nan":
            return None
        return int(float(value))
    except Exception as e:
        print("Error converting value '{}' to int: {}".format(value, e))
        return None
def lambda_handler(event, context):
    try:
        db_host     = os.environ['DB_HOST']
        db_name     = os.environ['DB_NAME']
        db_user     = os.environ['DB_USER']
        db_password = os.environ['DB_PASSWORD']
        s3_bucket   = os.environ['S3_BUCKET']
    except KeyError as e:
        print("Missing environment variable: {}".format(e))
        raise e

    if ':' in db_host:
        db_host = db_host.split(':')[0]
    db_port = 5432
    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        conn.autocommit = False
        print("Connected to PostgreSQL RDS instance.")
    except Exception as e:
        print("ERROR: Could not connect to RDS instance: {}".format(e))
        raise e
    try:
        s3_client = boto3.client('s3')
        print("Connected to S3.")
    except Exception as e:
        print("ERROR: Could not connect to S3: {}".format(e))
        raise e
    cur = conn.cursor()
    try:
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
        print("Tables created or verified successfully.")
    except Exception as e:
        print("Error creating tables: {}".format(e))
        conn.rollback()
        raise e
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key    = record['s3']['object']['key']
        print("Processing file from bucket '{}' with key '{}'".format(bucket, key))
        if "item_group_srl_no_household_consumption" in key.lower():
            try:
                cur.execute("SELECT COUNT(*) FROM dim_item_group;")
                count = cur.fetchone()[0]
                if count > 0:
                    print("dim_item_group already has data. Skipping file: {}".format(key))
                    continue
                response = s3_client.get_object(Bucket=bucket, Key=key)
                print("Fetched file '{}' from S3.".format(key))
                content = response['Body'].read().decode('utf-8').splitlines()
                reader = csv.DictReader(content)
                rows_to_insert = []
                for row in reader:
                    item_group = safe_int(row.get("Value"))
                    description = row.get("Label", "").strip()
                    if item_group is not None:
                        rows_to_insert.append((item_group, description))
                    else:
                        print("Item Group is None, skipping row: {}".format(row))
                if rows_to_insert:
                    cur.executemany("""
                        INSERT INTO dim_item_group (item_group_srl_no, description)
                        VALUES (%s, %s)
                        ON CONFLICT (item_group_srl_no)
                        DO UPDATE SET description = EXCLUDED.description;
                    """, rows_to_insert)
                    conn.commit()
                    print("Batch inserted {} rows into dim_item_group.".format(len(rows_to_insert)))
                else:
                    print("No valid rows found in file: {}".format(key))
            except Exception as e:
                print("Error processing file '{}': {}".format(key, e))
                conn.rollback()
                continue
        elif "filtered_dataset_household_consumption" in key.lower():
            try:
                cur.execute("SELECT COUNT(*) FROM fact_household_consumption;")
                count = cur.fetchone()[0]
                if count > 0:
                    print("fact_household_consumption already has data. Skipping file: {}".format(key))
                    continue
                response = s3_client.get_object(Bucket=bucket, Key=key)
                print("Fetched file '{}' from S3.".format(key))
                content = response['Body'].read().decode('utf-8').splitlines()
                reader = csv.DictReader(content)
                household_set = set()
                geography_set = set()
                sector_set = set()
                item_group_set = set()
                records = [] 
                for row in reader:
                    hhid = row.get("HHID", "").strip()
                    sector = safe_int(row.get("Sector"))
                    state_region = row.get("State_Region", "").strip()
                    district = row.get("District", "").strip()
                    item_group_srl_no = safe_int(row.get("Item_Group_Srl_No"))
                    state = row.get("State", "").strip()
                    district_code = row.get("District_Code", "").strip()
                    consumption_30 = safe_float(row.get("Value_of_Consumption_Last_30_Day"))
                    consumption_365 = safe_float(row.get("Value_Consumption_Last_365_Days"))
                    if not hhid:
                        print("Empty HHID, skipping row: {}".format(row))
                        continue
                    household_set.add(hhid)
                    geography_tuple = (state, district, district_code, state_region)
                    geography_set.add(geography_tuple)
                    if sector is not None:
                        sector_set.add((sector, "Sector {}".format(sector)))
                    if item_group_srl_no is not None:
                        item_group_set.add((item_group_srl_no, "Item Group {}".format(item_group_srl_no)))
                    records.append({
                        "hhid": hhid,
                        "geography": geography_tuple,
                        "sector": sector,
                        "item_group_srl_no": item_group_srl_no,
                        "consumption_30": consumption_30,
                        "consumption_365": consumption_365
                    })
                if household_set:
                    household_rows = [(hhid,) for hhid in household_set]
                    cur.executemany("""
                        INSERT INTO dim_household (hhid)
                        VALUES (%s)
                        ON CONFLICT (hhid) DO NOTHING;
                    """, household_rows)
                    print("Batch inserted {} rows into dim_household.".format(len(household_rows)))
                if geography_set:
                    geography_rows = list(geography_set)
                    cur.executemany("""
                        INSERT INTO dim_geography (state, district, district_code, state_region)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (state, district, district_code, state_region) DO NOTHING;
                    """, geography_rows)
                    print("Batch inserted {} rows into dim_geography.".format(len(geography_rows)))
                if sector_set:
                    sector_rows = list(sector_set)
                    cur.executemany("""
                        INSERT INTO dim_sector (sector, description)
                        VALUES (%s, %s)
                        ON CONFLICT (sector) DO NOTHING;
                    """, sector_rows)
                    print("Batch inserted {} rows into dim_sector.".format(len(sector_rows)))
                if item_group_set:
                    item_group_rows = list(item_group_set)
                    cur.executemany("""
                        INSERT INTO dim_item_group (item_group_srl_no, description)
                        VALUES (%s, %s)
                        ON CONFLICT (item_group_srl_no) DO NOTHING;
                    """, item_group_rows)
                    print("Batch inserted {} rows into dim_item_group.".format(len(item_group_rows)))
                conn.commit()
                cur.execute("""
                    SELECT state, district, district_code, state_region, geography_id
                    FROM dim_geography;
                """)
                geo_map = {}
                for row in cur.fetchall():
                    key_tuple = (row[0], row[1], row[2], row[3])
                    geo_map[key_tuple] = row[4]
                fact_rows = []
                for rec in records:
                    geo_id = geo_map.get(rec["geography"])
                    if geo_id is None:
                        print("Geography id not found for record: {}".format(rec))
                        continue
                    fact_rows.append((
                        rec["hhid"],
                        geo_id,
                        rec["sector"],
                        rec["item_group_srl_no"],
                        rec["consumption_30"],
                        rec["consumption_365"]
                    ))
                if fact_rows:
                    cur.executemany("""
                        INSERT INTO fact_household_consumption (
                            hhid, geography_id, sector, item_group_srl_no,
                            value_consumption_last_30_day, value_consumption_last_365_days
                        )
                        VALUES (%s, %s, %s, %s, %s, %s);
                    """, fact_rows)
                    print("Batch inserted {} rows into fact_household_consumption.".format(len(fact_rows)))
                conn.commit()
            except Exception as e:
                print("Error processing file '{}': {}".format(key, e))
                conn.rollback()
                continue
        elif "clean_cpi_data" in key.lower():
            try:
                cur.execute("SELECT COUNT(*) FROM fact_cpi;")
                count = cur.fetchone()[0]
                if count > 0:
                    print("fact_cpi already has data. Skipping file: {}".format(key))
                    continue
                response = s3_client.get_object(Bucket=bucket, Key=key)
                print("Fetched file '{}' from S3.".format(key))
                content = response['Body'].read().decode('utf-8').splitlines()
                reader = csv.DictReader(content)
                records = []
                time_set = set()
                context_set = set()
                for row in reader:
                    base_year = safe_int(row.get("BaseYear"))
                    year_val = safe_int(row.get("Year"))
                    month_val = row.get("Month", "").strip()
                    state_val = row.get("State", "").strip()
                    sector_val = row.get("Sector", "").strip()
                    group_val = row.get("Group", "").strip()
                    sub_group_val = row.get("SubGroup", "").strip()
                    index_value = safe_float(row.get("Index"))
                    inflation = safe_float(row.get("Inflation (%)"))
                    time_set.add((base_year, year_val, month_val))
                    context_set.add((state_val, sector_val, group_val, sub_group_val))
                    records.append({
                        "time": (base_year, year_val, month_val),
                        "context": (state_val, sector_val, group_val, sub_group_val),
                        "index_value": index_value,
                        "inflation": inflation
                    })
                if time_set:
                    time_rows = list(time_set)
                    cur.executemany("""
                        INSERT INTO dim_time_cpi (base_year, year, month)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (base_year, year, month) DO NOTHING;
                    """, time_rows)
                    print("Batch inserted {} rows into dim_time_cpi.".format(len(time_rows)))
                if context_set:
                    context_rows = list(context_set)
                    cur.executemany("""
                        INSERT INTO dim_cpi_context (state, sector, group_name, sub_group)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (state, sector, group_name, sub_group) DO NOTHING;
                    """, context_rows)
                    print("Batch inserted {} rows into dim_cpi_context.".format(len(context_rows)))
                conn.commit()
                cur.execute("""
                    SELECT base_year, year, month, time_id
                    FROM dim_time_cpi;
                """)
                time_map = {}
                for row in cur.fetchall():
                    key_tuple = (row[0], row[1], row[2])
                    time_map[key_tuple] = row[3]
                cur.execute("""
                    SELECT state, sector, group_name, sub_group, context_id
                    FROM dim_cpi_context;
                """)
                context_map = {}
                for row in cur.fetchall():
                    key_tuple = (row[0], row[1], row[2], row[3])
                    context_map[key_tuple] = row[4]
                fact_rows = []
                for rec in records:
                    time_id = time_map.get(rec["time"])
                    context_id = context_map.get(rec["context"])
                    if time_id is None or context_id is None:
                        print("Mapping not found for record: {}".format(rec))
                        continue
                    fact_rows.append((time_id, context_id, rec["index_value"], rec["inflation"]))
                if fact_rows:
                    cur.executemany("""
                        INSERT INTO fact_cpi (time_id, context_id, index_value, inflation)
                        VALUES (%s, %s, %s, %s);
                    """, fact_rows)
                    print("Batch inserted {} rows into fact_cpi.".format(len(fact_rows)))
                conn.commit()
            except Exception as e:
                print("Error processing file '{}': {}".format(key, e))
                conn.rollback()
                continue
        else:
            print("No matching processing block for file: {}. Skipping.".format(key))
    cur.close()
    conn.close()
    return {
        'statusCode': 200,
        'body': 'CSV data processed and loaded into the star schema successfully!'
    }
