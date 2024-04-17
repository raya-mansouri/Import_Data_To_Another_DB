import psycopg2

# Database connection details (replace with your actual values)
source_host = "185.147.161.216"
source_database = "news_coverage_db"
source_user = "postgres"
source_password = "YaZahra_110"

target_host = "185.147.161.216"
target_database = "fateme_live_fix_db"
target_user = "postgres"
target_password = "YaZahra_110"

# Connect to source and target databases
source_conn = psycopg2.connect(
    host=source_host,
    database=source_database,
    user=source_user,
    password=source_password,
    port='5432'
)
target_conn = psycopg2.connect(
    host=target_host,
    database=target_database,
    user=target_user,
    password=target_password,
    port='5432'
)

# Create cursors for execution
source_cur = source_conn.cursor()
target_cur = target_conn.cursor()

table_name = 'Image'
app_name = 'attachments'
source_table = app_name + "_" + table_name.lower()
target_table = app_name + "_" + table_name.lower()

# Fetch column names from the source table
source_cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{source_table}'")
column_names = [row[0] for row in source_cur.fetchall()]

# Construct the INSERT statement dynamically with placeholders
placeholders = ', '.join(['%s'] * len(column_names))
insert_sql = f"""
        INSERT INTO {target_table} ({', '.join(column_names)})
        VALUES ({placeholders})
        """

# Fetch data from the source table
source_cur.execute(f"SELECT * FROM {source_table}")
source_data = source_cur.fetchall()


# Insert data using executemany for efficiency
target_cur.executemany(insert_sql, source_data)

# Commit changes to the target database
target_conn.commit()
print(f"Successfully inserted {len(source_data)} rows from '{source_table}' to '{target_table}'.")

source_conn.close()
target_conn.close()
