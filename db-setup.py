import psycopg2

# connect to the database
conn = psycopg2.connect(
    dbname="spaceweather",
    user="connor",
    password="password",
    host="localhost",
    port="5432"
)

cur = conn.cursor()

# Create table query
create_table_query = """
CREATE TABLE IF NOT EXISTS solar_flares (
    id SERIAL PRIMARY KEY,
    flr_id VARCHAR(100),
    class_type VARCHAR(20),
    begin_time VARCHAR(50),
    peak_time VARCHAR(50),
    end_time VARCHAR(50),
    source_location VARCHAR(50)
);
"""

# Execute the query
cur.execute(create_table_query)

# Commit the changes
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()


