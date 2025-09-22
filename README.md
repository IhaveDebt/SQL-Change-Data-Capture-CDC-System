# main.py
import psycopg2
import time

DB_PARAMS = {
    "dbname": "cdc_db",
    "user": "postgres",
    "password": "password",
    "host": "localhost",
    "port": 5432
}

def capture_changes():
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    cur.execute("SELECT * FROM change_log ORDER BY change_time DESC LIMIT 10;")
    rows = cur.fetchall()
    print("Latest Changes:")
    for r in rows:
        print(r)
    cur.close()
    conn.close()

if __name__ == "__main__":
    while True:
        capture_changes()
        time.sleep(5)
-- schema.sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username TEXT,
    email TEXT,
    created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE change_log (
    id SERIAL PRIMARY KEY,
    table_name TEXT,
    operation TEXT,
    row_data JSONB,
    change_time TIMESTAMP DEFAULT now()
);

CREATE OR REPLACE FUNCTION log_user_changes()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO change_log(table_name, operation, row_data)
    VALUES('users', TG_OP, row_to_json(NEW));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER user_changes
AFTER INSERT OR UPDATE OR DELETE ON users
FOR EACH ROW EXECUTE FUNCTION log_user_changes();
