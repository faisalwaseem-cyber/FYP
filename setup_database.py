"""
Database Setup Script for PostgreSQL 18.
- Connects to Postgres
- Creates the 'ecommerce_intelligence' database if missing
- Executes 'ecommerce_intelligence_schema.sql'
- Creates 'raw.sync_log' table
- Seeds the initial business row (business_id = 1)
"""
import sys
from pathlib import Path
from sqlalchemy import create_engine, text
from config.settings import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, get_db_url

def setup_database():
    print("=" * 70)
    print(f"[*] Setting up PostgreSQL Database: {DB_NAME} on {DB_HOST}:{DB_PORT}")
    print("=" * 70)

    # 1. Connect to postgres system database to ensure target DB exists
    postgres_url = get_db_url(db_name="postgres")
    sys_engine = create_engine(postgres_url, isolation_level="AUTOCOMMIT")

    try:
        with sys_engine.connect() as conn:
            check_sql = text("SELECT 1 FROM pg_database WHERE datname = :dbname")
            exists = conn.execute(check_sql, {"dbname": DB_NAME}).scalar()
            if not exists:
                print(f"[*] Database '{DB_NAME}' does not exist. Creating it now...")
                conn.execute(text(f'CREATE DATABASE "{DB_NAME}"'))
                print(f"[OK] Database '{DB_NAME}' created successfully.")
            else:
                print(f"[OK] Database '{DB_NAME}' already exists.")
    except Exception as e:
        print(f"[!] Error checking/creating database: {e}")
        print("Please verify your DB_USER and DB_PASSWORD in .env")
        sys.exit(1)

    # 2. Connect to the target database
    target_engine = create_engine(get_db_url(), isolation_level="AUTOCOMMIT")
    schema_file = Path(__file__).resolve().parent / "ecommerce_intelligence_schema.sql"

    if not schema_file.exists():
        print(f"[!] Schema file not found at: {schema_file}")
        sys.exit(1)

    print(f"[*] Executing schema definitions from: {schema_file.name}...")
    with open(schema_file, "r", encoding="utf-8") as f:
        schema_sql = f.read()

    with target_engine.connect() as conn:
        # Run schema script
        conn.execute(text(schema_sql))
        print("[OK] Base schema and 24 tables verified/created.")

        # Ensure raw.sync_log exists
        sync_log_sql = text("""
            CREATE TABLE IF NOT EXISTS raw.sync_log (
                sync_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                business_id      BIGINT NOT NULL REFERENCES core.business (business_id),
                source           TEXT NOT NULL,
                sync_type        TEXT NOT NULL,
                status           TEXT NOT NULL,
                records_fetched  INTEGER DEFAULT 0,
                records_inserted INTEGER DEFAULT 0,
                records_updated  INTEGER DEFAULT 0,
                error_message    TEXT,
                started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
                completed_at     TIMESTAMPTZ
            );
        """)
        conn.execute(sync_log_sql)
        print("[OK] Verified pipeline table: raw.sync_log.")

        # Seed initial business if not present
        seed_business_sql = text("""
            INSERT INTO core.business (business_id, business_name, is_active, created_at)
            OVERRIDING SYSTEM VALUE
            VALUES (1, 'Main Store', TRUE, now())
            ON CONFLICT (business_id) DO NOTHING;
        """)
        conn.execute(seed_business_sql)
        print("[OK] Seeded default business entity (ID: 1) in core.business.")

        # Print table summary
        summary_sql = text("""
            SELECT table_schema, count(*) AS table_count
            FROM information_schema.tables
            WHERE table_schema IN ('raw', 'staging', 'core', 'analytics', 'growth')
              AND table_type = 'BASE TABLE'
            GROUP BY table_schema
            ORDER BY table_schema;
        """)
        print("\n--- Current Database Schema Summary ---")
        results = conn.execute(summary_sql).fetchall()
        for schema_name, count in results:
            print(f"Schema: {schema_name:<12} | Tables: {count}")
        print("---------------------------------------")
        print("[OK] Database is ready for Shopify data ingestion!\n")

if __name__ == "__main__":
    setup_database()
