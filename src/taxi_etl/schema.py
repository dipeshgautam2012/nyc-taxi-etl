"""Warehouse DDL — table names and CREATE TABLE statements."""

TABLE_PIPELINE_RUNS = "pipeline_runs"
TABLE_SILVER_TRIPS = "silver_trips"
TABLE_REJECTS = "rejects_quarantine"
TABLE_DIM_DATE = "dim_date"
TABLE_DIM_LOCATION = "dim_location"
TABLE_FACT_RIDES = "fact_rides"
TABLE_BRONZE_STAGING = "_bronze_staging"


def ddl_sqlite() -> list[str]:
    return [
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id TEXT PRIMARY KEY,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            bronze_rows INTEGER DEFAULT 0,
            silver_rows INTEGER DEFAULT 0,
            quarantine_rows INTEGER DEFAULT 0,
            gold_rows INTEGER DEFAULT 0,
            validation_json TEXT,
            error_message TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS silver_trips (
            vendor_id INTEGER,
            tpep_pickup_datetime TEXT NOT NULL,
            tpep_dropoff_datetime TEXT NOT NULL,
            passenger_count REAL,
            trip_distance REAL,
            pulocation_id INTEGER,
            dolocation_id INTEGER,
            payment_type INTEGER,
            fare_amount REAL,
            tip_amount REAL,
            total_amount REAL,
            trip_duration_minutes INTEGER,
            pickup_year INTEGER,
            pickup_month INTEGER,
            pickup_day INTEGER,
            pickup_hour INTEGER,
            pickup_dow INTEGER,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            run_id TEXT NOT NULL,
            _source_file TEXT,
            _ingested_at TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS rejects_quarantine (
            reject_reason TEXT NOT NULL,
            vendor_id INTEGER,
            tpep_pickup_datetime TEXT,
            tpep_dropoff_datetime TEXT,
            passenger_count REAL,
            trip_distance REAL,
            pulocation_id INTEGER,
            dolocation_id INTEGER,
            payment_type INTEGER,
            fare_amount REAL,
            tip_amount REAL,
            total_amount REAL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            run_id TEXT NOT NULL,
            _source_file TEXT,
            _ingested_at TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_date (
            date_key INTEGER PRIMARY KEY,
            pickup_date TEXT NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            day INTEGER NOT NULL,
            hour INTEGER NOT NULL,
            day_of_week INTEGER NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_location (
            location_id INTEGER PRIMARY KEY,
            borough TEXT,
            zone_name TEXT,
            service_zone TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_rides (
            fact_ride_id INTEGER PRIMARY KEY AUTOINCREMENT,
            date_key INTEGER NOT NULL,
            pickup_location_id INTEGER NOT NULL,
            dropoff_location_id INTEGER NOT NULL,
            vendor_id INTEGER,
            passenger_count REAL,
            trip_distance REAL,
            fare_amount REAL,
            tip_amount REAL,
            total_amount REAL,
            trip_duration_minutes INTEGER,
            payment_type INTEGER,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            run_id TEXT NOT NULL,
            FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
            FOREIGN KEY (pickup_location_id) REFERENCES dim_location(location_id),
            FOREIGN KEY (dropoff_location_id) REFERENCES dim_location(location_id)
        )
        """,
    ]


def ddl_duckdb() -> list[str]:
    return [
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id VARCHAR PRIMARY KEY,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            status VARCHAR NOT NULL,
            started_at TIMESTAMP NOT NULL,
            finished_at TIMESTAMP,
            bronze_rows INTEGER DEFAULT 0,
            silver_rows INTEGER DEFAULT 0,
            quarantine_rows INTEGER DEFAULT 0,
            gold_rows INTEGER DEFAULT 0,
            validation_json VARCHAR,
            error_message VARCHAR
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS silver_trips (
            vendor_id INTEGER,
            tpep_pickup_datetime TIMESTAMP NOT NULL,
            tpep_dropoff_datetime TIMESTAMP NOT NULL,
            passenger_count DOUBLE,
            trip_distance DOUBLE,
            pulocation_id INTEGER,
            dolocation_id INTEGER,
            payment_type INTEGER,
            fare_amount DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE,
            trip_duration_minutes INTEGER,
            pickup_year INTEGER,
            pickup_month INTEGER,
            pickup_day INTEGER,
            pickup_hour INTEGER,
            pickup_dow INTEGER,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            run_id VARCHAR NOT NULL,
            _source_file VARCHAR,
            _ingested_at TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS rejects_quarantine (
            reject_reason VARCHAR NOT NULL,
            vendor_id INTEGER,
            tpep_pickup_datetime TIMESTAMP,
            tpep_dropoff_datetime TIMESTAMP,
            passenger_count DOUBLE,
            trip_distance DOUBLE,
            pulocation_id INTEGER,
            dolocation_id INTEGER,
            payment_type INTEGER,
            fare_amount DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            run_id VARCHAR NOT NULL,
            _source_file VARCHAR,
            _ingested_at TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_date (
            date_key INTEGER PRIMARY KEY,
            pickup_date DATE NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            day INTEGER NOT NULL,
            hour INTEGER NOT NULL,
            day_of_week INTEGER NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_location (
            location_id INTEGER PRIMARY KEY,
            borough VARCHAR,
            zone_name VARCHAR,
            service_zone VARCHAR
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_rides (
            fact_ride_id BIGINT PRIMARY KEY,
            date_key INTEGER NOT NULL,
            pickup_location_id INTEGER NOT NULL,
            dropoff_location_id INTEGER NOT NULL,
            vendor_id INTEGER,
            passenger_count DOUBLE,
            trip_distance DOUBLE,
            fare_amount DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE,
            trip_duration_minutes INTEGER,
            payment_type INTEGER,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            run_id VARCHAR NOT NULL
        )
        """,
    ]


def ddl_statements() -> list[str]:
    """Default DDL — SQLite."""
    return ddl_sqlite()

