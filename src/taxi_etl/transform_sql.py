"""SQL fragments for silver/gold transforms; wording differs by dialect ("sqlite" | "duckdb").

Called from transform.py with warehouse.dialect. Per-row cleaning: valid_where, silver_select.
Partition gates are in validate.py.
"""


def valid_where(dialect: str) -> str:
    if dialect == "duckdb":
        return """
    tpep_pickup_datetime IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL
    AND PULocationID IS NOT NULL AND CAST(PULocationID AS INTEGER) > 0
    AND fare_amount IS NOT NULL AND CAST(fare_amount AS DOUBLE) >= 0
    AND trip_distance IS NOT NULL AND CAST(trip_distance AS DOUBLE) >= 0
    AND CAST(tpep_dropoff_datetime AS TIMESTAMP) >= CAST(tpep_pickup_datetime AS TIMESTAMP)
"""
    return """
    tpep_pickup_datetime IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL
    AND PULocationID IS NOT NULL AND CAST(PULocationID AS INTEGER) > 0
    AND fare_amount IS NOT NULL AND CAST(fare_amount AS REAL) >= 0
    AND trip_distance IS NOT NULL AND CAST(trip_distance AS REAL) >= 0
    AND datetime(tpep_dropoff_datetime) >= datetime(tpep_pickup_datetime)
"""


def silver_select(staging: str, dialect: str) -> str:
    if dialect == "duckdb":
        pickup_ts = "CAST(tpep_pickup_datetime AS TIMESTAMP)"
        dropoff_ts = "CAST(tpep_dropoff_datetime AS TIMESTAMP)"
        duration = (
            f"CAST(datediff('minute', {pickup_ts}, {dropoff_ts}) AS INTEGER)"
        )
        pickup_year = f"CAST(EXTRACT(YEAR FROM {pickup_ts}) AS INTEGER)"
        pickup_month = f"CAST(EXTRACT(MONTH FROM {pickup_ts}) AS INTEGER)"
        pickup_day = f"CAST(EXTRACT(DAY FROM {pickup_ts}) AS INTEGER)"
        pickup_hour = f"CAST(EXTRACT(HOUR FROM {pickup_ts}) AS INTEGER)"
        pickup_dow = f"CAST(EXTRACT(DOW FROM {pickup_ts}) AS INTEGER)"
    else:
        duration = (
            "CAST("
            "(strftime('%s', tpep_dropoff_datetime) - strftime('%s', tpep_pickup_datetime)) / 60"
            " AS INTEGER)"
        )
        pickup_year = "CAST(strftime('%Y', tpep_pickup_datetime) AS INTEGER)"
        pickup_month = "CAST(strftime('%m', tpep_pickup_datetime) AS INTEGER)"
        pickup_day = "CAST(strftime('%d', tpep_pickup_datetime) AS INTEGER)"
        pickup_hour = "CAST(strftime('%H', tpep_pickup_datetime) AS INTEGER)"
        pickup_dow = "CAST(strftime('%w', tpep_pickup_datetime) AS INTEGER)"

    return f"""
    SELECT
        CAST(VendorID AS INTEGER) AS vendor_id,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        CAST(PULocationID AS INTEGER) AS pulocation_id,
        CAST(DOLocationID AS INTEGER) AS dolocation_id,
        CAST(payment_type AS INTEGER) AS payment_type,
        fare_amount,
        tip_amount,
        total_amount,
        {duration} AS trip_duration_minutes,
        {pickup_year} AS pickup_year,
        {pickup_month} AS pickup_month,
        {pickup_day} AS pickup_day,
        {pickup_hour} AS pickup_hour,
        {pickup_dow} AS pickup_dow,
        _source_file,
        _ingested_at
    FROM {staging}
"""


def date_key_expr(dialect: str) -> str:
    if dialect == "duckdb":
        pickup_ts = "CAST(tpep_pickup_datetime AS TIMESTAMP)"
        return (
            f"CAST(strftime({pickup_ts}, '%Y%m%d') AS INTEGER) * 100"
            f" + CAST(strftime({pickup_ts}, '%H') AS INTEGER)"
        )
    return (
        "CAST(strftime('%Y%m%d', tpep_pickup_datetime) AS INTEGER) * 100"
        " + CAST(strftime('%H', tpep_pickup_datetime) AS INTEGER)"
    )


def pickup_date_expr(dialect: str) -> str:
    if dialect == "duckdb":
        return "CAST(CAST(tpep_pickup_datetime AS TIMESTAMP) AS DATE)"
    return "date(tpep_pickup_datetime)"
