#!/usr/bin/env python3
"""
Shopping Sessions - DuckDB implementation (Optimized)
Processes click-stream events into user_daily and campaign_daily Parquet tables.
"""

import os
import duckdb
from pathlib import Path

DATA_DIR = Path(os.environ.get('DATA_DIR', Path(__file__).parent / 'data'))
OUTPUT_DIR = Path(os.environ.get('OUTPUT_DIR', Path(__file__).parent / 'output'))

def main():
    # Ensure output directories exist
    (OUTPUT_DIR / 'user_daily').mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / 'campaign_daily').mkdir(parents=True, exist_ok=True)

    # Connect with optimizations
    con = duckdb.connect(database=':memory:')

    # Enable DuckDB optimizations
    con.execute("SET threads TO 8")
    con.execute("SET enable_progress_bar TO false")

    print(f"Data directory: {DATA_DIR}")
    print(f"Data exists: {DATA_DIR.exists()}")

    if DATA_DIR.exists():
        files = list(DATA_DIR.rglob('*'))
        print(f"Files found: {len(files)}")
        for f in files:
            if f.is_file():
                print(f"  {f}")

    # Step 1: Find and read all event data
    # Events are in /data/events/ as part-XXXXX.csv files
    # Also try /data/clicks/ as alternative location
    events_pattern = f"{DATA_DIR}/events/*.csv"

    # Check which directory exists
    try:
        con.execute(f"""
            CREATE TABLE events_raw AS
            SELECT * FROM read_csv_auto('{events_pattern}', union_by_name=true)
        """)
        print(f"Loaded events from: {events_pattern}")
    except Exception as e:
        print(f"Failed to load from events directory: {e}")
        # Try clicks directory
        clicks_pattern = f"{DATA_DIR}/clicks/*.csv"
        con.execute(f"""
            CREATE TABLE events_raw AS
            SELECT * FROM read_csv_auto('{clicks_pattern}', union_by_name=true)
        """)
        print(f"Loaded events from: {clicks_pattern}")

    # Get column names
    cols = [c[0] for c in con.execute("DESCRIBE events_raw").fetchall()]

    # Step 2: Clean events - handle malformed lines
    con.execute("""
        CREATE TABLE cleaned_events AS
        SELECT
            user_id,
            timestamp,
            event_type,
            campaign_id,
            COALESCE(revenue, 0) as revenue,
            value
        FROM events_raw
        WHERE timestamp IS NOT NULL
        AND user_id IS NOT NULL
    """)

    # Step 3: Link user IDs (from /data/links/ directory)
    links_pattern = f"{DATA_DIR}/links/*.csv"
    try:
        con.execute(f"""
            CREATE TABLE user_links AS
            SELECT * FROM read_csv_auto('{links_pattern}', union_by_name=true)
        """)
        print(f"Loaded user links from: {links_pattern}")
    except Exception as e:
        print(f"Could not load from links directory: {e}")
        # Create empty table if no links file
        con.execute("""
            CREATE TABLE user_links AS
            SELECT '' as user_id, '' as canonical_user_id
            WHERE FALSE
        """)

    # Step 4: Build sessions with user linking
    # Group by user per day (simplified sessionization)
    con.execute("""
        CREATE TABLE sessions AS
        SELECT
            COALESCE(ul.canonical_user_id, e.user_id) as user_id,
            DATE_TRUNC('day', e.timestamp) as session_date,
            COUNT(*) as events,
            SUM(CASE WHEN e.event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
            SUM(e.revenue) as session_revenue
        FROM cleaned_events e
        LEFT JOIN user_links ul ON e.user_id = ul.user_id
        GROUP BY 1, 2
    """)

    # Step 5: Get attribution (first campaign for each user)
    con.execute("""
        CREATE TABLE user_first_campaign AS
        SELECT
            COALESCE(ul.canonical_user_id, e.user_id) as user_id,
            FIRST_VALUE(e.campaign_id) OVER (
                PARTITION BY COALESCE(ul.canonical_user_id, e.user_id)
                ORDER BY e.timestamp
            ) as attribution_campaign
        FROM cleaned_events e
        LEFT JOIN user_links ul ON e.user_id = ul.user_id
        WHERE e.event_type = 'ad_click'
    """)

    # Step 6: Build user_daily table
    con.execute("""
        CREATE TABLE user_daily AS
        SELECT
            user_id,
            session_date as date,
            1 as session_count,
            events,
            purchases,
            session_revenue as revenue
        FROM sessions
    """)

    # Step 7: Build campaign_daily by joining attribution
    con.execute("""
        CREATE TABLE campaign_daily AS
        SELECT
            uc.attribution_campaign as campaign_id,
            ud.date as date,
            COUNT(*)::BIGINT as sessions,
            SUM(ud.purchases) as assisted_conversions,
            SUM(ud.revenue) as cost
        FROM user_daily ud
        JOIN user_first_campaign uc ON ud.user_id = uc.user_id
        GROUP BY 1, 2
    """)

    # Step 8: Write Parquet outputs
    user_daily_path = str(OUTPUT_DIR / 'user_daily' / 'user_daily.parquet')
    campaign_daily_path = str(OUTPUT_DIR / 'campaign_daily' / 'campaign_daily.parquet')

    con.execute(f"COPY (SELECT * FROM user_daily) TO '{user_daily_path}'")
    con.execute(f"COPY (SELECT * FROM campaign_daily) TO '{campaign_daily_path}'")

    con.close()

if __name__ == '__main__':
    main()