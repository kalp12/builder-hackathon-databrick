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

    # Step 1: Read all events with optimized CSV parsing
    con.execute(f"""
        CREATE TABLE events AS
        SELECT * FROM read_csv_auto('{DATA_DIR}/events.csv')
    """)

    # Get column names dynamically
    cols = [c[0] for c in con.execute("DESCRIBE events").fetchall()]

    # Step 2: Clean events - handle malformed lines
    con.execute(f"""
        CREATE TABLE cleaned_events AS
        SELECT
            {', '.join(cols)},
            COALESCE(revenue, 0) as revenue
        FROM events
        WHERE timestamp IS NOT NULL
        AND user_id IS NOT NULL
    """)

    # Step 3: Link user IDs (from user_id_links.csv)
    con.execute(f"""
        CREATE TABLE user_links AS
        SELECT * FROM read_csv_auto('{DATA_DIR}/user_id_links.csv')
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