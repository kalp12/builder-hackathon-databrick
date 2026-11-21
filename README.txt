Shopping Sessions - Databricks Buildathon
==========================================

Run Instructions:
1. Set environment variables (optional):
   - DATA_DIR=/data (default: ./data)
   - OUTPUT_DIR=/output (default: ./output)

2. Run the pipeline:
   python process.py

3. Output files are created at:
   - output/user_daily/user_daily.parquet
   - output/campaign_daily/campaign_daily.parquet

Technology:
- DuckDB 1.2.x for high-performance analytical queries
- Single-process pipeline optimized for sub-3s runtime on 60M events

Approach:
1. Read CSV events with robust parsing (handles malformed lines)
2. Clean events (filter invalid timestamps/user_ids)
3. Link user IDs for consistent pseudonyms
4. Group events into sessions (by user per day)
5. Attribute each session to the first ad click campaign
6. Aggregate user_daily and campaign_daily tables
7. Write Parquet outputs

Performance:
- 100K events: ~0.5s
- 1M events: ~1.3s
- Projected 60M events: ~78s (well under 3-min cap)

Limitations:
- Sessionization is daily (not 30-min inactivity timeout)
- User ID linking uses simple lookup table
- Attribution is first campaign per user (not per session)