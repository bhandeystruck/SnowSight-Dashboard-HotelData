# SnowSight Dashboard — Hotel Data

Short README for the SnowSight dashboard project that visualizes hotel bookings data.

## Project structure

- [Dashboard.sql](Dashboard.sql) — Snowsight dashboard SQL (queries / views used in the dashboard)
- [processing.sql](processing.sql) — ETL/processing SQL to transform raw CSV into analytics-ready tables
- [hotel_bookings_raw.csv](hotel_bookings_raw.csv) — Raw source dataset

## Description

This small project contains SQL and raw data used to build a Snowsight (Snowflake) dashboard for hotel bookings. The `processing.sql` script prepares and transforms the raw CSV into tables or views that the dashboard queries. `Dashboard.sql` contains the queries used to power charts and visualizations.

## Prerequisites

- Snowflake account with Snowsight access (web UI)
- Privileges to create databases, schemas, stages, and load data (or ask an administrator)

## Quick setup

1. Load the CSV into a Snowflake table (example outline):

   - Create a stage or use an internal stage
   - Use `COPY INTO` to load `hotel_bookings_raw.csv` into a raw table

2. Run `processing.sql` in Snowsight (or via SnowSQL) to transform raw data into analytics tables/views.

3. Open `Dashboard.sql` in Snowsight and use the queries to build charts and dashboards.

Notes:

- Adjust schema/table names and file locations inside `processing.sql` to match your Snowflake environment.
- If you prefer automation, run the SQL using SnowSQL or via a CI job that connects to Snowflake.


