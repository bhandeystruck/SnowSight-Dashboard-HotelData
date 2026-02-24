-- Create a Database 
CREATE DATABASE IF NOT EXISTS HOTEL_DB;

-- Create a file format
    --FIELD_OPTIONALLY_ENCLOSED_BY: If data is "Andrew" skip the ""
    --SKIP HEADER: Skips the assigned row while loading
    --NULL_IF: If values are null replace them by the following
CREATE OR REPLACE FILE FORMAT FF_CSV
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('NULL','null','')

-- Create a Stage to load the file and temporary hold
CREATE OR REPLACE STAGE STG_HOTEL_BOOKINGS
    FILE_FORMAT = FF_CSV;HOTEL_DB.PUBLIC.STG_HOTEL_BOOKINGS


--After loading file to stage, create a bronze table
CREATE TABLE IF NOT EXISTS BRONZE_HOTEL_BOOKING(
    booking_id STRING,
    hotel_id STRING,
    hotel_city STRING,
    customer_id STRING,
    customer_name STRING,
    customer_email STRING,
    check_in_date STRING,
    check_out_date STRING,
    room_type STRING,
    num_guests STRING,
    total_amount STRING,
    currency STRING,
    booking_status STRING
)

-- Now that we have the bronze table copy data from staging to bronze
COPY INTO BRONZE_HOTEL_BOOKING
FROM @STG_HOTEL_BOOKINGS
FILE_FORMAT=(FORMAT_NAME = FF_CSV)
ON_ERROR = 'CONTINUE';

--Data has been loaded to bronze tier
--Now creating silver table to alter data types of table
CREATE TABLE SILVER_HOTEL_BOOKINGS(
    booking_id VARCHAR,
    hotel_id VARCHAR,
    hotel_city VARCHAR,
    customer_id VARCHAR,
    customer_name VARCHAR,
    customer_email VARCHAR,
    check_in_date DATE,
    check_out_date DATE,
    room_type VARCHAR,
    num_guests INTEGER,
    total_amount FLOAT,
    currency VARCHAR,
    booking_status VARCHAR
);

-- Checking for errors in email for customers
SELECT customer_email
FROM BRONZE_HOTEL_BOOKING
WHERE 
    customer_email IS NULL 
    OR customer_email = ''
    OR customer_email NOT LIKE '%_@_%._%'

--Check for negative total_amounts
SELECT total_amount
FROM BRONZE_HOTEL_BOOKING
WHERE TRY_TO_NUMBER(total_amount) < 0;

--Check in date is less than check out date
SELECT check_in_date, check_out_date
FROM BRONZE_HOTEL_BOOKING
WHERE TRY_TO_DATE(check_in_date) > TRY_TO_DATE(check_out_date);

--Check for booking status errors
SELECT DISTINCT booking_status
FROM BRONZE_HOTEL_BOOKING;

--Now insert into silver table by fixing all the above errors
INSERT INTO SILVER_HOTEL_BOOKINGS
SELECT
    booking_id,
    hotel_id,
    INITCAP(TRIM(hotel_city)) AS hotel_city,
    customer_id,
    INITCAP(TRIM(customer_name)) AS customer_name,
    CASE
        WHEN customer_email LIKE '%@%.%' THEN LOWER(TRIM(customer_email))
        ELSE NULL
    END AS customer_email,
    TRY_TO_DATE(NULLIF(check_in_date, '')) AS check_in_date,
    TRY_TO_DATE(NULLIF(check_out_date, '')) AS check_out_date,
    room_type,
    num_guests,
    ABS(TRY_TO_NUMBER(total_amount)) AS total_amount,
    currency,
    CASE
        WHEN LOWER(booking_status) in ('confirmeeed', 'confirmd') THEN 'Confirmed'
        ELSE booking_status
    END AS booking_status
    FROM BRONZE_HOTEL_BOOKING
    WHERE
        TRY_TO_DATE(check_in_date) IS NOT NULL
        AND TRY_TO_DATE(check_out_date) IS NOT NULL
        AND TRY_TO_DATE(check_out_date) >= TRY_TO_DATE(check_in_date);

--Lets check for the cleaned data insertion
SELECT * 
FROM SILVER_HOTEL_BOOKINGS
LIMIT 30;


--Since the data has been cleaned now we need to make it business ready
--We need daily bookings, hotel city sales
CREATE TABLE GOLD_AGG_DAILY_BOOKING AS
SELECT
    check_in_date AS date,
    COUNT(*) AS total_booking,
    SUM(total_amount) AS total_revenue
FROM SILVER_HOTEL_BOOKINGS
GROUP BY check_in_date
ORDER BY date;

SELECT * FROM GOLD_AGG_DAILY_BOOKING;

--Identify top revenue cities
CREATE TABLE GOLD_AGG_HOTEL_CITY_SALES AS
SELECT
    hotel_city,
    SUM(total_amount) AS total_revenue
FROM SILVER_HOTEL_BOOKINGS
GROUP BY hotel_city
ORDER BY total_revenue DESC;

SELECT * FROM GOLD_AGG_HOTEL_CITY_SALES

--CREATE GOLD CLEAN TABLE
CREATE TABLE GOLD_BOOKING_CLEAN AS
SELECT
    booking_id,
    hotel_id,
    hotel_city,
    customer_id,
    customer_name,
    customer_email,
    check_in_date,
    check_out_date,
    room_type,
    num_guests,
    total_amount,
    currency,
    booking_status
FROM SILVER_HOTEL_BOOKINGS;