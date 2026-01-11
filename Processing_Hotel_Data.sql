
-- ===========================================================================
-- Processing_Hotel_Data.sql
-- Purpose: ETL pipeline for hotel booking data (Snowflake).
-- Stages: Bronze (raw), Silver (cleaned/typed), Gold (aggregates/details).
-- Notes: Assumes CSV file(s) are uploaded to the `STG_HOTEL_BOOKINGS` stage.
-- ===========================================================================

-- Create a Database
-- Purpose: Ensure a dedicated database exists for this pipeline.
CREATE DATABASE hotel_db;

-- Create File Format
-- Purpose: Define how incoming CSV files should be parsed when loading.
-- Assumptions: First row is a header and empty strings or literal 'NULL' should be treated as NULL.
CREATE OR REPLACE FILE FORMAT FF_CSV
    TYPE = "CSV"
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '')
    ;

-- Create Landing Zone to Load the Data
-- Purpose: Define an internal stage where CSV files will be uploaded prior to COPY INTO.
CREATE OR REPLACE STAGE STG_HOTEL_BOOKINGS
    FILE_FORMAT = FF_CSV;

-- Create a bronze table for Hotel Booking
-- Purpose: Persist raw CSV rows as-is (all columns as STRING) for lineage and replay.
CREATE TABLE BRONZE_HOTEL_BOOKING 
(
    booking_id  string,
    hotel_id    string,
    hotel_city  string,
    customer_id string,
    customer_name   string,
    customer_email  string,
    check_in_date   string,
    check_out_date  string,
    room_type   string,
    num_guests  string,
    total_amount    string,
    currency    string,
    booking_status  string
);

-- Insert data into the bronze table from the landing stage
-- Purpose: Load CSV data into bronze table. ON_ERROR='CONTINUE' ensures load continues
-- even if some records fail parsing; inspect load history for failures.
COPY INTO BRONZE_HOTEL_BOOKING 
FROM @STG_HOTEL_BOOKINGS
FILE_FORMAT = (FORMAT_NAME = FF_CSV)
ON_ERROR = 'CONTINUE'
;

-- Select sample data from the bronze table
-- Purpose: Quick sanity-check of loaded rows in bronze.
SELECT * FROM HOTEL_DB.PUBLIC.BRONZE_HOTEL_BOOKING LIMIT 10
;

-- Create a silver table for hotel bookings
-- Purpose: Schema-enforced table for cleaned and typed data used for analytics.
CREATE TABLE SILVER_HOTEL_BOOKING
(
    booking_id  VARCHAR,
    hotel_id    INTEGER,
    hotel_city  VARCHAR,
    customer_id VARCHAR,
    customer_name   VARCHAR,
    customer_email  VARCHAR,
    check_in_date   DATE,
    check_out_date  DATE,
    room_type   VARCHAR,
    num_guests  INTEGER,
    total_amount    FLOAT,
    currency    VARCHAR,
    booking_status  VARCHAR 
);

-- Data profiling: hotel_id distribution
-- Purpose: Count occurrences of each hotel_id value in bronze to find nulls or strange values.
select hotel_id, count(*)
from hotel_db.public.bronze_hotel_booking
group by 1
limit 2
;

-- Data profiling: hotel_id content check
-- Purpose: Find hotel_id values containing alphanumeric characters (potentially invalid IDs).
select hotel_id as len_hotel_id, count(*)
from hotel_db.public.bronze_hotel_booking
where hotel_id rlike '[A-Za-z0-9]'
group by 1
;

-- Data profiling: customer_email length distribution
-- Purpose: Detect missing or malformed email addresses; later used to assign 'unknown'.
select length(coalesce(customer_email,'e')) as len_email, count(*)
from hotel_db.public.bronze_hotel_booking
group by 1
order by 2 desc
;

-- Data check: negative total_amount
-- Purpose: Identify transactions with negative amounts for investigation/cleanup.
SELECT total_amount    
FROM hotel_db.public.bronze_hotel_booking
where TRY_TO_NUMBER(total_amount) < 0
;

-- Data check: parseable check_in_date
-- Purpose: Count rows where check_in_date cannot be parsed into a DATE.
select try_to_date(check_in_date), count(*)
from hotel_db.public.bronze_hotel_booking
where try_to_date(check_in_date) is null
group by 1
;
-- Note: previously observed ~196 records with check_in_date null (informational)

-- Data check: parseable check_out_date
-- Purpose: Count rows where check_out_date cannot be parsed into a DATE.
select try_to_date(check_out_date), count(*)
from hotel_db.public.bronze_hotel_booking
where try_to_date(check_out_date) is null
group by 1
;
-- Note: previously observed ~4 records with check_out_date null (informational)

-- Count rows where both dates are null
select count(*)
from hotel_db.public.bronze_hotel_booking
where 
    try_to_date(check_out_date) is null
    and 
    try_to_date(check_in_date) is null
;
-- Note: previously observed 0 rows with both dates null (informational)


-- Count rows where either date is null
select count(*)
from hotel_db.public.bronze_hotel_booking
where 
    try_to_date(check_out_date) is null
    or 
    try_to_date(check_in_date) is null
;
-- Note: previously observed ~200 rows with one or both dates unparseable (informational)


-- Check for date logic violations
-- Purpose: Find rows where the check_out_date is earlier than the check_in_date.
select try_to_date(check_out_date), try_to_date(check_in_date),  count(*)
from hotel_db.public.bronze_hotel_booking
where 
    try_to_date(check_out_date) < try_to_date(check_in_date)
group by 1,2
;
-- Note: previously observed ~71 such records (informational)

-- booking_status distribution
-- Purpose: Inspect the different booking_status values present in the raw data.
select booking_status, count(*)
from hotel_db.public.bronze_hotel_booking
group by 1
;

-- Optionally truncate the silver table before reloading (uncomment if needed)
-- TRUNCATE TABLE hotel_db.public.silver_hotel_booking;

-- Insert cleaned data into the silver zone table
-- Purpose: Transform and type-cast raw bronze data into reliable, typed silver table.
-- Inputs: `hotel_db.public.bronze_hotel_booking` (all STRING fields)
-- Outputs: `hotel_db.public.silver_hotel_booking` with proper types and normalized values.
INSERT INTO hotel_db.public.silver_hotel_booking
SELECT
     booking_id
    ,TRY_TO_NUMBER(TRIM(hotel_id)) as hotel_id
    ,INITCAP(TRIM(hotel_city)) as hotel_city
    ,customer_id
    ,INITCAP(TRIM(customer_name)) as customer_name
    ,CASE 
        WHEN customer_email like '%@%.%' then LOWER(TRIM(customer_email))
        ELSE 'unknown' -- replace malformed/missing emails with sentinel
     END as customer_email
    ,TRY_TO_DATE(NULLIF(check_in_date, '')) as check_in_date
    ,TRY_TO_DATE(NULLIF(check_out_date, '')) as check_out_date
    ,INITCAP(TRIM(room_type)) as room_type
    ,TRY_TO_NUMBER(NULLIF(num_guests,0)) as num_guests
    ,(ABS(NULLIF(TRY_TO_NUMBER(total_amount),0))) as total_amount
    ,UPPER(TRIM(currency)) as currency
    ,CASE 
        WHEN LOWER(TRIM(booking_status)) in ('confirmeeed', 'confirmd') THEN 'Confirmed'
        ELSE INITCAP(TRIM(booking_status))
     END as booking_status
FROM hotel_db.public.bronze_hotel_booking
WHERE 
    TRY_TO_DATE(check_in_date) is not null 
    AND 
    TRY_TO_DATE(check_out_date) is not null
    AND 
    TRY_TO_DATE(check_out_date) >= TRY_TO_DATE(check_in_date)
;

-- Select sample data from the silver zone table
-- Purpose: Verify transformation and type-casting results.
select * from hotel_db.public.silver_hotel_booking
limit 10
;

-- Create Gold zone tables for hotel bookings
-- Purpose: Materialize analytic aggregates used by reporting/dashboarding.

-- Daily booking count
-- Output: `GOLD_AGG_DAILY_HOTEL_BOOKINGS` with total bookings per check_in_date.
CREATE TABLE GOLD_AGG_DAILY_HOTEL_BOOKINGS
AS
SELECT 
    check_in_date as check_in_date,
    COUNT(*) as total_bookings    
FROM hotel_db.public.silver_hotel_booking
GROUP BY check_in_date
ORDER BY check_in_date DESC
;

-- Daily booking revenue
-- Output: `GOLD_AGG_DAILY_HOTEL_SALES` with total revenue per check_in_date.
CREATE TABLE GOLD_AGG_DAILY_HOTEL_SALES
AS
SELECT 
    check_in_date as check_in_date
    ,sum(total_amount) as total_revenue   
FROM hotel_db.public.silver_hotel_booking
GROUP BY check_in_date
ORDER BY check_in_date DESC
;

-- City revenue
-- Output: `GOLD_AGG_CITY_HOTEL_SALES` with total revenue per hotel_city (ranked desc).
CREATE TABLE GOLD_AGG_CITY_HOTEL_SALES
AS
SELECT
     hotel_city
    ,sum(total_amount) as total_revenue
FROM hotel_db.public.silver_hotel_booking
GROUP BY hotel_city
ORDER BY total_revenue DESC
;

-- Gold detail table: cleaned hotel booking records
-- Purpose: Materialized detail table (identical to silver) for downstream consumers.
CREATE TABLE GOLD_DTL_HOTEL_BOOKINGS
AS 
SELECT 
    *
FROM hotel_db.public.silver_hotel_booking
;
