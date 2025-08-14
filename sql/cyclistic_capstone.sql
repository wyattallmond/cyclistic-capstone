/* =============================================================================
Cyclistic Capstone — BigQuery SQL
Author: Wyatt Allmond
Project: cyclistic-case-study-468919
Dataset: cyclistic_trips

Purpose:
- Load 12 months of trips, clean anomalies, and build summary tables for analysis.

Key data-quality rules:
- Keep rides where ended_at > started_at
- Remove rides <= 1 minute (accidental unlocks / false starts)
- Remove rides >= 24 hours (system anomalies)
- Keep all stationless (dockless) rides, but exclude NULL station names in "Top Stations"

Outputs:
- Fact table:   cleaned_trips
- Summaries:    summ_member_stats, summ_trips_by_dow, summ_trips_by_month, summ_trips_by_hour,
                summ_rideable_share, summ_top_start_stations, summ_top_end_stations, summ_weekend_split

Run Order:
1) Window raw data
2) Build cleaned data table
3) Sanity checks
4) Build summary data tables
============================================================================= */


-- ==================== Window raw data ====================

-- Preview
SELECT 
  EXTRACT(month from started_at) as month, 
  EXTRACT(year from started_at) as year,
  COUNT(*) as trips
FROM `cyclistic-case-study-468919.cyclistic_trips.all_trips_raw` 
GROUP BY year, month
ORDER BY year, month;

-- Create windowed table to only grab 12 months of ride data
CREATE OR REPLACE TABLE `cyclistic-case-study-468919.cyclistic_trips.all_trips_windowed_12mo` AS
SELECT *
FROM `cyclistic-case-study-468919.cyclistic_trips.all_trips_raw`
WHERE started_at >= TIMESTAMP('2024-08-01')
  AND started_at < TIMESTAMP ('2025-08-01');


-- ==================== Build cleaned data table ====================

CREATE OR REPLACE TABLE `cyclistic-case-study-468919.cyclistic_trips.cleaned_trips` AS
SELECT
  ride_id,
  member_casual, -- 'member' or 'casual'
  rideable_type, -- 'classic_bike' or 'electric_bike'
  started_at,
  ended_at,
  TIMESTAMP_DIFF(ended_at, started_at, MINUTE) AS ride_length_minutes,
  DATE(started_at) AS ride_date,
  EXTRACT(MONTH FROM started_at) AS ride_month,
  EXTRACT(YEAR FROM started_at) AS ride_year,
  FORMAT_TIMESTAMP('%A', started_at) AS day_of_week, -- Monday...Sunday
  EXTRACT(HOUR FROM started_at) AS start_hour,
  CASE WHEN EXTRACT(DAYOFWEEK FROM started_at) IN (1,7) THEN 1 ELSE 0 END AS is_weekend,
  -- bucket by season (Northern Hemisphere)
  CASE
    WHEN EXTRACT(MONTH FROM started_at) IN (12,1,2) THEN 'Winter'
    WHEN EXTRACT(MONTH FROM started_at) IN (3,4,5)  THEN 'Spring'
    WHEN EXTRACT(MONTH FROM started_at) IN (6,7,8)  THEN 'Summer'
    ELSE 'Fall'
  END AS season,
  start_station_name,
  end_station_name,
  start_lat, 
  start_lng, 
  end_lat, 
  end_lng
FROM `cyclistic-case-study-468919.cyclistic_trips.all_trips_windowed_12mo`
WHERE ended_at > started_at -- remove records with start time before end time
  AND TIMESTAMP_DIFF(ended_at, started_at, MINUTE) > 1 -- remove accidental starts
  AND TIMESTAMP_DIFF(ended_at, started_at, HOUR) < 24 -- remove outliers
;


-- ==================== Sanity checks ====================

-- Exactly 12 months?
SELECT COUNT(DISTINCT FORMAT_TIMESTAMP('%Y-%m', started_at)) AS months_present
FROM `cyclistic-case-study-468919.cyclistic_trips.cleaned_trips`;

-- No negatives / sub-minute / >=24h left?
SELECT
  COUNTIF(ended_at <= started_at) AS bad_order,
  COUNTIF(TIMESTAMP_DIFF(ended_at, started_at, MINUTE) <= 1) AS le_1_min,
  COUNTIF(TIMESTAMP_DIFF(ended_at, started_at, HOUR)   >= 24) AS ge_24h
FROM `cyclistic-case-study-468919.cyclistic_trips.cleaned_trips`;

-- Duplicate ride_id?
SELECT COUNT(*) AS dupes
FROM (
  SELECT ride_id FROM `cyclistic-case-study-468919.cyclistic_trips.cleaned_trips` GROUP BY ride_id HAVING COUNT(*) > 1
);


-- ==================== Build summary tables for analysis ====================

--Member vs Casual stats
CREATE OR REPLACE TABLE `cyclistic-case-study-468919.cyclistic_trips.summ_member_stats` AS
SELECT
  member_casual,
  COUNT(*) AS trips,
  ROUND(AVG(ride_length_minutes),2) AS avg_mins,
  ROUND(APPROX_QUANTILES(ride_length_minutes, 100)[OFFSET(10)],2) AS p10_mins,
  ROUND(APPROX_QUANTILES(ride_length_minutes, 100)[OFFSET(50)],2) AS median_mins,
  ROUND(APPROX_QUANTILES(ride_length_minutes, 100)[OFFSET(90)],2) AS p90_mins,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS trip_share_pct
FROM `cyclistic-case-study-468919.cyclistic_trips.cleaned_trips`
GROUP BY member_casual;

-- Day of Week pattern
CREATE OR REPLACE TABLE `cyclistic-case-study-468919.cyclistic_trips.summ_trips_by_dow` AS
SELECT
  member_casual,
  day_of_week,
  EXTRACT(DAYOFWEEK FROM started_at) AS dow_sun1, -- 1=Sun..7=Sat
  CASE day_of_week
    WHEN 'Monday' THEN 1 WHEN 'Tuesday' THEN 2 WHEN 'Wednesday' THEN 3
    WHEN 'Thursday' THEN 4 WHEN 'Friday' THEN 5 WHEN 'Saturday' THEN 6
    ELSE 7  -- Monday thru Sunday sort order
  END AS dow_mon1,
  COUNT(*) AS trips,
  ROUND(AVG(ride_length_minutes),2) AS avg_mins
FROM `cyclistic-case-study-468919.cyclistic_trips.cleaned_trips`
GROUP BY member_casual, day_of_week, dow_sun1, dow_mon1
ORDER BY dow_mon1, member_casual;

--Monthly seasonality
CREATE OR REPLACE TABLE `cyclistic-case-study-468919.cyclistic_trips.summ_trips_by_month` AS
SELECT
  ride_year AS yr,
  ride_month AS mo,
  FORMAT('%04d-%02d', ride_year, ride_month) AS ym,
  season,
  member_casual,
  COUNT(*) AS trips,
  ROUND(AVG(ride_length_minutes), 2) AS avg_mins
FROM `cyclistic-case-study-468919.cyclistic_trips.cleaned_trips`
GROUP BY yr, mo, ym, season, member_casual
ORDER BY yr, mo, member_casual;

-- Hour of day pattern
CREATE OR REPLACE TABLE `cyclistic-case-study-468919.cyclistic_trips.summ_trips_by_hour` AS
SELECT
  member_casual,
  start_hour,
  COUNT(*) AS trips,
  ROUND(AVG(ride_length_minutes), 2) AS avg_mins
FROM `cyclistic-case-study-468919.cyclistic_trips.cleaned_trips`
GROUP BY member_casual, start_hour
ORDER BY member_casual, start_hour;

-- Rideable type share
CREATE OR REPLACE TABLE `cyclistic-case-study-468919.cyclistic_trips.summ_rideable_share` AS
SELECT
  member_casual,
  rideable_type,
  COUNT(*) AS trips,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY member_casual), 1) AS type_share_pct
FROM `cyclistic-case-study-468919.cyclistic_trips.cleaned_trips`
GROUP BY member_casual, rideable_type
ORDER BY member_casual, trips DESC;

-- Top Stations (start)
CREATE OR REPLACE TABLE `cyclistic-case-study-468919.cyclistic_trips.summ_top_start_stations` AS
SELECT * FROM (
  SELECT
    member_casual,
    start_station_name,
    COUNT(*) AS trips,
    ROW_NUMBER() OVER (PARTITION BY member_casual ORDER BY COUNT(*) DESC) AS rn
  FROM `cyclistic-case-study-468919.cyclistic_trips.cleaned_trips`
  WHERE start_station_name IS NOT NULL
  GROUP BY member_casual, start_station_name
)
WHERE rn <= 20;

-- Top Stations (end)
CREATE OR REPLACE TABLE `cyclistic-case-study-468919.cyclistic_trips.summ_top_end_stations` AS
SELECT * FROM (
  SELECT
    member_casual,
    end_station_name,
    COUNT(*) AS trips,
    ROW_NUMBER() OVER (PARTITION BY member_casual ORDER BY COUNT(*) DESC) AS rn
  FROM `cyclistic-case-study-468919.cyclistic_trips.cleaned_trips`
  WHERE end_station_name IS NOT NULL
  GROUP BY member_casual, end_station_name
)
WHERE rn <= 20;

--Weekend vs Weekday
CREATE OR REPLACE TABLE `cyclistic-case-study-468919.cyclistic_trips.summ_weekend_split` AS
WITH base AS (
  SELECT member_casual, is_weekend FROM `cyclistic-case-study-468919.cyclistic_trips.cleaned_trips`
),
agg AS (
  SELECT
    member_casual,
    COUNTIF(is_weekend = 1) AS weekend_trips,
    COUNTIF(is_weekend = 0) AS weekday_trips
  FROM base
  GROUP BY member_casual
)
SELECT
  member_casual,
  weekend_trips,
  weekday_trips,
  ROUND(100 * weekend_trips / NULLIF(weekend_trips + weekday_trips, 0), 1) AS weekend_pct,
  ROUND(100 * weekday_trips / NULLIF(weekend_trips + weekday_trips, 0), 1) AS weekday_pct
FROM agg;
