-- DIM_GEOGRAPHY

INSERT INTO DIM_GEOGRAPHY
  (COUNTRY, STATE, CITY, REGION, LAT, LON)
VALUES 
  ('México', 'Quintana Roo', 'Cancún', 'Centro', 21.1215, -86.9193),
  ('México', 'CDMX', 'CDMX', 'Ángel de la Independencia', 19.427, -99.1681);

-- DIM_DATE

DO $$
DECLARE 
  initial_date DATE := '2021-01-01';
  final_date DATE := '2030-12-31';
  curr_date DATE;
BEGIN
  FOR curr_date IN select generate_series(initial_date, final_date,'1 day') LOOP
    INSERT INTO DIM_DATE
      (FULL_DATE, CALENDAR_YEAR, CALENDAR_MONTH, CALENDAR_DAY, MONTH_NAME, DAY_NAME, DAY_OF_WEEK)
    VALUES (
      curr_date,
      EXTRACT(YEAR FROM curr_date),
      EXTRACT(MONTH FROM curr_date),
      EXTRACT(DAY FROM curr_date),
      TRIM(TO_CHAR(curr_date, 'month')),
      TRIM(TO_CHAR(curr_date, 'day')),
      EXTRACT(ISODOW FROM curr_date));
  END LOOP;
END; $$

-- DIM_TIME

DO $$
DECLARE 
  initial_time TIMESTAMP := '2022-01-01 00:00:00';
  final_time TIMESTAMP := '2022-01-01 23:59:00';
  curr_time TIME;
BEGIN
  FOR curr_time IN select generate_series(initial_time, final_time,'1 minute') LOOP
    INSERT INTO DIM_TIME 
      (FULL_TIME, TIME_HOUR, TIME_MINUTE)
    VALUES (
      curr_time,
      EXTRACT(HOUR FROM curr_time),
      EXTRACT(MINUTE FROM curr_time));
  END LOOP;
END; $$

-- DIM_AQI

INSERT INTO DIM_AQI 
  (AQI, CLASSIFICATION)
VALUES 
  (1, 'Good'),
  (2, 'Fair'),
  (3, 'Moderate'),
  (4, 'Poor'),
  (5, 'Very Poor');