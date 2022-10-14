---------------------------------- STORED PROCEDURES ----------------------------------

CREATE OR REPLACE PROCEDURE insert_fact_air_measurement(
  latitude DECIMAL(12, 8), -- To get geography_id
  longitude DECIMAL(12, 8), -- To get geography_id
  unix_dt INT, -- To get date_id and time_id
  aqi_id INT,
  co REAL,
  no REAL,
  no2 REAL,
  o3 REAL,
  so2 REAL,
  pm2_5 REAL,
  pm10 REAL,
  nh3 REAL
)
LANGUAGE plpgsql
AS $$
DECLARE
  geography_id INT;
  date_id INT;
  time_id INT;
BEGIN
  -- Get geography_id
  SELECT id INTO geography_id
  FROM dim_geography
  WHERE lat = latitude AND lon = longitude
  LIMIT 1;

  -- Get date_id
  SELECT id INTO date_id
  FROM dim_date
  WHERE full_date = to_timestamp(unix_dt)::DATE
  LIMIT 1;

  -- Get time_id
  SELECT id INTO time_id
  FROM dim_time
  WHERE full_time = to_timestamp(unix_dt)::TIME
  LIMIT 1; 

  INSERT INTO 
    fact_air_measurements (geography_id, date_id, time_id, aqi_id, co, no, 
      no2, o3, so2, pm2_5, pm10, nh3)
  VALUES
    (geography_id, date_id, time_id, aqi_id, co, no, 
      no2, o3, so2, pm2_5, pm10, nh3);
END;
$$;
