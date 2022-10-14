CREATE TABLE dim_geography (
  id BIGSERIAL PRIMARY KEY,
  country VARCHAR(100) NOT NULL,
  state VARCHAR(100) NOT NULL,
  city VARCHAR(100) NOT NULL,
  region VARCHAR(100) NOT NULL,
  lat DECIMAL(12, 8) NOT NULL,
  lon DECIMAL(12, 8) NOT NULL
);

CREATE TABLE dim_date (
  id BIGSERIAL PRIMARY KEY,
  full_date DATE NOT NULL,
  calendar_year INT NOT NULL,
  calendar_month INT NOT NULL,
  calendar_day INT NOT NULL,
  month_name VARCHAR(50) NOT NULL,
  day_name VARCHAR(50) NOT NULL,
  day_of_week INT
);

CREATE TABLE dim_time (
  id BIGSERIAL PRIMARY KEY,
  full_time TIME NOT NULL,
  time_hour INT NOT NULL,
  time_minute INT NOT NULL
);

CREATE TABLE dim_aqi (
  id SERIAL PRIMARY KEY,
  aqi INT NOT NULL,
  classification VARCHAR(50)
);

CREATE TABLE fact_air_measurements (
  id BIGSERIAL PRIMARY KEY,
  geography_id INT NOT NULL,
  date_id INT NOT NULL,
  time_id INT NOT NULL,
  aqi_id INT NOT NULL,
  co REAL,
  no REAL,
  no2 REAL,
  o3 REAL,
  so2 REAL,
  pm2_5 REAL,
  pm10 REAL,
  nh3 REAL,

  FOREIGN KEY("geography_id") REFERENCES "dim_geography"("id"),
  FOREIGN KEY("date_id") REFERENCES "dim_date"("id"),
  FOREIGN KEY("time_id") REFERENCES "dim_time"("id"),
  FOREIGN KEY("aqi_id") REFERENCES "dim_aqi"("id")
);