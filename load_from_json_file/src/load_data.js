const { Pool } = require('pg')
const config = require('../etc/conf.staging.json')

const pool = new Pool({
  user: config.db_user,
  host: config.db_server,
  database: config.db_name,
  password: config.db_passwd,
  port: config.db_port,
  max: config.db_conn_limit,
  ssl: {
    rejectUnauthorized: false
  }
})

const aqiData = require('../data/aqi_openweather_2021_jan_01_to_2022_sep_10.json')
const lat = aqiData.coord.lat
const lon = aqiData.coord.lon

async function main() {
  let count = 0
  console.log('Loading records: in progress.')
  for (let measurement of aqiData.list) {
    let q = `
      INSERT INTO air_measurements
        (lon, lat, aqi, co, no, no2, o3, so2, pm2_5, pm10, nh3, unix_measurement_dt)
      VALUES
        ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
    `
    let args = [
      lon,
      lat,
      measurement.main.aqi,
      measurement.components.co,
      measurement.components.no,
      measurement.components.no2,
      measurement.components.o3,
      measurement.components.so2,
      measurement.components.pm2_5,
      measurement.components.pm10,
      measurement.components.nh3,
      measurement.dt
    ]
    await pool.query(q, args)
    count++
  }
  console.log(`Loading records: Done. Loaded ${count} rows.`)
}

main().then(console.log).catch(console.error)
