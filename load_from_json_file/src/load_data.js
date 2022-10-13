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

// Make sure to insert the directory of your downloaded data here.
const aqiData = require('../data/aqi_openweather_cancun_2021_jan_01_to_2022_oct_13.json')
const lat = aqiData.coord.lat
const lon = aqiData.coord.lon

async function main() {
  let count = 0
  console.log('Loading records: in progress.')
  for (let measurement of aqiData.list) {
    // Get geographyId
    let q = `
      select id 
      from dim_geography dg 
      where dg.lat = $1 and lon = $2
      limit 1;
    `
    let args = [lat, lon]
    let res = await pool.query(q, args)
    const geographyId = parseInt(res.rows[0].id)
    // console.log(`dim_geography.id: ${geographyId}`)

    // Get dateId
    q = `
      select id
      from dim_date
      where full_date = to_timestamp($1)::date;
    `
    args = [measurement.dt]
    res = await pool.query(q, args)
    const dateId = parseInt(res.rows[0].id)
    // console.log(`dim_date.id: ${dateId}`)

    // Get timeId
    q = `
      select id
      from dim_time
      where full_time = to_timestamp($1)::time;
    `
    args = [measurement.dt]
    res = await pool.query(q, args)
    const timeId = parseInt(res.rows[0].id)
    // console.log(`dim_time.id: ${timeId}`)

    // console.log('---------------------')

    q = `
      INSERT INTO fact_air_measurements
        ( 
          geography_id,
          date_id,
          time_id,
          aqi_id,
          co,
          no,
          no2,
          o3,
          so2,
          pm2_5,
          pm10,
          nh3)
      VALUES
        ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
    `
    args = [
      geographyId,
      dateId,
      timeId,
      measurement.main.aqi,
      measurement.components.co,
      measurement.components.no,
      measurement.components.no2,
      measurement.components.o3,
      measurement.components.so2,
      measurement.components.pm2_5,
      measurement.components.pm10,
      measurement.components.nh3
    ]
    await pool.query(q, args)
    count++
  }
  console.log(`Loading records: Done. Loaded ${count} rows.`)
}

main().then(console.log).catch(console.error)
