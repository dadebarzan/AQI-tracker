package com.aqimonitor

import slick.jdbc.PostgresProfile.api._
import scala.concurrent.{ExecutionContext, Future}
import java.sql.Timestamp
import com.typesafe.config.Config

object DBWriter {
  
  case class AQIHistoryRow(
    id: Option[Long] = None,
    city: String,
    latitude: Option[Double],
    longitude: Option[Double],
    aqi: Int,
    co: Option[Double],
    no2: Option[Double],
    pm10: Option[Double],
    pm25: Option[Double],
    temperature: Option[Double],
    timestamp: Timestamp
  )
  
  class AQIHistoryTable(tag: Tag) extends Table[AQIHistoryRow](tag, "aqi_history") {
    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
    def city = column[String]("city")
    def latitude = column[Option[Double]]("latitude")
    def longitude = column[Option[Double]]("longitude")
    def aqi = column[Int]("aqi")
    def co = column[Option[Double]]("co")
    def no2 = column[Option[Double]]("no2")
    def pm10 = column[Option[Double]]("pm10")
    def pm25 = column[Option[Double]]("pm25")
    def temperature = column[Option[Double]]("temperature")
    def timestamp = column[Timestamp]("timestamp")
    
    def * = (id.?, city, latitude, longitude, aqi, co, no2, pm10, pm25, temperature, timestamp).mapTo[AQIHistoryRow]
  }
  
  val aqiHistory = TableQuery[AQIHistoryTable]
  
  def createDatabase(config: Config): Database = {
    val dbHost = config.getString("database.host")
    val dbPort = config.getInt("database.port")
    val dbName = config.getString("database.name")
    val dbUser = config.getString("database.user")
    val dbPassword = config.getString("database.password")
    val numThreads = config.getInt("database.numThreads")
    
    val url = s"jdbc:postgresql://$dbHost:$dbPort/$dbName"
    
    Database.forURL(
      url = url,
      user = dbUser,
      password = dbPassword,
      driver = "org.postgresql.Driver",
      executor = AsyncExecutor("postgres", numThreads = numThreads)
    )
  }
  
  def insertMeasurement(
    db: Database,
    measurement: CityActor.NewMeasurement
  )(implicit ec: ExecutionContext): Future[Int] = {
    
    val row = AQIHistoryRow(
      city = measurement.city,
      latitude = Some(measurement.latitude),
      longitude = Some(measurement.longitude),
      aqi = measurement.aqi,
      co = Some(measurement.co),
      no2 = Some(measurement.no2),
      pm10 = Some(measurement.pm10),
      pm25 = Some(measurement.pm25),
      temperature = Some(measurement.temperature),
      timestamp = new Timestamp(measurement.timestamp * 1000) // Convert to milliseconds
    )
    
    val insertAction = aqiHistory += row
    db.run(insertAction)
  }
}