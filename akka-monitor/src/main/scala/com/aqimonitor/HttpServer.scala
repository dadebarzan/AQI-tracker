package com.aqimonitor

import akka.actor.typed.{ActorRef, ActorSystem}
import akka.actor.typed.scaladsl.AskPattern._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.util.Timeout
import spray.json.DefaultJsonProtocol._
import spray.json._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import slick.jdbc.PostgresProfile.api.Database

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object HttpServer {

  // Custom JsonFormat per java.sql.Timestamp
  implicit object TimestampFormat extends JsonFormat[java.sql.Timestamp] {
    def write(obj: java.sql.Timestamp) = JsNumber(obj.getTime)
    def read(json: JsValue) = json match {
      case JsNumber(time) => new java.sql.Timestamp(time.toLong)
      case _ => throw DeserializationException("Expected number for Timestamp")
    }
  }

  // Response model for error messages
  final case class ErrorResponse(error: String)

  // Mappers to convert case classes to JSON
  implicit val cityStatusFormat: RootJsonFormat[CityActor.CityStatus] = jsonFormat9(CityActor.CityStatus)
  implicit val aqiHistoryRowFormat: RootJsonFormat[DBWriter.AQIHistoryRow] = jsonFormat11(DBWriter.AQIHistoryRow)
  implicit val errorResponseFormat: RootJsonFormat[ErrorResponse] = jsonFormat1(ErrorResponse)

  def start(
    system: ActorSystem[_], 
    cityActors: scala.collection.concurrent.Map[String, ActorRef[CityActor.Command]],
    db: Database
  )(implicit ec: ExecutionContext): Unit = {
    
    implicit val sys: ActorSystem[_] = system
    implicit val timeout: Timeout = Timeout(3.seconds)

    val route =
      pathPrefix("api" / "aqi" / Segment) { cityName =>
        concat(
          // ROUTE 1: GET /api/aqi/{città} -> Real time data from actor
          pathEnd {
            get {
              cityActors.get(cityName) match {
                case Some(actor) =>
                  onComplete(actor.ask(ref => CityActor.GetStatus(ref))) {
                    case Success(status) => complete(status)
                    case Failure(ex) =>
                      system.log.error(s"Failed to retrieve real-time data for city: $cityName", ex)
                      complete(StatusCodes.InternalServerError -> ErrorResponse("Internal server error while retrieving real-time data."))
                  }
                case None =>
                  complete(StatusCodes.NotFound -> ErrorResponse(s"City '$cityName' not found or no data yet"))
              }
            }
          },
          // ROUTE 2: GET /api/aqi/{città}/history -> database history
          path("history") {
            get {
              // optional 'limit' parameter (e.g., /history?limit=10), default to 24
              parameters("limit".as[Int].withDefault(24)) { limit =>
                
                val minLimit = 1
                val maxLimit = 1000
                
                if (limit < minLimit || limit > maxLimit) {
                  complete(
                    StatusCodes.BadRequest -> 
                    ErrorResponse(s"Invalid 'limit' parameter, must be between $minLimit and $maxLimit")
                  )
                } else {
                  onComplete(DBWriter.getHistory(db, cityName, limit)) {
                    case Success(history) => complete(history)
                    case Failure(ex) =>
                      system.log.error(s"Failed to retrieve history data for city: $cityName", ex)
                      complete(StatusCodes.InternalServerError -> ErrorResponse("Internal server error while retrieving historical data."))
                  }
                }
              }
            }
          }
        )
      }

    val config = system.settings.config
    val host = config.getString("http.host")
    val port = config.getInt("http.port")
    val exposedPort = config.getInt("http.exposed-port")

    Http().newServerAt(host, port).bind(route).onComplete {
      case Success(binding) =>
        val address = binding.localAddress
        system.log.info(s"Server internally bound to ${address.getHostString}:${address.getPort}")
        system.log.info(s"HTTP REST API online at http://localhost:$exposedPort/")
        
      case Failure(ex) =>
        system.log.error(s"Failed to bind HTTP endpoint", ex)
        system.terminate()
    }
  }
}