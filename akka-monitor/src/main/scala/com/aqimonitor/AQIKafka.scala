package com.aqimonitor

import akka.actor.typed.{ActorRef, ActorSystem}
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.RestartSettings
import akka.stream.scaladsl.RestartSource
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import spray.json._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import slick.jdbc.PostgresProfile.api.Database

object AQIKafka {
  
  case class KafkaAQIMessage(
    city: String,
    aqi: Int,
    co: Double,
    no2: Double,
    pm10: Double,
    pm25: Double,
    temperature: Double,
    timestamp: Long,
    latitude: Double,
    longitude: Double
  )
  
  object KafkaAQIMessageProtocol extends DefaultJsonProtocol {
    implicit val messageFormat: RootJsonFormat[KafkaAQIMessage] = jsonFormat10(KafkaAQIMessage)
  }
  
  def start(
    system: ActorSystem[_],
    cityActors: scala.collection.concurrent.Map[String, ActorRef[CityActor.Command]],
    db: Database
  )(implicit ec: ExecutionContext): Unit = {
    
    import KafkaAQIMessageProtocol._
    
    val config = system.settings.config.getConfig("akka.kafka.consumer")
    val consumerSettings = ConsumerSettings(config, new StringDeserializer, new StringDeserializer)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
      .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    
    val restartSettings = RestartSettings(
      minBackoff = 3.seconds,
      maxBackoff = 30.seconds,
      randomFactor = 0.2
    ).withMaxRestarts(10, 1.minute)
    
    val source = RestartSource.withBackoff(restartSettings) { () =>
      Consumer
        .plainSource(consumerSettings, Subscriptions.topics("aqi-raw"))
        .map { record =>
          try {
            val message = record.value().parseJson.convertTo[KafkaAQIMessage]
            
            // Get or create actor for this city
            val cityActor = cityActors.getOrElseUpdate(
              message.city,
              system.systemActorOf(
                CityActor(message.city, system.settings.config),
                s"city-${message.city.replaceAll("[^a-zA-Z0-9]", "-")}"
              )
            )
            
            // Send to actor
            val measurement = CityActor.NewMeasurement(
              city = message.city,
              aqi = message.aqi,
              co = message.co,
              no2 = message.no2,
              pm10 = message.pm10,
              pm25 = message.pm25,
              temperature = message.temperature,
              timestamp = message.timestamp,
              latitude = message.latitude,
              longitude = message.longitude
            )
            
            cityActor ! measurement
            
            // Save to database
            DBWriter.insertMeasurement(db, measurement).recover {
              case ex: Exception if ex.getMessage != null && ex.getMessage.contains("duplicate key") =>
                system.log.warn(s"Duplicate measurement for ${message.city} at ${message.timestamp}, skipping DB insert")
                0
              case ex: Exception =>
                system.log.error(s"Failed to insert measurement for ${message.city}: ${ex.getMessage}")
                0
            }
            
            message.city
          } catch {
            case ex: Exception =>
              system.log.error(s"Error processing Kafka message: ${ex.getMessage}")
              "error"
          }
        }
    }
    
    source
      .toMat(Sink.ignore)(Keep.right)
      .run()(akka.stream.Materializer(system))
    
    system.log.info("🚀 Kafka consumer started, listening to topic: aqi-raw")
  }
}