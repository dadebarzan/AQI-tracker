package com.aqimonitor

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import scala.concurrent.ExecutionContext
import scala.io.StdIn
import slick.jdbc.PostgresProfile.api.Database

object Main extends App {
  
  val system = ActorSystem(Behaviors.empty, "AQIMonitorSystem")
  implicit val ec: ExecutionContext = system.executionContext
  
  system.log.info("=" * 60)
  system.log.info("AQI Monitor System Starting...")
  system.log.info("=" * 60)
  
  // Initialize database
  val db = DBWriter.createDatabase(system.settings.config)
  system.log.info("Database connection initialized")
  
  // City actors map
  val cityActors = scala.collection.concurrent.TrieMap[String, akka.actor.typed.ActorRef[CityActor.Command]]()
  
  // Start Kafka consumer
  AQIKafka.start(system, cityActors, db)

  // Start HTTP server
  HttpServer.start(system, cityActors, db)
  
  system.log.info("=" * 60)
  system.log.info("System ready! Monitoring air quality...")
  system.log.info("=" * 60)
  
  // Graceful shutdown hook
  sys.addShutdownHook {
    system.log.info("Shutting down AQI Monitor System...")
    db.close()
    system.terminate()
  }
  
  // Keep application running
  scala.concurrent.Await.result(system.whenTerminated, scala.concurrent.duration.Duration.Inf)
}