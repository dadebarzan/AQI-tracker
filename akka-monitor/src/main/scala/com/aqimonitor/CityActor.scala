package com.aqimonitor

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import scala.collection.immutable.Queue
import java.time.Instant

/**
 * CityActor maintains the in-memory state for a single city.
 * It calculates the EMA (Exponential Moving Average) to detect sudden spikes
 * and monitors static threshold breaches (e.g., AQI > 150).
 */
object CityActor {
  
  sealed trait Command
  
  final case class NewMeasurement(
    city: String,
    aqi: Int,
    co: Double,
    no2: Double,
    pm10: Double,
    pm25: Double,
    temperature: Double,
    timestamp: Long,
    latitude: Double,
    longitude: Double,
    replyTo: Option[ActorRef[MeasurementResult]] = None
  ) extends Command
  
  sealed trait MeasurementResult
  case object MeasurementProcessed extends MeasurementResult
  final case class AlertTriggered(alertType: String, details: String) extends MeasurementResult

  final case class GetStatus(replyTo: ActorRef[CityStatus]) extends Command

  final case class CityStatus(
    city: String,
    currentAqi: Option[Int],
    ema: Option[Double],
    co: Option[Double],
    no2: Option[Double],
    pm10: Option[Double],
    pm25: Option[Double],
    temperature: Option[Double],
    timestamp: Option[Long]
  )
  
  private final case class State(
    cityName: String,
    measurements: Queue[Int] = Queue.empty,
    latestMeasurement: Option[NewMeasurement] = None,
    maxSize: Int = 10,
    unhealthyThreshold: Int = 150,
    spikeMultiplier: Double = 1.3
  ) {
    
    def addMeasurement(measurement: NewMeasurement): State = {
      val updated = measurements.enqueue(measurement.aqi)
      val newQueue = if (updated.size > maxSize) updated.dequeue._2 else updated

      copy(measurements = newQueue, latestMeasurement = Some(measurement))
    }
    
    def calculateEMA(): Option[Double] = {
      if (measurements.isEmpty) None
      else {
        val alpha = 2.0 / (measurements.size + 1)
        val ema = measurements.foldLeft(measurements.head.toDouble) { (acc, value) =>
          alpha * value + (1 - alpha) * acc
        }
        Some(ema)
      }
    }
  }
  
  def apply(cityName: String, config: com.typesafe.config.Config): Behavior[Command] = {
    Behaviors.setup { context =>
      val unhealthyThreshold = config.getInt("aqi-monitor.alert-threshold-unhealthy")
      val spikeMultiplier = config.getDouble("aqi-monitor.alert-threshold-spike-multiplier")
      val windowSize = config.getInt("aqi-monitor.history-window-size")
      
      context.log.info(s"CityActor started for: $cityName (window=$windowSize, unhealthy>$unhealthyThreshold, spike>x$spikeMultiplier)")
      active(State(cityName, maxSize = windowSize, unhealthyThreshold = unhealthyThreshold, spikeMultiplier = spikeMultiplier))
    }
  }
  
  private def active(state: State): Behavior[Command] = {
    Behaviors.receive { (context, message) =>
      message match {
        case m: NewMeasurement =>
          handleMeasurement(context, state, m)
          
        case GetStatus(replyTo) =>
          val status = CityStatus(
            city = state.cityName,
            currentAqi = state.latestMeasurement.map(_.aqi),
            ema = state.calculateEMA(),
            co = state.latestMeasurement.map(_.co),
            no2 = state.latestMeasurement.map(_.no2),
            pm10 = state.latestMeasurement.map(_.pm10),
            pm25 = state.latestMeasurement.map(_.pm25),
            temperature = state.latestMeasurement.map(_.temperature),
            timestamp = state.latestMeasurement.map(_.timestamp)
          )
          replyTo ! status
          Behaviors.same
      }
    }
  }
  
  private def handleMeasurement(
    context: ActorContext[Command],
    state: State,
    measurement: NewMeasurement
  ): Behavior[Command] = {
    
    val newAqi = measurement.aqi
    val ema = state.calculateEMA()
    var alertTriggered = false
    
    // Alert 1: Unhealthy AQI
    if (newAqi > state.unhealthyThreshold) {
      val msg = s"[${state.cityName}] ALERT 1: Unhealthy AQI! " +
        s"Value: $newAqi (threshold: ${state.unhealthyThreshold}) " +
        s"| PM2.5: ${measurement.pm25}, PM10: ${measurement.pm10}"
      context.log.warn(msg)
      measurement.replyTo.foreach(_ ! AlertTriggered("UNHEALTHY", msg))
      alertTriggered = true
    }
    
    // Alert 2: Sudden spike
    // Spike calculation: check if the current AQI exceeds the EMA multiplied by the tolerance threshold
    ema.foreach { emaValue =>
      val spikeThreshold = emaValue * state.spikeMultiplier
      if (newAqi > spikeThreshold) {
        val increase = ((newAqi - emaValue) / emaValue * 100).toInt
        val msg = s"[${state.cityName}] ALERT 2: Sudden spike! " +
          s"Current: $newAqi vs EMA: ${emaValue.toInt} " +
          s"(+$increase%, threshold: ${spikeThreshold.toInt})"
        context.log.warn(msg)
        measurement.replyTo.foreach(_ ! AlertTriggered("SPIKE", msg))
        alertTriggered = true
      }
    }
    
    // Normal measurement
    if (!alertTriggered) {
      context.log.info(
        s"[${state.cityName}] AQI: $newAqi " +
        s"(EMA: ${ema.map(_.toInt).getOrElse("N/A")}, " +
        s"T: ${measurement.temperature}°C)"
      )
      measurement.replyTo.foreach(_ ! MeasurementProcessed)
    }
    
    val newState = state.addMeasurement(measurement)
    active(newState)
  }
}