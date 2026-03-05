package com.aqimonitor

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import scala.collection.immutable.Queue
import java.time.Instant

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
  
  private final case class State(
    cityName: String,
    measurements: Queue[Int] = Queue.empty,
    maxSize: Int = 10,
    unhealthyThreshold: Int = 150,
    spikeMultiplier: Double = 1.3
  ) {
    
    def addMeasurement(aqi: Int): State = {
      val updated = measurements.enqueue(aqi)
      if (updated.size > maxSize) {
        copy(measurements = updated.dequeue._2)
      } else {
        copy(measurements = updated)
      }
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
    
    val newState = state.addMeasurement(newAqi)
    active(newState)
  }
}