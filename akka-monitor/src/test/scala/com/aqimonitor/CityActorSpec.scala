package com.aqimonitor

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.matchers.should.Matchers

class CityActorSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with Matchers {

  val testConfig = ConfigFactory.parseString(
    """
      aqi-monitor {
        alert-threshold-unhealthy = 150
        alert-threshold-spike-multiplier = 1.3
        history-window-size = 10
      }
    """)

  private def measurement(city: String, aqi: Int, replyTo: akka.actor.typed.ActorRef[CityActor.MeasurementResult]): CityActor.NewMeasurement =
    CityActor.NewMeasurement(
      city = city, aqi = aqi,
      co = 1.0, no2 = 5.0, pm10 = 20.0, pm25 = 30.0,
      temperature = 18.0, timestamp = 1234567890L,
      latitude = 45.46, longitude = 9.19,
      replyTo = Some(replyTo)
    )

  "CityActor" should {

    "respond with MeasurementProcessed for a normal AQI value" in {
      val probe = createTestProbe[CityActor.MeasurementResult]()
      val actor = spawn(CityActor("testCity", testConfig))

      actor ! measurement("testCity", 50, probe.ref)

      probe.expectMessage(CityActor.MeasurementProcessed)
    }

    "trigger UNHEALTHY alert when AQI exceeds the threshold" in {
      val probe = createTestProbe[CityActor.MeasurementResult]()
      val actor = spawn(CityActor("alertCity", testConfig))

      actor ! measurement("alertCity", 160, probe.ref)

      val result = probe.receiveMessage()
      result shouldBe a[CityActor.AlertTriggered]
      result.asInstanceOf[CityActor.AlertTriggered].alertType shouldBe "UNHEALTHY"
    }

    "trigger SPIKE alert when AQI jumps above EMA * multiplier" in {
      val probe = createTestProbe[CityActor.MeasurementResult]()
      val actor = spawn(CityActor("spikeCity", testConfig))

      // First measurement: AQI 50 → sets EMA to 50
      actor ! measurement("spikeCity", 50, probe.ref)
      probe.expectMessage(CityActor.MeasurementProcessed)

      // Second measurement: AQI 100 → EMA is 50, multiplier is 1.3 → threshold is 65 → should trigger SPIKE alert
      actor ! measurement("spikeCity", 100, probe.ref)

      val result = probe.receiveMessage()
      result shouldBe a[CityActor.AlertTriggered]
      result.asInstanceOf[CityActor.AlertTriggered].alertType shouldBe "SPIKE"
    }
  }
}