package com.aqimonitor

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.actor.typed.scaladsl.AskPattern._
import akka.util.Timeout
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import spray.json.RootJsonFormat

import com.aqimonitor.HttpServer._

import scala.concurrent.duration._
import scala.util.{Failure, Success}

class HttpServerSpec extends AnyWordSpec with Matchers with ScalatestRouteTest {

  // Typed actor system for ask pattern
  val testKit = ActorTestKit()
  implicit val typedSystem: akka.actor.typed.ActorSystem[_] = testKit.system
  implicit val timeout: Timeout = Timeout(3.seconds)

  override def afterAll(): Unit = {
    testKit.shutdownTestKit()
    super.afterAll()
  }

  // Stub actor that responds to GetStatus with fixed data
  private val stubActor = testKit.spawn(
    Behaviors.receiveMessage[CityActor.Command] {
      case CityActor.GetStatus(replyTo) =>
        replyTo ! CityActor.CityStatus(
          city = "milan",
          currentAqi = Some(42),
          ema = Some(40.0),
          co = Some(1.2),
          no2 = Some(5.0),
          pm10 = Some(20.0),
          pm25 = Some(30.0),
          temperature = Some(18.5),
          timestamp = Some(1234567890L)
        )
        Behaviors.same
      case _ => Behaviors.same
    }
  )

  private val cityActors = scala.collection.concurrent.TrieMap[String, akka.actor.typed.ActorRef[CityActor.Command]](
    "milan" -> stubActor
  )

  private val route = HttpServer.createRoute(
    cityActors,
    null.asInstanceOf[slick.jdbc.PostgresProfile.backend.Database]
  )

  "GET /api/aqi/{city}" should {

    "return 200 OK with a valid CityStatus JSON for a known city" in {
      Get("/api/aqi/milan") ~> route ~> check {
        status shouldBe StatusCodes.OK
        val result = responseAs[CityActor.CityStatus]
        result.city shouldBe "milan"
        result.currentAqi shouldBe Some(42)
        result.ema shouldBe Some(40.0)
        result.temperature shouldBe Some(18.5)
      }
    }

    "return 404 Not Found for an unknown city" in {
      Get("/api/aqi/unknown") ~> route ~> check {
        status shouldBe StatusCodes.NotFound
        val result = responseAs[HttpServer.ErrorResponse]
        result.error should include("not found")
      }
    }
  }
}