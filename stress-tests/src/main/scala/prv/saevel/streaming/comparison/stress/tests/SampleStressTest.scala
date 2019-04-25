package prv.saevel.streaming.comparison.stress.tests

import io.gatling.core.Predef._
import io.gatling.core.scenario.Simulation
import io.gatling.http.Predef._

import scala.concurrent.duration._

class SampleStressTest extends Simulation {

  val httpProtocol = http
    .baseUrl("http://onet.pl")
    .acceptLanguageHeader("pl-PL")
    .acceptHeader("application/html")

  val scenario1 = scenario("First Scenario")
    .exec(
      http("request1").get("/")
    ).pause(5 seconds)

  setUp(
    scenario1.inject(atOnceUsers(10))
  ).protocols(httpProtocol)
}
