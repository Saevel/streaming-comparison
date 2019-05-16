package prv.saevel.streaming.comparison.akka

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import org.junit.runner.RunWith
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, WordSpec}
import org.scalatestplus.junit.JUnitRunner
import prv.saevel.streaming.comparison.akka.streams.StreamJoins
import prv.saevel.streaming.comparison.common.tests.scenarios.BasicGenerators

import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class StreamJoinsTest extends WordSpec with Matchers with PropertyChecks with BasicGenerators with StreamJoins with ScalaFutures {

  private val dataSequences: Gen[List[Int]] = smallInts.flatMap(Gen.listOfN(_, Gen.choose(0, 100)))

  private val unmatchedDataSequences: Gen[List[Int]] = smallInts.flatMap(Gen.listOfN(_, Gen.choose(300, 500)))

  private val partiallyMatchingSequences: Gen[List[Int]] = smallInts.flatMap(Gen.listOfN(_, Gen.choose(50, 150)))

  private implicit val actorSystem = ActorSystem("StreamJoinsTest")

  private implicit val materializer = ActorMaterializer()

  "StreamsJoin.join" when {

    "given two streams of data" should {

        "join matching ones" in forAll(dataSequences){ data =>
          val s1 = Source(data)
          val s2 = Source(data)

          val results = join(s1, s2, 3 seconds)(_ == _).runWith(Sink.seq).futureValue

          results.size should be >= data.size
          results.map(_._1) should contain theSameElementsInOrderAs(results.map(_._2))
        }

        "eliminate unmatched ones" in forAll(dataSequences, unmatchedDataSequences){ (seq1, seq2) =>
          val s1 = Source(seq1)
          val s2 = Source(seq2)

          val results = join(s1, s2, 3 seconds)(_ == _).runWith(Sink.seq).futureValue
          results should be(empty)
        }

        "perform correct matches" in forAll(dataSequences, partiallyMatchingSequences){ (seq1, seq2) =>
          val s1 = Source(seq1)
          val s2 = Source(seq2)

          val results = join(s1, s2, 3 seconds)(_ == _).runWith(Sink.seq).futureValue
          results.foreach { case (i, j) =>
            i should equal(j)
            i should be >= 50
            i should be <= 100
          }
        }
    }
  }
}
