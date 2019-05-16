package prv.saevel.streaming.comparison.common.tests.utils

import cats.implicits._
import cats.{Functor, Semigroupal}
import org.scalacheck.Gen
import org.scalatest.Suite

/**
  * A util for testing
  */
trait StaticPropertyChecks { self: Suite =>

  protected val maxRetries = 10

  implicit val generatorSemigroupal: Semigroupal[Gen] = new Semigroupal[Gen] {
    override def product[A, B](fa: Gen[A], fb: Gen[B]): Gen[(A, B)] = for {
      r1 <- fa
      r2 <- fb
    } yield (r1, r2)
  }

  implicit val generatorFunctor: Functor[Gen] = new Functor[Gen] {
    override def map[A, B](fa: Gen[A])(f: A => B): Gen[B] = fa.map(f)
  }

  protected def forOneOf[X](generator: Gen[X])(f: X => Unit): Unit = f(generator.evaluate(maxRetries))

  protected def forOneOf[A, B](g1: Gen[A], g2: Gen[B])(f: (A, B) => Unit): Unit = (g1, g2).mapN(f).evaluate(maxRetries)

  protected def forOneOf[A, B, C](g1: Gen[A], g2: Gen[B], g3: Gen[C])(f: (A, B, C) => Unit): Unit =
    (g1, g2, g3).mapN(f).evaluate(maxRetries)

  protected def forOneOf[A, B, C, D](g1: Gen[A], g2: Gen[B], g3: Gen[C], g4: Gen[D])(f: (A, B, C, D) => Unit): Unit =
    (g1, g2, g3, g4).mapN(f).evaluate(maxRetries)

  protected def forOneOf[A, B, C, D, E](g1: Gen[A], g2: Gen[B], g3: Gen[C], g4: Gen[D], g5: Gen[E])(f: (A, B, C, D, E) => Unit): Unit =
    (g1, g2, g3, g4, g5).mapN(f).evaluate(maxRetries)

  implicit class GeneratorEvaluation[X](g: Gen[X]) {
    def evaluate(maxRetries: Int): X = {
      var result: Option[X] = None
      var i = 0

      while(i < maxRetries && result == None) {
        result = g.sample
        i += 1
      }

      result match {
        case Some(x) => x
        case None => fail(s"Failed to generate data in $maxRetries retries.")
      }
    }
  }
}
