package prv.saevel.streaming.comparison.akka.streams

import akka.stream.scaladsl.Source

import scala.concurrent.duration.FiniteDuration

trait StreamJoins {

  def join[X, Y, M](s1: Source[X, M], s2: Source[Y, M], joinDuration: FiniteDuration)
                        (condition: (X, Y) => Boolean): Source[(X, Y), M] =
    s1.groupedWithin(Int.MaxValue, joinDuration).flatMapMerge(1, xs =>
      s2.groupedWithin(Int.MaxValue, joinDuration).flatMapMerge(1, ys =>
        Source.fromIterator(() => joinLocal(condition)(xs, ys).iterator)
      )
    )

  private[akka] def joinLocal[X, Y](condition: (X, Y) => Boolean)(s1: Seq[X], s2: Seq[Y]): Seq[(X, Y)] = s1.flatMap(x =>
    s2.filter(y => condition(x, y)).map(y => (x, y))
  )

  protected implicit class SourceJoins[X, M](private val s1: Source[X, M]){

    def joinWith[Y, M2](s2: Source[Y, M], joinDuration: FiniteDuration)
                       (condition: (X, Y) => Boolean): Source[(X, Y), M] = join(s1, s2, joinDuration)(condition)
  }
}
