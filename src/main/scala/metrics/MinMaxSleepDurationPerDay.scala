package metrics

import org.apache.flink.api.common.functions.AggregateFunction
import scheme.AppUsage

class MinMaxSleepDurationPerDay extends AggregateFunction[(String, AppUsage), (String, Long, Long), (String, Double)]{
  override def createAccumulator(): (String, Long, Long) = ("", 0L, 0L)

  override def add(value: (String, AppUsage), accumulator: (String, Long, Long)): (String, Long, Long) =
    (
      value._1,
      accumulator._2 + value._2.duration.getSeconds,
      accumulator._3 + 1
    )

  override def getResult(accumulator: (String, Long, Long)): (String, Double) =
    (
      accumulator._1,
      accumulator._2.toDouble / (accumulator._3 * 3600)
    )

  override def merge(a: (String, Long, Long), b: (String, Long, Long)): (String, Long, Long) =
    (a._1, a._2 + b._2, a._3 + b._3)
}
