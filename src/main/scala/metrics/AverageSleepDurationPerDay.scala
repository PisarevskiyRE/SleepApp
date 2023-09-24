package metrics

import org.apache.flink.api.common.functions.AggregateFunction
import scheme.{AppUsage, Constant}

import java.time.Duration


class AverageSleepDurationPerDay extends AggregateFunction[AppUsage, (Duration, Long), String]{

  override def createAccumulator(): (Duration, Long) = (Duration.ZERO, 0)

  override def add(value: AppUsage, accumulator: (Duration, Long)): (Duration, Long) =
    (
      accumulator._1.plus(value.duration),
      accumulator._2 + 1
    )

  override def getResult(accumulator: (Duration, Long)): String = {
    val result = accumulator._1.toMillis.toDouble  / (accumulator._2 * Constant.oneHours)
    result.toString
  }

  override def merge(a: (Duration, Long), b: (Duration, Long)): (Duration, Long) =
    (
      a._1.plus(b._1),
      a._2 + b._2
    )
}




