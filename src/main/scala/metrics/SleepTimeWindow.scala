package metrics

import org.apache.flink.api.common.functions.AggregateFunction
import scheme.AppUsage

import java.time.{Duration, LocalTime}

class SleepTimeWindow extends AggregateFunction[AppUsage, (Duration, Long), String]{
  override def createAccumulator(): (Duration, Long) = (Duration.ZERO, 0L)

  override def add(value: AppUsage, accumulator: (Duration, Long)): (Duration, Long) = {
    val localTime: LocalTime = value.startTime
    val duration: Duration =
      Duration
        .ofHours(localTime.getHour)
        .plusMinutes(localTime.getMinute)
        .plusSeconds(localTime.getSecond)

    (
      accumulator._1.plus(duration),
      accumulator._2 + 1
    )
  }

  override def getResult(accumulator: (Duration, Long)): String = {
    val averageDurationInMillis = accumulator._1.dividedBy(accumulator._2)
    LocalTime.of(
      averageDurationInMillis.toHoursPart,
      averageDurationInMillis.toMinutesPart,
      averageDurationInMillis.toSecondsPart
    ).toString
  }

  override def merge(a: (Duration, Long), b: (Duration, Long)): (Duration, Long) =
    (a._1.plus(b._1), a._2 + b._2)

}
