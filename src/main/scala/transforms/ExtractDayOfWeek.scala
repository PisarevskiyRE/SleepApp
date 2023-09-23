package transforms

import org.apache.flink.api.common.functions.MapFunction
import scheme._

import java.time.{ZoneId, ZonedDateTime}

class ExtractDayOfWeek extends MapFunction[AppUsage, (String, AppUsage)]{
  override def map(value: AppUsage): (String, AppUsage) = {
    val dateTime: ZonedDateTime = ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(value.startDate), ZoneId.of("UTC"))
    (dateTime.getDayOfWeek.toString, value)
  }
}