package scheme

import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalTime}


trait DataRecord{
  val appName: String
  val startDate: Long
  val endDate: Long
  val duration: Duration
  val startTime: LocalTime
}

case class AppUsage(
                           appName: String,
                           startDate: Long,
                           endDate: Long,
                           duration: Duration,
                           startTime: LocalTime
                         ) extends DataRecord

object AppUsage {
  def apply(app_name: String, date: String, time: String, duration: String): AppUsage = {

    val dateTime = java.time.LocalDateTime.parse(
      date + "|" + time,
      DateTimeFormatter.ofPattern(
        Constant.defaultFormat
      )
    )

    val parts = duration.split(":").map(_.toLong)

    new AppUsage(
      app_name,
      dateTime.toInstant(java.time.ZoneOffset.UTC).toEpochMilli,
      dateTime.toInstant(java.time.ZoneOffset.UTC).toEpochMilli + Duration.ofHours(parts(0)).plusMinutes(parts(1)).plusSeconds(parts(2)).toMillis,
      Duration.ofHours(parts(0)).plusMinutes(parts(1)).plusSeconds(parts(2)),
      dateTime.toLocalTime
    )
  }
}