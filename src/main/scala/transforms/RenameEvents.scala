package transforms

import org.apache.flink.api.common.functions.MapFunction
import scheme.AppUsage

class RenameEvents(fromName: String, toName: String) extends MapFunction[AppUsage,AppUsage]{
  override def map(value: AppUsage): AppUsage = {
    if (value.appName.indexOf(fromName) >= 0)
      new AppUsage(toName, value.startDate, value.startDate + value.duration.toMillis, value.duration, value.startTime)
    else value
  }
}
