package transforms

import org.apache.flink.api.common.functions.FilterFunction
import scheme.AppUsage

class FilterEvent(appName: String, threshold: Long) extends FilterFunction[AppUsage]{
  override def filter(value: AppUsage): Boolean = {
    value.appName == appName && value.duration.toMillis >= threshold
  }
}
