package transforms

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import scheme.AppUsage

class EventMerge extends KeyedProcessFunction[Int, AppUsage, AppUsage]{

  private var previousAppUsage: ValueState[AppUsage] = _

  override def open(parameters: Configuration): Unit = {
    val previousAppNameDescriptor: ValueStateDescriptor[AppUsage] = new ValueStateDescriptor("previousAppUsage", classOf[AppUsage])
    previousAppUsage = getRuntimeContext.getState(previousAppNameDescriptor)
  }

  override def processElement(
                               value: AppUsage,
                               ctx: KeyedProcessFunction[Int, AppUsage, AppUsage]#Context,
                               out: Collector[AppUsage]): Unit = {

    val lastAppUsage = previousAppUsage.value()

    if (lastAppUsage != null) {
      if (value.startDate == lastAppUsage.endDate) {
        val mergedAppUsage = AppUsage(
          value.appName,
          lastAppUsage.startDate,
          value.endDate,
          lastAppUsage.duration.plus(value.duration),
          if (value.startTime.isBefore(lastAppUsage.startTime)) value.startTime else lastAppUsage.startTime
        )
        previousAppUsage.update(mergedAppUsage)
        out.collect(mergedAppUsage)
      } else {
        out.collect(value)
        previousAppUsage.update(lastAppUsage)
      }
    } else {
      previousAppUsage.update(value)
    }
  }

}
