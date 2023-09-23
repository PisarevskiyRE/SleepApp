package transforms

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import scheme.AppUsage

class EventAggregator extends KeyedProcessFunction[Int, AppUsage,AppUsage]{

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
      if (value.appName == lastAppUsage.appName) {

        val mergedAppUsage = AppUsage(
          value.appName,
          scala.math.min(
            lastAppUsage.startDate,
            value.startDate),
          scala.math.max(
            lastAppUsage.endDate,
            value.endDate),
          lastAppUsage.duration.plus(value.duration),
          if (value.startTime.isBefore(lastAppUsage.startTime)) value.startTime else lastAppUsage.startTime
        )
        previousAppUsage.update(mergedAppUsage)
      } else {
        out.collect(lastAppUsage)
        previousAppUsage.update(value)
      }
    } else {
      previousAppUsage.update(value)
    }
  }
}
