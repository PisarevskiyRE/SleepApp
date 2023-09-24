package transforms

import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import scheme.AppUsage

class EventAggregator extends KeyedProcessFunction[Int, AppUsage,AppUsage]{

  val ttlConfig = StateTtlConfig
    .newBuilder(Time.minutes(1))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .build


  private var previousAppUsage: ValueState[AppUsage] = _

  override def open(parameters: Configuration): Unit = {
    val previousAppNameDescriptor: ValueStateDescriptor[AppUsage] = new ValueStateDescriptor("previousAppUsage", classOf[AppUsage])
    previousAppNameDescriptor.enableTimeToLive(ttlConfig)
    previousAppUsage = getRuntimeContext.getState(previousAppNameDescriptor)
  }

  override def processElement(
                               value: AppUsage,
                               ctx: KeyedProcessFunction[Int, AppUsage, AppUsage]#Context,
                               out: Collector[AppUsage]): Unit = {

    val lastAppUsage = previousAppUsage.value()
    val currentAppUsage = value


    if (lastAppUsage == null) {
      previousAppUsage.update(currentAppUsage)
    } else if (currentAppUsage.appName == lastAppUsage.appName) {
      val mergedAppUsage = AppUsage(
        currentAppUsage.appName,
        scala.math.min(
          lastAppUsage.startDate,
          currentAppUsage.startDate),
        scala.math.max(
          lastAppUsage.endDate,
          currentAppUsage.endDate),
        lastAppUsage.duration.plus(currentAppUsage.duration),
        if (currentAppUsage.startTime.isBefore(lastAppUsage.startTime)) currentAppUsage.startTime else lastAppUsage.startTime
      )
      previousAppUsage.update(mergedAppUsage)
    } else {
      previousAppUsage.update(currentAppUsage)
      out.collect(lastAppUsage)
    }
  }
}
