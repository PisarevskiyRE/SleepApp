package transforms

import org.apache.flink.api.common.functions.MapFunction
import scheme._

/**
 * почему-то map не хочет принимать обобщения
 */
//class ConverterToAppUsage[A <: Record, B <: DataRecord] extends MapFunction[A, B]{
//  override def map(value: A): B = AppUsage(
//        value.app_name,
//        value.date,
//        value.time,
//        value.duration).asInstanceOf[B]
//}


class ConverterToAppUsage extends MapFunction[CsvRecord, AppUsage]{
  override def map(value: CsvRecord): AppUsage = AppUsage(
    value.app_name,
    value.date,
    value.time,
    value.duration)
}