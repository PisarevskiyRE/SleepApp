import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import reader.{CsvReader, CsvSchemaBuilder}
import scheme._
import transforms.{ConverterToAppUsage, Watermark}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.csv.CsvReaderFormat
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema
import org.apache.flink.streaming.api.datastream.{DataStreamSource, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalTime, ZoneId, ZonedDateTime}

object Main extends App {

  val env = StreamExecutionEnvironment
    .getExecutionEnvironment


  implicit val typeInfo: TypeInformation[CsvRecord] = Types.GENERIC(classOf[CsvRecord])

  val csvReader = new CsvReader[CsvRecord](
    CsvSchemaBuilder.getSchema(),
    new Path(Constant.path),
    env
  )

  val rawStream: DataStreamSource[CsvRecord] = csvReader.getStream()



  val typedStream = rawStream
    .map(new ConverterToAppUsage)
    .assignTimestampsAndWatermarks(
      new Watermark(100).getWatermarkStrategy[AppUsage]()
    )





  typedStream.print()
  env.execute()


}
