package reader

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
import scheme.{Constant, Record}

import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalTime, ZoneId, ZonedDateTime}
import scala.reflect.ClassTag
import org.apache.flink.api.common.typeinfo.TypeInformation


class CsvReader[A <: Record](
                              schema: CsvSchema,
                              filePath: Path,
                              env: StreamExecutionEnvironment)
                            (implicit typeInfo: TypeInformation[A]) {

  private def getSource(): FileSource[A] = FileSource
    .forRecordStreamFormat(
      CsvReaderFormat.forSchema(schema, Types.GENERIC(typeInfo.getTypeClass)),
      filePath)
    .build()

  def getStream(): DataStreamSource[A] = env
    .fromSource(
      this.getSource(),
      WatermarkStrategy.noWatermarks(),
      Constant.sourceName
    )
}