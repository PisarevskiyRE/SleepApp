package reader

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.csv.CsvReaderFormat
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import scheme.{Constant, Record}


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