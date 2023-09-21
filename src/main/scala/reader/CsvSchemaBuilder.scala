package reader

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema

object CsvSchemaBuilder {
  def getSchema(): CsvSchema = CsvSchema
    .builder()
    .addColumn("app_name")
    .addColumn("date")
    .addColumn("time")
    .addColumn("duration")
    .build()
}
