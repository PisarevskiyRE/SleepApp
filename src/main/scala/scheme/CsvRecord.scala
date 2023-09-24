package scheme

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty


trait Record{
  var app_name: String
  var date: String
  var time: String
  var duration: String
}


final case class CsvRecord(
                         @JsonProperty("app_name") var app_name: String,
                         @JsonProperty("date") var date: String,
                         @JsonProperty("time") var time: String,
                         @JsonProperty("duration") var duration: String,
                       ) extends Record
