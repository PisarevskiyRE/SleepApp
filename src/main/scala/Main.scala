import metrics.{AverageSleepDurationPerDay, MinMaxSleepDurationPerDay, SleepTimeWindow}
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import reader.{CsvReader, CsvSchemaBuilder}
import scheme._
import transforms._

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

  val clearStream = typedStream
    .map(new RenameEvents("Screen", "Sleep"))
    .keyBy(new KeyEventSelector)
    .process(new EventAggregator)

  val filteredStream= clearStream
    .filter(new FilterEvent("Sleep", Constant.thresholdSleep))
    .keyBy(new KeyEventSelector)
    .process(new EventMerge)

  val averageSleepDurationPerDay = filteredStream
    .keyBy(new KeyEventSelector)
    .windowAll(TumblingEventTimeWindows.of(Time.days(3)))
    .aggregate(new AverageSleepDurationPerDay())

  val sleepTimeWindow = filteredStream
    .keyBy(new KeyEventSelector)
    .windowAll(TumblingEventTimeWindows.of(Time.days(3)))
    .aggregate(new SleepTimeWindow)

  val minMaxSleepDurationPerDay = filteredStream
    .map(new ExtractDayOfWeek)
    .keyBy(new KeyDayOfWeekSelector)
    .window(TumblingEventTimeWindows.of(Time.days(3)))
    .aggregate(new MinMaxSleepDurationPerDay)


  averageSleepDurationPerDay.print("AverageSleepDurationPerDay -> ")
  sleepTimeWindow.print("SleepTimeWindow -> ")
  minMaxSleepDurationPerDay.print("minMaxSleepDurationPerDay -> ")

  env.execute("SleepApp")
}
