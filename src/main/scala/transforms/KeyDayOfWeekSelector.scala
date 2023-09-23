package transforms

import org.apache.flink.api.java.functions.KeySelector
import scheme.AppUsage

class KeyDayOfWeekSelector extends KeySelector[(String, AppUsage), String]{
  override def getKey(value: (String, AppUsage)): String = value._1
}
