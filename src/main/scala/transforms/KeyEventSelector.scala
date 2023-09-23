package transforms

import org.apache.flink.api.java.functions.KeySelector
import scheme.AppUsage

class KeyEventSelector extends KeySelector[AppUsage, Int]{
  override def getKey(value: AppUsage): Int = 1
}
