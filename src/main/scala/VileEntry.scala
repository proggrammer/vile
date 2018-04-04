import java.io.FileInputStream
import java.util.Properties

object VileEntry {
  def main(args: Array[String]): Unit = {
    val prop = ConfigUtil.getAppProperties(args(0))
    VileCore.io(prop)
  }
}
