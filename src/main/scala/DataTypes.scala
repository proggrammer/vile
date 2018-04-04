import scala.Enumeration
object DataTypes extends Enumeration {
  def getDataType(str: String): DataTypes.types = {
    if(str.trim.toLowerCase() == IntegerDataType.toString.toLowerCase()){
      return IntegerDataType
    } else if(str.trim.toLowerCase() == LongDataType.toString.toLowerCase()){
      return LongDataType
    } else if(str.trim.toLowerCase() == StringDataType.toString.toLowerCase()){
      return StringDataType
    } else if(str.trim.toLowerCase() == BooleanDataType.toString.toLowerCase()){
      return BooleanDataType
    } else if(str.trim.toLowerCase() == DoubleDataType.toString.toLowerCase()){
      return DoubleDataType
    } else return null
  }

  type types = Value
  val IntegerDataType = Value("Integer")
  val LongDataType  = Value("Long")
  val StringDataType = Value("String")
  val BooleanDataType = Value("Boolean")
  val DoubleDataType = Value("Double")
}
