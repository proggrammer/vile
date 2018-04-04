import java.io.FileInputStream
import java.util.Properties

object ConfigUtil {
  def fetchConfigs(properties: Properties): (InputConfig, OutputConfig, ProcessConfig) = {
    val partition = Partition(properties.getProperty("partitionField").split(":")(0), null, DataTypes.getDataType(properties.getProperty("partitionField").split(":")(1)))
    val nonValueFieldList: List[Fields] = properties.getProperty("nonValueFieldList").split(",").map(_.trim).map(s => Fields(s.split(":")(0), DataTypes.getDataType(s.split(":")(1)))).toList
    val valueFieldList: List[ValueFields] = properties.getProperty("valueFieldList").split(",").map(_.trim)
      .map(s => ValueFields(s.split(":")(0), DataTypes.getDataType(s.split(":")(1)),
        Aggregator.getAggregator(s.split(":")(2)), s.split(":")(3))).toList

    (InputConfig(properties.getProperty("inputUrl"), properties.getProperty("inputDbName"), properties.getProperty("inputTableName"),
      properties.getProperty("inputUserName"),properties.getProperty("inputPassword"), partition),
     OutputConfig(properties.getProperty("outputUrl"), properties.getProperty("outputDbName"), properties.getProperty("outputTableName")),
      ProcessConfig(nonValueFieldList, valueFieldList))
  }


  def getAppProperties(path: String = null): Properties = {
    val safePath = if(path == null) "src/config.properties" else path
    val prop = new Properties()
    prop.load(new FileInputStream(safePath))
    prop
  }
}
