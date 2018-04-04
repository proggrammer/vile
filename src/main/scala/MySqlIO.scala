import java.sql
import java.sql.{DriverManager, ResultSet}
import DataTypes.types

import scala.collection.mutable.ListBuffer

object MySqlIO {
  def getTable(inputConfig: InputConfig, processConfig: ProcessConfig): Table = {
    val driver = "com.mysql.jdbc.Driver"
    var connection:sql.Connection = null
    Class.forName(driver)
    connection = DriverManager.getConnection(inputConfig.url+"/"+inputConfig.dbName, inputConfig.userName, inputConfig.password)
    val statement = connection.createStatement
    val selectQuery: String = "select "+ processConfig.nonValueFieldList.map(_.columnName).mkString(",")+","+processConfig.valueFieldList.map(_.columnName).mkString(",") +" from "+
      inputConfig.tableName + " where "+ inputConfig.partition.partitionField +"=" +
      {if(inputConfig.partition.dataType == DataTypes.StringDataType) "'"+inputConfig.partition.partitionValue+"'"
        else inputConfig.partition.partitionValue}

    val rs = statement.executeQuery(selectQuery)
    val listBuffer: ListBuffer[Any] = ListBuffer.empty[Any]
    val tableName = inputConfig.tableName+"/"+inputConfig.partition.partitionField+"/"+inputConfig.partition.partitionValue

    val allFields: List[(String, DataTypes.types)] = processConfig.nonValueFieldList.map(nonValueField => (nonValueField.columnName, nonValueField.dataType))++processConfig.valueFieldList.map(valueField => (valueField.columnName, valueField.dataTypes))
    val tableValue: Map[String, (DataTypes.types, ListBuffer[Any])]= allFields.map(fd=> fd._1->(fd._2, ListBuffer.empty[Any])).toMap
    while (rs.next) {
      for(fieldName<-tableValue.keys){
        val datatype = tableValue.getOrElse(fieldName, null)._1
        val listBuffer = tableValue.getOrElse(fieldName, null)._2
        listBuffer.append(getAny(rs, datatype, fieldName))
      }
    }
    Table(tableName, tableValue.toList.map(kv => Column(kv._1, kv._2._2.toList, kv._2._1)))
  }

  def getAny(rs: ResultSet, dataType: types, fieldName: String): Any = {
    {
      if(dataType == DataTypes.LongDataType){
        rs.getLong(fieldName)
      } else if(dataType == DataTypes.IntegerDataType) {
        rs.getInt(fieldName)
      } else if(dataType == DataTypes.StringDataType) {
        rs.getString(fieldName)
      } else if(dataType == DataTypes.BooleanDataType) {
        rs.getBoolean(fieldName)
      } else if(dataType == DataTypes.DoubleDataType) {
        rs.getDouble(fieldName)
      } else null
    }
  }

  def getPartitionList(inputConfig: InputConfig): List[Partition] = {
        val driver = "com.mysql.jdbc.Driver"
        var connection:sql.Connection = null
        Class.forName(driver)
        connection = DriverManager.getConnection(inputConfig.url+"/"+inputConfig.dbName, inputConfig.userName, inputConfig.password)
        val statement = connection.createStatement
          val selectQuery: String = "select "+ inputConfig.partition.partitionField +" from "+inputConfig.tableName
          val rs = statement.executeQuery(selectQuery)
          val listBuffer: ListBuffer[Any] = ListBuffer.empty[Any]
          while (rs.next) {
            listBuffer.append(getAny(rs, inputConfig.partition.dataType, inputConfig.partition.partitionField))
          }
    val setBuffer = listBuffer.toSet.toList
    setBuffer.map(v => Partition(inputConfig.partition.partitionField, v, inputConfig.partition.dataType)).toList
  }
}
