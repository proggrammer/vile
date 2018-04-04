import java.util.Properties

import Aggregator.functionName
import DataTypes.types
import com.mongodb.casbah.commons.Imports._
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable.ListBuffer

case class ValuesWithIndices(name: String, values: List[Any])
case class AggregatedValue(name: String, total: Any)

case class VileDS(tableName: String,
                   smartIndicesForFieldList: Map[String, Map[Any, List[Int]]],
                   valuesWithIndicesList: Map[String, List[Any]],
                   aggregatedValueList: Map[String, Any])
object VileCore extends LazyLogging {
  def io(properties: Properties): Unit ={
    val (inputConfig, outputConfig, processConfig): (InputConfig, OutputConfig, ProcessConfig) = ConfigUtil.fetchConfigs(properties)
    val partitionList: List[Partition] = MySqlIO.getPartitionList(inputConfig)
    for(partition <- partitionList){
      val updatedInputConfig = InputConfig(inputConfig.url, inputConfig.dbName, inputConfig.tableName, inputConfig.userName, inputConfig.password, partition)
      val table: Table = MySqlIO.getTable(updatedInputConfig, processConfig)
      val vileDS = transformTable(table, processConfig)
      MongoIO.persist(vileDS, processConfig, outputConfig, partition)
    }
  }
  def dbObject(vileDS: VileDS): MongoDBObject = {
    val obj:MongoDBObject = MongoDBObject(
      "_id"-> vileDS.tableName,
      "smartIndicesForFieldList" -> vileDS.smartIndicesForFieldList,
      "valuesWithIndicesList"-> vileDS.valuesWithIndicesList,
      "aggregatedValueList"-> vileDS.aggregatedValueList
    )
    return obj
  }
  def transformTable(table: Table, processConfig: ProcessConfig): VileDS = {
    require(processConfig != null , "processConfig is null.")
    require(table != null, " table is null.")
    require(processConfig != null, "processconfig is null")
    require(processConfig.nonValueFieldList != null || processConfig.valueFieldList != null,
      "processConfig.nonValueFieldList != null || processConfig.valueFieldList != null")

    logger.info("Transforming table: "+ table)

    val (smartIndicesForFieldList, valuesWithIndicesList, aggregatedValueList):
      (Map[String, Map[Any, List[Int]]], Map[String, List[Any]], Map[String, Any]) =
      processFields(table, processConfig)
    VileDS(table.tableName, smartIndicesForFieldList, valuesWithIndicesList, aggregatedValueList)
  }


  def processFields(table: Table, processConfig: ProcessConfig):
  (Map[String, Map[Any, List[Int]]], Map[String, List[Any]], Map[String, Any]) = {
    require(table != null && table.columnList != null && !table.columnList.isEmpty, "table should not be null")
    require(table != null, "table should not be null")

    val nonValueFieldList: List[Fields] = processConfig.nonValueFieldList
    val valueFieldList: List[ValueFields] = processConfig.valueFieldList

    val nonValueFieldNameList: List[String] = processConfig.nonValueFieldList.map(_.columnName)
    val valueFieldNameList: List[String] = processConfig.valueFieldList.map(_.columnName)

    val filterPartTableAsMap = if(null == nonValueFieldList || table.columnList == null || table.columnList.isEmpty) null
      else table.columnList.filter(col => nonValueFieldNameList.contains(col.name)).map(col => col.name-> (col.valueList, col.valueTypeString)).toMap

    val  valuePartTableAsMap: Map[String, (List[Any], DataTypes.types, Aggregator.functionName)] =
      if(null == processConfig || null == valueFieldList || table.columnList == null || table.columnList.isEmpty) null
      else
        table.columnList.filter(col => valueFieldNameList.contains(col.name))
          .map(col => col.name -> (col.valueList, col.valueTypeString, valueFieldList.find(vf => vf.columnName == col.name).get.aggregator)).toMap
    val aggregatedMap: Map[String, String] = valueFieldList.map(vf => vf.name->vf.columnName).toMap

    val countTable = table.columnList(0).valueList.size

    val smartIndicesForFieldList: scala.collection.mutable.Map[String, scala.collection.mutable.Map[Any, ListBuffer[Int]]] =  scala.collection.mutable.Map.empty
    val valuesWithIndicesListMutMap =  scala.collection.mutable.Map.empty[String, (ListBuffer[Any], Aggregator.functionName, DataTypes.types)]

    for(rowIndex <- 0 until countTable){
      for(field <- filterPartTableAsMap.keys){
        val v =  filterPartTableAsMap.getOrElse(field, null)._1(rowIndex)
        if(!(smartIndicesForFieldList.contains(field)))
          smartIndicesForFieldList.put(field, scala.collection.mutable.Map.empty[Any, ListBuffer[Int]])

        if(!(smartIndicesForFieldList.getOrElse(field, null).contains(v)))  {
          val mapp = smartIndicesForFieldList.getOrElse(field, null)
          mapp.put(v, ListBuffer())
        }
        val bufferList: ListBuffer[Int] = smartIndicesForFieldList.getOrElse(field, null).getOrElse(v, null)
        bufferList.append(rowIndex)
        val mapp : scala.collection.mutable.Map[Any, ListBuffer[Int]] = smartIndicesForFieldList.getOrElse(field, null)
        mapp.put(v, bufferList)
        smartIndicesForFieldList.put(field, mapp)
      }
      for(valueName <- valuePartTableAsMap.keys){
        val v = valuePartTableAsMap.getOrElse(valueName, null)._1(rowIndex)
        val bufferList =  if(!valuesWithIndicesListMutMap.contains(valueName)) ListBuffer.empty[Any] else valuesWithIndicesListMutMap.getOrElse(valueName, null)._1
        bufferList.append(v)
        valuesWithIndicesListMutMap.put(valueName, (bufferList, valuePartTableAsMap.getOrElse(valueName, null)._3, valuePartTableAsMap.getOrElse(valueName, null)._2))
      }
    }
    val valuesWithIndicesListMap: Map[String, (ListBuffer[Any], Aggregator.functionName, DataTypes.types)] = valuesWithIndicesListMutMap.toMap
    val aggregatedValueList: Map[String, Any] = aggregate(valuesWithIndicesListMap, aggregatedMap)
    (smartIndicesForFieldList.mapValues(_.mapValues(_.toList)).mapValues(_.toMap).toMap, valuesWithIndicesListMutMap.map(kv => kv._1-> kv._2._1.toList).toMap, aggregatedValueList)
  }

  def aggregateEach(tuple: (List[Any], functionName, types)): Any = {
    val list:List[Any] = tuple._1.toList
    val func = tuple._2
    val dataType = tuple._3

    val r =
    if(func == Aggregator.average){
      if(dataType == DataTypes.DoubleDataType){
        list.map(_.asInstanceOf[Double]).sum/list.size
      }
      else if(dataType == DataTypes.LongDataType || dataType == DataTypes.IntegerDataType){
        list.map(_.asInstanceOf[Long]).sum * 1.0/list.size
      }
    }
    else if(func == Aggregator.sum){
      if(dataType == DataTypes.DoubleDataType){
        list.map(_.asInstanceOf[Double]).sum
      }
      else if(dataType == DataTypes.LongDataType){
        list.map(_.asInstanceOf[Long]).sum
      }
      else if(dataType == DataTypes.IntegerDataType){
        list.map(_.asInstanceOf[Int]).sum
      }
    }
    return r
  }

  def aggregate(valuesWithIndicesListMap: Map[String, (ListBuffer[Any], Aggregator.functionName, DataTypes.types)],
                aggregatedMap: Map[String, String]): Map[String, Any] = {
    val result = aggregatedMap.mapValues(colName => aggregateEach(
      valuesWithIndicesListMap.getOrElse(colName, null)._1.toList,
      valuesWithIndicesListMap.getOrElse(colName, null)._2,
      valuesWithIndicesListMap.getOrElse(colName, null)._3
    ))
    result
  }
}