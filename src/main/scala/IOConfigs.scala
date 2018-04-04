import sun.security.util.Password

case class Fields(columnName: String, dataType: DataTypes.types)
case class ValueFields(columnName: String, dataTypes: DataTypes.types, aggregator: Aggregator.functionName, name: String)
case class Column(name: String, valueList: List[Any], valueTypeString: DataTypes.types)
case class Partition(partitionField: String, partitionValue: Any, dataType: DataTypes.types)

case class InputConfig(url: String, dbName: String, tableName: String, userName: String, password: String, partition: Partition)
case class OutputConfig(host: String, dbName: String, collectionName: String)
case class ProcessConfig(nonValueFieldList: List[Fields], valueFieldList: List[ValueFields])
case class Table(tableName: String, columnList: List[Column])
