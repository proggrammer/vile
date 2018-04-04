object Machine {
  def query(queryFieldName: String, filters: Map[String, List[Any]], vileDS: VileDS, aggregator: Aggregator.functionName,
            dataType: DataTypes.types): Any = {
    //filterout any filedname which doesnt have any filter selected
    val cleanFilter = filters.toList.filter(kv => kv._2 != null && !kv._2.isEmpty)
    //if no filters selected, get values directed from the aggregatedValueList

    if(cleanFilter == null || cleanFilter.isEmpty) return vileDS.aggregatedValueList.getOrElse(queryFieldName, null)
    else{
      //get all contextual indices to the field which needs to be aggregated
      val setOfSet: Set[Int] = cleanFilter.map(kv => kv._2.map(v => vileDS.smartIndicesForFieldList.getOrElse(kv._1, null)
        .getOrElse(v, null).toSet)
        .reduce(_ union _))
        .reduce(_ intersect _)
      //get the values in context
      val resultValueList:List[Any] = setOfSet.toList.map(index => vileDS.valuesWithIndicesList.get(queryFieldName)
        .get(index))
      //aggregate
      val result = VileCore.aggregateEach((resultValueList, aggregator, dataType))
      result
    }
  }
}
