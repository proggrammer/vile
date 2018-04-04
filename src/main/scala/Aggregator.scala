object Aggregator extends Enumeration {
  def getAggregator(str: String): Aggregator.functionName = {
    if(str.toLowerCase().trim == sum.toString.toLowerCase()){
      return sum
    } else if(str.toLowerCase().trim == average.toString.toLowerCase()){
      return average
    } else null
  }

  type functionName = Value
  val sum      = Value("sum")
  val average  = Value("average")
}