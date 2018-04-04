import org.scalatest.FunSuite

class VileTest extends FunSuite{
  val (table, processConfig) = dummyTableProcessConfig()
  val prop = ConfigUtil.getAppProperties()
  def dummyTableProcessConfig():(Table, ProcessConfig) = {

    val breedList: List[Any] = List("German Sheperd", "Labrador Retriever", "Beagle", "Poodle", "Labrador Retriever", "German Sheperd", "Labrador Retriever", "Beagle", "Poodle")
    val valueTypeStringBreed = DataTypes.StringDataType
    val nameBreed = "breed"
    val breedCol = Column(nameBreed, breedList, valueTypeStringBreed)

    val colorList: List[Any] = List("white", "black", "blue", "white", "yellow", "black", "blue", "white", "yellow")
    val valueTypeStringColor = DataTypes.StringDataType
    val nameColor = "color"
    val colorCol = Column(nameColor, colorList, valueTypeStringColor)

    val locationList: List[Any] = List("siliguri", "kolkata", "patna", "delhi", "siliguri", "siliguri", "patna", "delhi", "siliguri")
    val valueTypeStringLocation = DataTypes.StringDataType
    val nameLocation = "location"
    val locCol = Column(nameLocation, locationList, valueTypeStringLocation)

    val priceList: List[Any] = List(222.55, 399.7, 1111.99, 6666.7, 5656.5, 222.55, 399.7, 6666.7, 5656.5)
    val valueTypeStringPrice = DataTypes.DoubleDataType
    val namePrice = "price"
    val priceCol = Column(namePrice, priceList, valueTypeStringPrice)

    val profitList: List[Any] = List(2.3, 4.6, 5.5, 6.4, 7.7, 4.6, 5.5, 6.4, 7.7)
    val valueTypeStringProfit = DataTypes.DoubleDataType
    val nameProfit = "profit"
    val profitCol = Column(nameProfit, profitList, valueTypeStringProfit)


    val weightInKGList: List[Any] = List(04.2,2.1,1.4,3.9, 4.3,2.1,1.4,3.9, 4.3)
    val valueTypeStringWeight = DataTypes.DoubleDataType
    val nameWeight = "weight"
    val wtCol = Column(nameWeight, weightInKGList, valueTypeStringWeight)

    val dateList: List[Any] = List("2017-01-01","2017-01-01","2017-01-01","2017-01-01", "2017-01-02","2017-01-02","2017-01-02","2017-01-02", "2017-01-02")
    val valueTypeStringDate = DataTypes.StringDataType
    val nameDate = "date"
    val dateCol = Column(nameDate, dateList, valueTypeStringDate)

    val tableName = "dogs"

    val inputConfig:InputConfig = null;
    val partition: Partition = null;
    val listTesting:List[Any] = List("ok", 77, 77.7)

    val columns: List[Column] = List(breedCol, colorCol, locCol, priceCol, profitCol, wtCol,dateCol)
    val table = Table("tt2", columns)
    val nonValueFieldList = List(Fields(nameBreed, valueTypeStringBreed),
                                  Fields(nameColor, valueTypeStringColor),
                                  Fields(nameLocation, valueTypeStringLocation))
    val valueFieldList = List(ValueFields(namePrice, valueTypeStringPrice, Aggregator.sum, "revenue"),
                              ValueFields(nameProfit, valueTypeStringProfit, Aggregator.sum, "totalProfit"),
                              ValueFields(nameWeight, valueTypeStringPrice, Aggregator.average, "avgWeight"))

    val processConfig = ProcessConfig(nonValueFieldList, valueFieldList)
    (table, processConfig)
  }
  test("Running transformation for vile") {
    val vileDS = VileCore.transformTable(table, processConfig)
    vileDS
  }

  test("running end to end vile") {
    VileCore.io(prop)
  }
}
