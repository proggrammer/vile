import com.mongodb.casbah.MongoConnection
import com.mongodb.casbah.Imports._
object MongoIO {
  def persist(vileDS: VileDS, processConfig: ProcessConfig, outputConfig: OutputConfig, partition: Partition) = {
    val monoDbObject = VileCore.dbObject(vileDS)
    val mongoConn = MongoConnection(outputConfig.host)
    mongoConn.getDB(outputConfig.dbName).getCollection(outputConfig.collectionName).save(monoDbObject)
  }
}
