/**
  * Created by stefano on 08/05/17.
  */
object Settings {
  private val MONGO_USERNAME = "lanser"
  private val MONGO_PASSWORD = "nakamotocatenE"
  private val MONGO_HOST = "localhost";
  private val MONGO_DB = "blockchain"
  private val MONGO_COL = "transaction_test"

  def HDFS_DIR = "hdfs://192.167.155.71:9000/stefano.lande/"
  def HDFS_OUT = "hdfs://192.167.155.71:9000/stefano.lande/out/"


  def getMongoUri(auth: Boolean): String = {

    if (auth) {
      return "mongodb://" + MONGO_USERNAME + ":" + MONGO_PASSWORD + "@" + MONGO_HOST + "/" + MONGO_DB + "." + MONGO_COL
    } else {
      "mongodb://" + MONGO_HOST + "/" + MONGO_DB + "." + MONGO_COL
    }

  }


}
