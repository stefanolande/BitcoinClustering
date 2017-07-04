/**
  * Created by stefano on 08/05/17.
  */
object Settings {
  private val MONGO_USERNAME = "lanser"
  private val MONGO_PASSWORD = "nakamotocatenE"
  private val MONGO_HOST = "192.167.155.71";
  private val MONGO_DB = "clustering"
  private val MONGO_COL = "bc_small"

  def HDFS_DIR = "hdfs://192.167.155.71:9000/stefano.lande/"
  def HDFS_OUT = "hdfs://192.167.155.71:9000/stefano.lande/out/"

  //def HDFS_DIR = "/home/osboxes/"

  //def HDFS_OUT = "/home/osboxes/out/"


  def getMongoUri(auth: Boolean): String = {

    if (auth) {
      return "mongodb://" + MONGO_USERNAME + ":" + MONGO_PASSWORD + "@" + MONGO_HOST + "/" + MONGO_DB + "." + MONGO_COL
    } else {
      "mongodb://" + MONGO_HOST + "/" + MONGO_DB + "." + MONGO_COL
    }

  }


}
