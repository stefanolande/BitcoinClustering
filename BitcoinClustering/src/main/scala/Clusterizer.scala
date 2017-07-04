/* SimpleApp.scala */

import java.util.ArrayList

import com.mongodb.spark._
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document

import scala.collection.JavaConverters._


object Clusterizer {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Clusterizer")
      .set("spark.mongodb.input.uri", Settings.getMongoUri(MONGO_AUTH_ENABLED))
      .set("spark.mongodb.output.uri", Settings.getMongoUri(MONGO_AUTH_ENABLED))
      .set("spark.cores.max", "6")
      .set("cores", "6")
      .set("spark.executor.heartbeatInterval", "30s")
      .set("spark.driver.memory", "6G")
      .set("spark.executor.memory", "6G")

    val sc = new SparkContext(conf)

    //carico la collection mongo in un rdd
    val rdd = MongoSpark.load(sc).withPipeline(Seq(Document.parse("{ $match: { time : { $lte : 1309744800 } } }")))

    val total_cores = conf.getInt("spark.executor.instances", 1) * conf.getInt("sc._conf.get('spark.executor.cores", 1)
    rdd.coalesce(total_cores * 3)


    //scorro l'rdd per creare i vertici del grafo
    val verticesWithDup = rdd.flatMap(tx => {

      //data una transazione tx, ne prendo tutti gli input
      val inputs = tx.get("vin")

      if (inputs != null) {
        val vertexlist = inputs.
          asInstanceOf[ArrayList[Document]]
          .asScala
          .foldLeft(Set[(VertexId, String)]()) {
          (acc, input) =>
            val addr = input.getString("address")
            if (addr != null) {
              acc + ((addr.hashCode, addr))
            } else {
              acc
            }
        }
        vertexlist
      } else Set[(VertexId, String)]()
    })

    val vertices = verticesWithDup.distinct()


    val edges = rdd.flatMap(tx => {

      val addrSet = heuristic1(tx) ++ heuristic2(tx)

      generateEdges(addrSet)

    })

    val btcGraph = Graph(vertices, edges, "")

    //val cc = RobustConnectedComponents.run(btcGraph, dir = Settings.HDFS_OUT)._1.vertices
    val cc = btcGraph.connectedComponents().vertices

    val ccByAddr = vertices.join(cc).map {
      case (id, (addr, clusterId)) => (addr, clusterId)
    }

    ccByAddr.cache()

    val identities = sc.textFile(Settings.HDFS_DIR + "identities.txt").map { line =>
      val fields = line.split(",")
      (fields(0), fields(1))
    }

    val tagged = ccByAddr.leftOuterJoin(identities).map {
      case (addr, (clusterId, None)) => (addr, clusterId, "")
      case (addr, (clusterId, Some(tag))) => (addr, clusterId, tag)
    }

    val outAll = tagged.sortBy(_._2)

    outAll.saveAsTextFile(Settings.HDFS_OUT + "clustersAll.txt")


    //Get only the clusters with at least one tag
    val onlyAddrTagged = ccByAddr.join(identities).map {
      case (addr, (clusterId, tag)) => (addr, clusterId, tag)
    }

    onlyAddrTagged.saveAsTextFile(Settings.HDFS_OUT + "onlytagged.txt")


    //First, we filter the clusters with only one address
    val ccRev = ccByAddr.map {
      case (addr, clusterId) => (clusterId, addr)
    }

    val ccRevWOSingles = ccByAddr.map {
      case (addr, clusterId) => (clusterId, 1)
    }.reduceByKey((a, b) => a + b)
      .filter(_._2 > 1)
      .join(ccRev).map {
      case (clusterId, (num, addr)) => (clusterId, addr)
    }


    //then, we join back the filtered clusters with the original one
    val onlyAddrTaggedRev = onlyAddrTagged.map {
      case (addr, clusterId, tag) => (clusterId, (addr, tag))
    }


    val onlyClustersTagged = ccRevWOSingles.join(onlyAddrTaggedRev).map {
      case (clusterId, (addr, (addr1, tag))) => (addr, clusterId)
    }.leftOuterJoin(identities).map {
      case (addr, (clusterId, None)) => (addr, clusterId, "")
      case (addr, (clusterId, Some(tag))) => (addr, clusterId, tag)
    }

    onlyClustersTagged.sortBy(_._2).saveAsTextFile(Settings.HDFS_OUT + "clustersOnlyTagged.txt")

  }

  def MONGO_AUTH_ENABLED = false

  /**
    * Multi-input heuristic.
    * If a transaction spends coins originating from multiple inputs,
    * the transaction has to be signed using the appropriate private keys that match the public keys of all inputs.
    * If we assume that a transaction was executed by one user,
    * then this user owns all addresses that were included in the inputs of this transaction.
    *
    * @param tx
    * @return Set of addresses controlled by the same users
    */
  def heuristic1(tx: Document): Set[String] = {
    val inputs = tx.get("vin").asInstanceOf[ArrayList[Document]] asScala

    if (inputs != null) {
      val addrSet = inputs.foldLeft(Set[String]()) {
        (acc, input) =>

          val addr = input.getString("address")
          if (addr != null && !"".equals(addr)) {
            acc + addr
          } else acc
      }
      return addrSet
    } else return Set[String]()
  }

  /**
    * Bridge transaction heuristic.
    * We assume that a transaction with no change output is not probably used to move bitcoins
    * from a user to another, but rather to move funds from an address to another,
    * both controlled by the same users.
    * The transaction must have only one input and one output.
    *
    * @param tx
    * @return Set of addresses controlled by the same users
    */
  def heuristic2(tx: Document): Set[String] = {
    val inputs = tx.get("vin").asInstanceOf[ArrayList[Document]].asScala
    val outputs = tx.get("vout").asInstanceOf[ArrayList[Document]].asScala

    if (inputs != null && outputs != null) {
      if (inputs.size == 1 && outputs.size == 1) {

        val inputAddr = inputs.apply(0).getString("address")
        val outputAddresses = outputs.apply(0).get("address")

        if (inputAddr != null && outputAddresses != null) {

          val inputSet = Set(inputAddr)

          val outputaddr = outputAddresses.asInstanceOf[ArrayList[String]].asScala

          val outputSet = outputaddr.foldLeft(inputSet) {
            (acc, addr) =>
              return acc + addr
          }

          return outputSet
        }
      }
    }

    return Set[String]()

  }

  /**
    * Given a set of addresses, each of them in relation to each other,
    * returns a Set of edges (address_1, address_2)
    *
    * @param addrSet
    * @return Set of edges (address_1, address_2)
    */
  def generateEdges(addrSet: Set[String]): Set[Edge[String]] = {
    for {a_ <- addrSet
         b_ <- addrSet
         if !a_.equals(b_)
    } yield Edge(a_.hashCode, b_.hashCode, "")
  }

}