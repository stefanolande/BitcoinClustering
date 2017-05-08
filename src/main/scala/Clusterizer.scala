/* SimpleApp.scala */

import java.util.ArrayList

import org.apache.spark.{SparkConf, SparkContext}
import com.mongodb.spark._
import org.bson.Document
import org.apache.spark.graphx._
import scala.collection.JavaConverters._


object Clusterizer {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Clusterizer")
      .set("spark.mongodb.input.uri", "mongodb://lanser:nakamotocatenE@127.0.0.1/blockchain.transaction_test")
      .set("spark.mongodb.output.uri", "mongodb://lanser:nakamotocatenE@127.0.0.1/blockchain.transaction_test")
    val sc = new SparkContext(conf)


    //carico la collection mongo in un rdd
    val rdd = MongoSpark.load(sc)


    //scorro l'rdd per creare i vertici del grafo
    val verticesWithDup = rdd.flatMap(tx => {

      //data una transazione tx, ne prendo tutti gli input
      val inputs = tx.get("vin").asInstanceOf[ArrayList[Document]].asScala;

      val vertexlist = inputs.foldLeft(Set[(VertexId, String)]()) {
        (acc, input) =>
          val addr = input.getString("address")
          if (addr != null) {
            acc + ((addr.hashCode, addr))
          } else {
            acc
          }
      }
      vertexlist
    })

    val vertices = verticesWithDup.distinct()


    val edges = rdd.flatMap(tx => {

      val addrSet = heuristic1(tx) ++ heutistic2(tx)

      generateEdges(addrSet)

    })

    val btcGraph = Graph(vertices, edges)

    val cc = btcGraph.connectedComponents().vertices

    val ccByAddr = vertices.join(cc).map {
      case (id, (addr, clusterId)) => (addr, clusterId)
    }

    val d = ccByAddr.sortBy(_._2)

    println(d.collect().mkString("\n"))

  }

  /**
    * Multi-input heuristic.
    * If a transaction spends coins originating from multiple inputs,
    * the transaction has to be signed using the appropriate private keys that match the public keys of all inputs.
    * If we assume that a transaction was executed by one user,
    * then this user owns all addresses that were included in the inputs of this transaction.
    * @param tx
    * @return Set of addresses controlled by the same users
    */
  def heuristic1(tx: Document): Set[String] = {
    val inputs = tx.get("vin").asInstanceOf[ArrayList[Document]] asScala

    val addrList = inputs.flatMap(input => {

      val addr = input.getString("address")
      if (addr != null && !"".equals(addr)) {
        Set(addr)
      } else Set[String]()
    })

    val addrSet = addrList.toSet

    return addrSet

  }

  /**
    * Bridge transaction heuristic.
    * We assume that a transaction with no change output is not probably used to move bitcoins
    * from a user to another, but rather to move funds from an address to another,
    * both controlled by the same users.
    * The transaction must have only one input and one output.
    * @param tx
    * @return Set of addresses controlled by the same users
    */
  def heutistic2(tx: Document): Set[String] = {
    val inputs = tx.get("vin").asInstanceOf[ArrayList[Document]].asScala
    val outputs = tx.get("vout").asInstanceOf[ArrayList[Document]].asScala


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

    return Set[String]()

  }

  /**
    * Given a set of addresses, each of them in relation to each other,
    * returns a Set of edges (address_1, address_2)
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