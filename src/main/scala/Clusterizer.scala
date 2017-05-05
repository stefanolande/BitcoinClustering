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

    val vertices =  verticesWithDup.distinct()


    val edges = rdd.flatMap(tx => {

      val addrSet = heuristic1(tx)

      for {a_ <- addrSet
           b_ <- addrSet
           if !a_.equals(b_)
      } yield Edge(a_.hashCode, b_.hashCode, "")

    })

    val btcGraph = Graph(vertices, edges)

    val cc = btcGraph.connectedComponents().vertices

    val ccByAddr = vertices.join(cc).map {
      case (id, (addr, clusterId)) => (addr, clusterId)
    }

    val d = ccByAddr.sortBy(_._2)

    println(d.collect().mkString("\n"))

  }

  def heuristic1(tx: Document) : Set[String] = {
    val javaInputs = tx.get("vin").asInstanceOf[ArrayList[Document]];

    val addrList = javaInputs.asScala.flatMap(input => {

      val addr = input.getString("address")
      if (addr != null && !"".equals(addr)) {
        Set(addr)
      } else Set("")
    })

    val addrSet = addrList.toSet

    return addrSet - ("")

  }

}