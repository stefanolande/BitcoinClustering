
import org.apache.spark.Logging

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object RobustConnectedComponents extends Logging with java.io.Serializable {

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], interval: Int = 50, dir : String): (Graph[VertexId, String], Int) = {

    val ccGraph = graph.mapVertices { case (vid, _) => vid }.mapEdges { x =>""}
    def sendMessage(edge: EdgeTriplet[VertexId, String]): Iterator[(VertexId, Long)] = {
      if (edge.srcAttr < edge.dstAttr) {
        Iterator((edge.dstId, edge.srcAttr))
      } else if (edge.srcAttr > edge.dstAttr) {
        Iterator((edge.srcId, edge.dstAttr))
      } else {
        Iterator.empty
      }
    }
    val initialMessage = Long.MaxValue


    var g: Graph[VertexId, String] = ccGraph
    var i = interval
    var count = 0
    while (i == interval) {
      g = refreshGraph(g, dir, count)
      g.cache()
      val (g1, i1) = pregel(g, initialMessage, interval, activeDirection = EdgeDirection.Either)(
        vprog = (id, attr, msg) => math.min(attr, msg),
        sendMsg = sendMessage,
        mergeMsg = (a, b) => math.min(a, b))
      g.unpersist()
      g = g1
      i = i1
      count = count + i
      logInfo("Checkpoint reached. iteration so far: " + count)

    }
    logInfo("Final Converge: Total Iteration:" + count)
    (g, count)
  } // end of connectedComponents



  def refreshGraph(g : Graph[VertexId, String], dir:String, count:Int): Graph[VertexId, String] = {
    val vertFile = dir + "/iter-" + count + "/vertices"
    val edgeFile = dir + "/iter-" + count + "/edges"
    g.vertices.saveAsObjectFile(vertFile)
    g.edges.saveAsObjectFile(edgeFile)

    //load back
    val v : RDD[(VertexId, VertexId)] = g.vertices.sparkContext.objectFile(vertFile)
    val e : RDD[Edge[String]]= g.vertices.sparkContext.objectFile(edgeFile)

    val newGraph = Graph(v, e)
    newGraph
  }

  def pregel[VD: ClassTag, ED: ClassTag, A: ClassTag](graph: Graph[VD, ED],
                                                      initialMsg: A,
                                                      maxIterations: Int = Int.MaxValue,
                                                      activeDirection: EdgeDirection = EdgeDirection.Either)(vprog: (VertexId, VD, A) => VD,
                                                                                                             sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
                                                                                                             mergeMsg: (A, A) => A): (Graph[VD, ED], Int) =
  {

    var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata,  initialMsg)).cache()
    // compute the messages
    var messages = g.mapReduceTriplets(sendMsg, mergeMsg)
    var activeMessages = messages.count()
    // Loop
    var prevG: Graph[VD, ED] = null
    var i = 0
    while (activeMessages > 0 && i < maxIterations) {
      // Receive the messages and update the vertices.
      prevG = g
      g = g.joinVertices(messages)(vprog).cache()
      val oldMessages = messages
      // Send new messages, skipping edges where neither side received a message. We must cache
      // messages so it can be materialized on the next line, allowing us to uncache the previous
      // iteration.
      messages = g.mapReduceTriplets(
        sendMsg, mergeMsg, Some((oldMessages, activeDirection))).cache()
      // The call to count() materializes `messages` and the vertices of `g`. This hides oldMessages
      // (depended on by the vertices of g) and the vertices of prevG (depended on by oldMessages
      // and the vertices of g).
      activeMessages = messages.count()
      // Unpersist the RDDs hidden by newly-materialized RDDs
      oldMessages.unpersist(blocking = false)
      prevG.unpersistVertices(blocking = false)
      prevG.edges.unpersist(blocking = false)
      // count the iteration
      i += 1
    }
    (g, i)
  } // end of apply
}