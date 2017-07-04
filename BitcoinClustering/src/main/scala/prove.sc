import org.apache.spark.graphx.{VertexId}

val a = Set[String]("a")
val b = Set[String]("b")

val c = for {a_ <- a
    b_ <- b
    if a_ != b_} yield (a_, b_)

"14kPy4DVV4x3PsVqi5S7V7y17QeuoVGxeJ".hashCode


val inputs = Seq()

val vertexlist = inputs.foldLeft(Set[(VertexId, String)]()) {
  (acc, input) =>
    val addr = input
    if (addr != null) {
      acc + ((addr.hashCode, addr))
    } else {
      acc
    }
}
