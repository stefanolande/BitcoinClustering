import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.GraphLoader

val a = Array(1,1,2,3)

val c = for {a_ <- a
    b_ <- a
    if a_ != b_} yield (a_, b_)

"14kPy4DVV4x3PsVqi5S7V7y17QeuoVGxeJ".hashCode
