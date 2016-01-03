package uis.cipsi.incognito.utils
import org.apache.log4j.Logger
import org.apache.log4j.Level
import uis.cipsi.incognito.rdd.CustomSparkContext
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ArrayBuffer
import uis.cipsi.incognito.rdd.Data
import java.util.Arrays
import org.apache.spark.mllib.linalg.Vectors

object DataGen {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]) = {

    val sparkMaster = args(0)
    val inFilePath = args(1)
    val outFilePath = args(2)
    val inSize = args(3).toInt
    val outSize = args(4).toInt
    val dataSplitChar = args(5)
    val indexes = args(6).split(",").map(_.toInt)

    val pidIndex = indexes(0)
    val saIndex = indexes(1)

    val sc = CustomSparkContext.create(sparkMaster)

    val dat = sc.textFile(inFilePath)
    val d = dat.map(_.split(",").drop(1).mkString(",")).persist(StorageLevel.MEMORY_ONLY)

    var itr = outSize / inSize - 1

    var count = false
    var m1 = d
    count = m1.isEmpty()
    for (i <- 0 until itr) { 
      m1 = m1.union(d) 
      count = m1.isEmpty()  
    }

    val data = m1.zipWithUniqueId.map({ case (k, v) => (v.toString + "," + k) })
      .map(_.split(dataSplitChar).map(v => v.trim))

    val QIs = data.map(t => (t(saIndex).hashCode, t.filter({ var i = (-1); x => i += 1; i != saIndex })))
      .map({
        x =>
          val qis = x._2.map { var i = (-1); m => i += 1; if (i == pidIndex) m.hashCode() else m.toDouble }

          new Data(qiID = Arrays.hashCode(qis),
            qisNumeric = Vectors.dense(qis), qisCategorical = Array(""), saHash = x._1)
      })

    println(count)

    QIs.saveAsObjectFile(outFilePath)
  }

}