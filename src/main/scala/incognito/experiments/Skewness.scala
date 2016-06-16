package incognito.experiments

import incognito.rdd.CustomSparkContext
import incognito.rdd.{ ECKey, Data }
import scala.collection.mutable.HashMap
import scala.collection.Map

import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
 * @author Antorweep Chakravorty
 * Calculates the skewness of SA values at different levels of its taxonomy tree
 */
object Skewness {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def saHashToLevel(saHash: String, taxonomy: Map[String, String], level: Int, height: Int): String = {
    var i = height
    //    val taxKeyHashMap = HashMap("-1" -> "")
    //    taxonomy.keys.foreach({ k => taxKeyHashMap += ((k, k)) })
    var parent = saHash //taxonomy(saHash)

    while (i > level) {
      i -= 1
      parent = taxonomy(parent)
    }
    parent
  }

  def main(args: Array[String]): Unit = {
    val sparkMaster = args(0)
    //Path to the folder that contain the data, taxonomy tree and data def.
    val filePath = args(1)
    val fileName = args(2)
    val redistributedECPath = args(3)
    var level = 0 //args(4).toInt
    val saIndex = args(5).toInt

    val dataPath = filePath + fileName
    val taxonomyPath = dataPath + ".taxonomy"
    val dataStrPath = dataPath + ".structure"

    val taxonomySplitChar = ","
    val dataSplitChar = ","
    val dataStrSplitChar = ","

    val sc = CustomSparkContext.create(sparkMaster)

    val taxonomy = sc.textFile(taxonomyPath).map(t => t.split(taxonomySplitChar))
      .map(t => (t(0), t(1))).collectAsMap

    //    val taxKeyHashMap = HashMap(0 -> "")
    //    taxonomy.keys.foreach({ k => taxKeyHashMap += ((k.hashCode(), k)) })

    val dataStr = sc.textFile(dataStrPath).map(t => t.split(dataStrSplitChar))
      .map(t => (t(0).toInt, (t(1).toInt, t(2).toInt))).collectAsMap

    //    val categoricalQIHeights = dataStr.filter(p => p._2._1 == 0)
    //      //we re-arrange the indexes, since the numeric and char values are seperated into 2 diff. vectors
    //      .map({ var i = (-1); f => i += 1; (i, f._2._2) })

    val saTreeHeight = dataStr(saIndex)._2

    val ecs = sc.objectFile[(ECKey, Data)](redistributedECPath)

    //    println("SA Probabilies in Whole Table")
    val count = ecs.count()

    val ecAtLevel = ecs
      .map(e => ((e._1, saHashToLevel(e._2.sa, taxonomy, level, saTreeHeight)), 1))

    //    println("SA Probabilies in ECs")
    val countPerEC = ecs.countByKey()

    val saPChangeTotal = ecAtLevel.map(v => (v._1._2, v._2))
      .reduceByKey(_ + _).map(x => (x._1, x._2.toDouble / count)).collectAsMap

    //    ecAtLevel
    //    .filter(v => v._1._1.level == 5 && v._1._1.sideParent == "1591764892" && v._1._1.side == '1')
    //      .reduceByKey(_ + _)
    //      .map(x => (x._1, x._2.toDouble, countPerEC(x._1._1), x._2.toDouble / countPerEC(x._1._1)))
    //      .collect.foreach(println)
    //
    //    saPChangeTotal.foreach(println)
    //    countPerEC.foreach(println)

    //      ecs
    //      .map(e => (saHashToLevel(e._2.saHash, taxonomy, level, saTreeHeight), 1))
    //      .reduceByKey(_ + _).map(x => (x._1, x._2.toDouble, count))
    //      .collect
    //      .foreach(println)

    val saPChangePerEC = ecAtLevel
      .reduceByKey(_ + _).map(x => (x._1, x._2.toDouble / countPerEC(x._1._1)))
      .map(x => (x._1._1, (x._1._2, x._2))).groupByKey


    //    ecs.map(e => ((e._1, saHashToLevel(e._2.saHash, taxonomy, level, saTreeHeight)), 1))
    //      .reduceByKey(_ + _)
    //      .map(x => (x._1, x._2.toDouble, countPerEC(x._1._1)))
    //      .map(x => (x._1._1, (x._1._2, x._2, x._3))).groupByKey.mapValues(_.toArray.sortBy(_._2).toSeq )
    //      .collect.foreach(println)

    //    println("Percentage change in probabilies of SA values appearing in their respective EC compared to their appearance in the complete dataset")
    val percentChange = saPChangePerEC.mapValues(v => v.map(f => (f._1, math.abs(saPChangeTotal(f._1) - f._2) / saPChangeTotal(f._1) * 100)))
    //        percentChange.collect.foreach(println)

    //    println("Avg. % change per EC")
    val avgChangePerEC = percentChange.mapValues({ v => val change = v.map(_._2); change.sum / change.size })
    //    avgChangePerEC.collect.foreach(println)

    //    println("Max. % change per EC")
    val maxChangePerEC = percentChange.mapValues({ v => val change = v.map(_._2); change.max })
            maxChangePerEC.collect.foreach(println)

    val _avgChangeCompleteDS = avgChangePerEC.map(_._2)
    val avgChangeCompleteDS = _avgChangeCompleteDS.sum / _avgChangeCompleteDS.count

    val maxChangeCompleteDS = maxChangePerEC.map(_._2).max()

    val x = percentChange.mapValues({ v => v.maxBy(_._2) }).sortBy(_._2._2, false).take(1).toSeq
    println(percentChange.count)

    println("Avg. % change complete dataset= " + avgChangeCompleteDS)
    println("Max. % change complete dataset= " + maxChangeCompleteDS)

  }
}