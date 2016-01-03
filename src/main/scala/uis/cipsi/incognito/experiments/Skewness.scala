package uis.cipsi.incognito.experiments

import uis.cipsi.incognito.rdd.CustomSparkContext
import uis.cipsi.incognito.rdd.{ ECKey, Data }
import scala.collection.mutable.HashMap
import scala.collection.Map

import org.apache.log4j.Logger
import org.apache.log4j.Level

object Skewness {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def saHashToLevel(saHash: Int, taxonomy: Map[String, String], level: Int, height: Int): String = {
    var i = height
    val taxKeyHashMap = HashMap(0 -> "")
    taxonomy.keys.foreach({ k => taxKeyHashMap += ((k.hashCode(), k)) })
    var parent = taxKeyHashMap(saHash)

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
    val level = args(4).toInt
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

    println("SA Probabilies in Whole Table")
    val count = ecs.count()
    val saPChangeTotal = ecs.map(e => (saHashToLevel(e._2.saHash, taxonomy, level, saTreeHeight), 1)).reduceByKey(_ + _).map(x => (x._1, x._2.toDouble / count)).collectAsMap

    saPChangeTotal.foreach(println)

    println("SA Probabilies in ECs")
    val countPerEC = ecs.countByKey()

    val saPChangePerEC = ecs.map(e => ((e._1, saHashToLevel(e._2.saHash, taxonomy, level, saTreeHeight)), 1))
      .reduceByKey(_ + _).map(x => (x._1, x._2.toDouble / countPerEC(x._1._1)))
      .map(x => (x._1._1, (x._1._2, x._2))).groupByKey

    saPChangePerEC.collect.foreach(println)

    //    println("Percentage change in probabilies of SA values appearing in their respective EC compared to their appearance in the complete dataset")
    val percentChange = saPChangePerEC.mapValues(v => v.map(f => (f._1, math.abs(f._2 - saPChangeTotal(f._1)) / saPChangeTotal(f._1) * 100)))
    //    percentChange.collect.foreach(println)

    //    println("Avg. % change per EC")
    val avgChangePerEC = percentChange.mapValues({ v => val change = v.map(_._2); change.sum / change.size })
    //    avgChangePerEC.collect.foreach(println)

    //    println("Max. % change per EC")
    val maxChangePerEC = percentChange.mapValues({ v => val change = v.map(_._2); change.max })
    //    maxChangePerEC.collect.foreach(println)

    val _avgChangeCompleteDS = avgChangePerEC.map(_._2)
    val avgChangeCompleteDS = _avgChangeCompleteDS.sum / _avgChangeCompleteDS.count

    val maxChangeCompleteDS = maxChangePerEC.map(_._2).max()

    println("Avg. % change complete dataset= " + avgChangeCompleteDS)
    println("Max. % change complete dataset= " + maxChangeCompleteDS)

  }
}