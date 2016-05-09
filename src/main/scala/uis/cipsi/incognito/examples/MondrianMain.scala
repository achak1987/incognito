package uis.cipsi.incognito.examples

import java.util.logging.Logger
import java.util.logging.Level
import uis.cipsi.incognito.rdd.CustomSparkContext
import org.apache.spark.mllib.linalg.Vectors
import uis.cipsi.incognito.anonymization.methods.Mondrian

object MondrianMain {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  def main(args: Array[String]): Unit = {
    val sparkMaster = args(0)
    val filePath = args(1)
    val outFilePath = args(2)
    val indexes = args(3).split(",").map(_.toInt)
    val K = args(4).toInt
    val numPartitions = args(5)
    val dataStrSplitChar = args(6)

    val pidIndex = indexes(0)
    val saIndex = indexes(1)

    val sc = CustomSparkContext.create(sparkMaster, numPartitions)
    val _data = sc.textFile(filePath)
    val data = _data.map(r => Vectors.dense(r.split(dataStrSplitChar).map(v => v.toDouble))).cache

    val startTime = System.nanoTime();

    val kAnonymize = new Mondrian(data, K, pidIndex, saIndex)
    kAnonymize.partitionData(kAnonymize.getNormalizedData)

    val anonymizedData = kAnonymize.getAnonymizedData(numPartitions.toInt)
    println("Total anonymized records" + anonymizedData.count)
    
    println("Mondrian Anonymization time= " + (System.nanoTime() - startTime).toDouble / 1000000000)

    anonymizedData.saveAsTextFile(outFilePath + "/ecAnn/")

  }
}