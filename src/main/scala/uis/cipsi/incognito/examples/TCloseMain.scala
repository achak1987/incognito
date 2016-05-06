package uis.cipsi.incognito.examples

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import java.util.Arrays
import org.apache.spark.rdd.RDD.numericRDDToDoubleRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import uis.cipsi.incognito.rdd._
import uis.cipsi.incognito.informationLoss.InformationLoss
import uis.cipsi.incognito.anonymization.recording.Recording
import uis.cipsi.incognito.utils.Utils
import org.apache.log4j.Logger
import org.apache.log4j.Level
import uis.cipsi.incognito.informationLoss.InformationLoss
import uis.cipsi.incognito.anonymization.redistribution.KMedoids
import uis.cipsi.incognito.anonymization.dichotomize.TDichotomize
import uis.cipsi.incognito.anonymization.buckets.TCloseBuckets
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.HashMap
import uis.cipsi.incognito.anonymization.redistribution.RedistributeNew2
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import breeze.linalg.DenseVector
import uis.cipsi.incognito.anonymization.redistribution.RedistributeFinalNew1

object TCloseMain {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]) = {
    val sparkMaster = args(0)
    //Path to the folder that contain the data, taxonomy tree and data def.
    val filePath = args(1)
    val outFilePath = args(2)
    val folderName = args(3)
    val fileName = args(4)
    val indexes = args(5).split(",").map(_.toInt)
    val t = args(6).toDouble //0.81
    val numPartitions = args(7)

    val dataPath = filePath + folderName
    val taxonomyPath = filePath + fileName + ".taxonomy"
    val dataStrPath = filePath + fileName + ".structure"

    val taxonomySplitChar = ","
    val dataSplitChar = ","
    val dataStrSplitChar = ","

    val pidIndex = indexes(0)
    val saIndex = indexes(1)

    val sc = CustomSparkContext.create(sparkMaster, numPartitions)
    val startTime = System.nanoTime();

    val QIs = sc.objectFile[Data](dataPath)
      .persist(StorageLevel.MEMORY_ONLY)

    val saHist = QIs.map(t => (t.saHash, 1)).reduceByKey(_ + _).persist(StorageLevel.MEMORY_ONLY)

    val saCount = saHist.map(_._2).sum

    val _taxonomy = sc.textFile(taxonomyPath).map(t => t.split(taxonomySplitChar))
      .map(t => (t(0), t(1))).collectAsMap

    //        _taxonomy.foreach(v => println( v._1 + "(" + v._1.hashCode() + ") -> " + v._2 + "(" + v._2.hashCode() + ")") ) 
    val taxonomy = sc.broadcast(_taxonomy)

    val dataStr = sc.textFile(dataStrPath).map(t => t.split(dataStrSplitChar))
      .map(t => (t(0).toInt, (t(1).toInt, t(2).toInt))).collectAsMap

    val _categoricalQIHeights = dataStr.filter(p => p._2._1 == 0)
      //we re-arrange the indexes, since the numeric and char values are seperated into 2 diff. vectors
      .map({ var i = (-1); f => i += 1; (i, f._2._2) })
    val categoricalQIHeights = sc.broadcast(_categoricalQIHeights)

    val saTreeHeight = dataStr(saIndex)._2

    println("\'t\'-closeness with \'t=\'" + t + " on a dataset with total records= " + saCount.toLong.toString)

    val SAs = saHist
      .map(t => new SATuple(t._1, Array(t._1), t._2, (t._2.toDouble / saCount))).persist(StorageLevel.MEMORY_ONLY)

    val totalBucketEstimatedTime = System.nanoTime()
    val buckets = new TCloseBuckets(taxonomy, saCount)

    val bucketsIn: RDD[SATuple] = sc.parallelize(
      buckets.getBuckets(SAs, height = saTreeHeight, t = t)).persist(StorageLevel.MEMORY_ONLY)

    println("bucketization time= " + (System.nanoTime() - totalBucketEstimatedTime).toDouble + "seconds [for the step]")

    val bucCount = bucketsIn.map(v => (-1, 1)).combineByKey((x: Int) => x,
      (acc: Int, x: Int) => acc + x, (acc1: Int, acc2: Int) => acc1 + acc2).map(_._2).reduce(_ + _)

    SAs.unpersist(false)

    if (bucCount == 0) {
      println("No buckets could be created with the given \'t=" + t + "\' for the dataset. Try changing the value of \'t\'")
      System.exit(1)
    }

    println("total buckets= " + bucCount)

    val bucketsSizes = bucketsIn.map(v => new BucketSizes(ecKey = new ECKey(level = 0, sideParent = "R", side = '0'),
      bucketCode = v.bucketCode, size = v.freq, uBound = v.uBound)).persist(StorageLevel.MEMORY_ONLY)

    println("bucketization time= " + (System.nanoTime() - startTime).toDouble / 1000000000)
    val totalECEstimatedTime = System.nanoTime()

    val ecSizes = new TDichotomize(t = t)
    val ecSizesIn = ecSizes.getECSizes(bucketsSizes)

    println("bucketsIn=" + bucketsIn.map(_.freq).sum)
    println("ecSizesIn=" + ecSizesIn.map(_.bucketCodeWithSize.map(_._2).sum).sum)

    bucketsSizes.unpersist(false)
    println("ec time= " + (System.nanoTime() - totalECEstimatedTime).toDouble / 1000000000 + " seconds")

    val k = ecSizesIn.count
    println("total ecs= " + k)

    if (k == 1) {
      println("only one ec generated.")
      System.exit(1)
    }

    val qiCount = saCount.toLong

    val totalRedistributionTime = System.nanoTime()
    val redistribute = new RedistributeFinalNew1(ecSizesIn, numIteration = 1)
    //    val redistribute = new RedistributeNew3(QIs, bucketsIn, ecSizesIn, qiCount)
    //qid, centerID
    val ecs = redistribute.start(QIs, bucketsIn, qiCount)

    val ecCount = ecs.count

    println("total redistributed data= " + ecCount)

    println("redistribution time= " + (System.nanoTime() - totalRedistributionTime).toDouble / 1000000000 + " seconds")

    val totalAnonymizationTime = System.nanoTime()
    val anonymize = new Recording(ecs, taxonomy)
    val anonymizedData = anonymize.generalize()

//    ecs.saveAsObjectFile(outFilePath + "/ecRaw/")
    anonymizedData.saveAsTextFile(outFilePath + "/ecAnn/")

    val anonymizedDataCount = anonymizedData.map(v => (-1, 1)).combineByKey((x: Int) => x,
      (acc: Int, x: Int) => acc + x, (acc1: Int, acc2: Int) => acc1 + acc2).map(_._2).reduce(_ + _)
    println("total anonymized data= " + anonymizedDataCount)

    println("recording time= " + (System.nanoTime() - totalAnonymizationTime).toDouble / 1000000000 + " seconds")

    val totalEstimatedTime = System.nanoTime() - startTime;
    println("total time= " + (totalEstimatedTime).toDouble / 1000000000 + " seconds")

  }
}