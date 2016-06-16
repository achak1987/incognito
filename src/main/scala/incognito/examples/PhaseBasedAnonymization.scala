package incognito.examples

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import java.util.Arrays
import org.apache.spark.rdd.RDD.numericRDDToDoubleRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import incognito.rdd._
import incognito.anonymization.buckets.IncognitoBuckets
import incognito.anonymization.dichotomize.IDichotomize
import incognito.informationLoss.InformationLoss
import incognito.anonymization.recording.Recording
import incognito.utils.Utils
import org.apache.log4j.Logger
import org.apache.log4j.Level
import incognito.informationLoss.InformationLoss
import incognito.archive.KMedoids
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import breeze.linalg.DenseVector
import incognito.anonymization.redistribution.RedistributeBasedOnNorm
import incognito.anonymization.buckets.Buckets
import incognito.anonymization.buckets.BetaBuckets
import incognito.anonymization.buckets.TCloseBuckets
import incognito.anonymization.dichotomize.Dichotomize
import incognito.anonymization.dichotomize.TDichotomize
import incognito.anonymization.dichotomize.TDichotomize
import incognito.anonymization.dichotomize.TDichotomize

/**
 * @author Antorweep Chakravorty
 * Anonymized datasets with phase based algorithms such as: Incognito, Beta-Likeness, T-Closeness
 */

object PhaseBasedAnonymization {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val sparkMaster = args(0)
    //Path to the folder that contain the data, taxonomy tree and data def.
    val filePath = args(1)
    val fileName = args(2)
    val fileSep = args(3)
    val indexes = args(4).split(",").map(_.toInt)
    val threshold = args(5).toDouble
    val numPartitions = args(6)
    val algorithm = args(8)
    val outFilePath = args(7) + "/" + algorithm + ".anonymized"

    val dataPath = filePath + fileName
    val taxonomyPath = filePath + fileName + ".taxonomy"
    val dataStrPath = filePath + fileName + ".structure"

    val taxonomySplitChar = ","
    val dataSplitChar = ","
    val dataStrSplitChar = ","

    val pidIndex = indexes(0)
    val saIndex = indexes(1)

    val sc = CustomSparkContext.create(sparkMaster, numPartitions)

    val startTime = System.nanoTime();

    //taxonomy tree of sensitive attribute values: (child -> parent)
    val _taxonomy = sc.textFile(taxonomyPath).map(t => t.split(taxonomySplitChar))
      .map(t => (t(0), t(1))).collectAsMap

    val taxonomy = sc.broadcast(_taxonomy)

    //data schema (index -> (isNumeric, height))
    val dataStr = sc.textFile(dataStrPath).map(t => t.split(dataStrSplitChar))
      .map(t => (t(0).toInt, (t(1).toInt, t(2).toInt))).collectAsMap

    val utils = new Utils
    //records to be anonymized		
    val data = sc.textFile(dataPath).map({ r =>
      val d = r.split(fileSep);
      val qNumeric = d.filter { var i = (-1); m => i += 1; i != pidIndex && dataStr(i)._1 == 1 && i != saIndex }.map(_.toDouble)
      val qCategorical = d.filter { var i = (-1); m => i += 1; i != pidIndex && dataStr(i)._1 == 0 && i != saIndex }
      new Data(id = utils.hashId(r), pID = d(pidIndex), qNumeric = Vectors.dense(qNumeric), qCategorical = qCategorical, sa = d(saIndex))
    })
      .persist(StorageLevel.MEMORY_ONLY)

    val count = data.count
    val saTreeHeight = dataStr(saIndex)._2

    println("\'" + algorithm + "\' with \'threshold=\'" + threshold + " on a dataset with total records= " + count.toLong.toString)

    //Preparing the Bucketization Phase
    val totalBucketEstimatedTime = System.nanoTime()
    val algoBucket: Buckets = (
      if (algorithm.toLowerCase == "incognito") new IncognitoBuckets(taxonomy, count, threshold)
      else if (algorithm.toLowerCase == "beta") new BetaBuckets(count, threshold)
      else new TCloseBuckets(taxonomy, count, threshold))

    val bucketsIn: RDD[SATuple] = algoBucket.getBuckets(data, height = saTreeHeight)
      .persist(StorageLevel.MEMORY_ONLY)
    val bucCount = bucketsIn.count

    println("total buckets=" + bucCount + ", with frequency sum= " + bucketsIn.map(_.freq).sum)
    println("bucketization time= " + (System.nanoTime() - totalBucketEstimatedTime).toDouble + "seconds [for the step]")

    if (bucCount == 0) {
      println("No buckets could be created with the given \'threshold=" + threshold + "\' for the dataset. Try changing the value of \'beta\'")
      System.exit(1)
    }

    //Preparing the Dichotomization Phase    
    val totalECEstimatedTime = System.nanoTime()
    val bucketsSizes = bucketsIn.map(v => new BucketSizes(ecKey = new ECKey(level = 0, sideParent = "R", side = 'x'),
      bucketCode = v.bucketCode, size = v.freq, uBound = v.uBound)).persist(StorageLevel.MEMORY_ONLY)

    val algoEC: Dichotomize = (
      if (algorithm.toLowerCase == "incognito" || algorithm.toLowerCase == "beta") new IDichotomize()
      else new TDichotomize(threshold))

    val ecSizesIn = algoEC.getECSizes(bucketsSizes)
    val k = ecSizesIn.count

    println("total ecs=" + k + ", with frequency sum=" + ecSizesIn.map(_.bucketCodeWithSize.map(_._2).sum).sum)

    bucketsSizes.unpersist(false)
    println("ec time= " + (System.nanoTime() - totalECEstimatedTime).toDouble / 1000000000 + " seconds")

    if (k == 1) {
      println("Only 1 ec was generated with the given \'threshold=" + threshold + "\' for the dataset. Try changing the value of \'beta\'")
      System.exit(1)
    }

    //Preparing the Redistribution Phase 
    val totalRedistributionTime = System.nanoTime()
    val redistribute = new RedistributeBasedOnNorm(data, bucketsIn, ecSizesIn, count)
    //qid, centerID
    val ecs = redistribute.start()
    val ecCount = ecs.count

    println("total redistributed data= " + ecCount)
    println("redistribution time= " + (System.nanoTime() - totalRedistributionTime).toDouble / 1000000000 + " seconds")

    //Preparing Recording Phase
    val totalAnonymizationTime = System.nanoTime()
    val anonymize = new Recording(ecs, taxonomy)
    val anonymizedData = anonymize.generalize()

    val anonymizedDataCount = anonymizedData.count

    println("total anonymized data= " + anonymizedDataCount)
    println("recording time= " + (System.nanoTime() - totalAnonymizationTime).toDouble / 1000000000 + " seconds")

    //    ecs.saveAsObjectFile(outFilePath)
    if (outFilePath != "")
      anonymizedData.saveAsTextFile(outFilePath)

    //    ecs.collect.foreach(println)
//        anonymizedData.collect.foreach(println)
    val totalEstimatedTime = System.nanoTime() - startTime;
    println("total time= " + (totalEstimatedTime).toDouble / 1000000000 + " seconds")

  }
}