package incognito.utils

import org.apache.log4j.Logger
import org.apache.log4j.Level
import incognito.rdd.CustomSparkContext
import incognito.rdd.Data
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import incognito.rdd.SATuple
import incognito.rdd.BucketSizes
import incognito.anonymization.dichotomize.IDichotomize
import incognito.rdd.ECKey
import incognito.anonymization.buckets.Buckets
import incognito.anonymization.buckets.IncognitoBuckets
import incognito.anonymization.buckets.BetaBuckets
import scala.collection.mutable.ArrayBuffer
import incognito.anonymization.dichotomize.Dichotomize
import incognito.anonymization.dichotomize.IDichotomize
import incognito.anonymization.dichotomize.TDichotomize
import incognito.anonymization.buckets.TCloseBuckets

/**
 * @author Antorweep Chakravorty
 * Determines how many buckets and equivalence classes can be created for a range of threshold
 */
object BucketECThesholdList {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {
    val sparkMaster = args(0)
    //Path to the folder that contain the data, taxonomy tree and data def.
    val filePath = args(1)
    val fileName = args(2)
    val fileSep = args(3)
    val indexes = args(4).split(",").map(_.toInt)
    
    val betaMin = args(5).toDouble
    val betaMax = args(6).toDouble
    val betaBy = args(7).toDouble
    val name = args(8)
    val numPartitions = args(9)

    val dataPath = filePath + fileName
    val taxonomyPath = filePath + fileName + ".taxonomy"
    val dataStrPath = filePath + fileName + ".structure"

    val taxonomySplitChar = ","
    val dataSplitChar = ","
    val dataStrSplitChar = ","

    val pidIndex = indexes(0)
    val saIndex = indexes(1)

    val sc = CustomSparkContext.create(sparkMaster, numPartitions)

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

    var threshold = betaMin

    //Threshold, #Buckets, #EC, bucketization time(in sec), dichotomization time (in sec)
    val results = new ArrayBuffer[(Double, Long, Long, Double, Double)]

    //Iterate through all the threshold from min to max
    for (i <- betaMin until betaMax by betaBy) {
      //Increment the threshold each time
      threshold = threshold + betaBy
      println("\'" + name + "\' with \'threshold=\'" + threshold + " on a dataset with total records= " + count.toLong.toString)
      //Preparing the Bucketization Phase
      val totalBucketEstimatedTime = System.nanoTime()

      //Determine which buckatization phase to run
      val algoBucket: Buckets = (
        if (name.toLowerCase == "incognito") new IncognitoBuckets(taxonomy, count, threshold)
        else if (name.toLowerCase == "beta") new BetaBuckets(count, threshold)
        else new TCloseBuckets(taxonomy, count, threshold))

      val bucketsIn = algoBucket.getBuckets(data, height = saTreeHeight)
        .persist(StorageLevel.MEMORY_ONLY)
      val bucCount = bucketsIn.count
      val bucketizationTime = (System.nanoTime() - totalBucketEstimatedTime).toDouble

      //Preparing the Dichotomization Phase    
      val totalECEstimatedTime = System.nanoTime()
      val bucketsSizes = bucketsIn.map(v => new BucketSizes(ecKey = new ECKey(level = 0, sideParent = "R", side = 'x'),
        bucketCode = v.bucketCode, size = v.freq, uBound = v.uBound)).persist(StorageLevel.MEMORY_ONLY)

      //Determine which dichiotomization phase to run
      val algoEC: Dichotomize = (
        if (name.toLowerCase == "incognito" || name.toLowerCase == "beta") new IDichotomize()
        else new TDichotomize(threshold))
        
      val ecSizesIn = algoEC.getECSizes(bucketsSizes)
      val ecCount = ecSizesIn.count

      bucketsSizes.unpersist(false)
      val dichotomizationTime = (System.nanoTime() - totalECEstimatedTime).toDouble / 1000000000

      //Store the results
      results += ((threshold, bucCount, ecCount, bucketizationTime, dichotomizationTime))
    }
    println("Threshold, #Buckets, #EC, bucketization time(in sec), dichotomization time (in sec)")
    results.foreach(println)

  }
}