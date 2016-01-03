package uis.cipsi.incognito.examples

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import java.util.Arrays
import org.apache.spark.rdd.RDD.numericRDDToDoubleRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import uis.cipsi.incognito.rdd._
import uis.cipsi.incognito.anonymization.buckets.IncognitoBuckets
import uis.cipsi.incognito.anonymization.dichotomize.Dichotomize
import uis.cipsi.incognito.informationLoss.InformationLoss
import uis.cipsi.incognito.anonymization.recording.Recording
import uis.cipsi.incognito.utils.Utils
import org.apache.log4j.Logger
import org.apache.log4j.Level
import uis.cipsi.incognito.informationLoss.InformationLoss
import uis.cipsi.incognito.anonymization.redistribution.KMedoids
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.util.MLUtils
import uis.cipsi.incognito.anonymization.redistribution.RedistributeNew2
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import breeze.linalg.DenseVector
import uis.cipsi.incognito.anonymization.redistribution.RedistributionGraph
import uis.cipsi.incognito.anonymization.redistribution.RedistributeFinal

//spark://haisen20.ux.uis.no:7077
//10
//4gb
//hdfs://haisen20.ux.uis.no:9000/islr/
//tachyon://haisen20.ux.uis.no:19998/tmp/islr/
//islr.dat
//0,9

object IncognitoMain {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val sparkMaster = args(0)
    //Path to the folder that contain the data, taxonomy tree and data def.
    val filePath = args(1)
    val outFilePath = args(2)
    val folderName = args(3)
    val fileName = args(4)
    val indexes = args(5).split(",").map(_.toInt)

    val dataPath = filePath + folderName
    val taxonomyPath = filePath + fileName + ".taxonomy"
    val dataStrPath = filePath + fileName + ".structure"

    val taxonomySplitChar = ","
    val dataSplitChar = ","
    val dataStrSplitChar = ","

    val pidIndex = indexes(0)
    val saIndex = indexes(1)

    val sc = CustomSparkContext.create(sparkMaster)

    val _taxonomy = sc.textFile(taxonomyPath).map(t => t.split(taxonomySplitChar))
      .map(t => (t(0), t(1))).collectAsMap

    val taxonomy = sc.broadcast(_taxonomy)

    val dataStr = sc.textFile(dataStrPath).map(t => t.split(dataStrSplitChar))
      .map(t => (t(0).toInt, (t(1).toInt, t(2).toInt))).collectAsMap

    val _categoricalQIHeights = dataStr.filter(p => p._2._1 == 0)
      //we re-arrange the indexes, since the numeric and char values are seperated into 2 diff. vectors
      .map({ var i = (-1); f => i += 1; (i, f._2._2) })
    val categoricalQIHeights = sc.broadcast(_categoricalQIHeights)

    val saTreeHeight = dataStr(saIndex)._2
    val startTime = System.nanoTime();

    val QIs = sc.parallelize(
        sc.objectFile[Data](dataPath)
        .take(10))
      .persist(StorageLevel.MEMORY_ONLY)

    //    //Precheck Start
    //    val minUniqueSA = 10
    //    val sasGreaterThanMinUniqueSA = _data.map(t => (t(saIndex), 1)).reduceByKey(_ + _).filter(_._2 > minUniqueSA).map(_._1).collect()
    //    val data = _data.filter { t => sasGreaterThanMinUniqueSA.contains(t(saIndex)) }
    //    //Precheck Finish

    val saHist = sc.parallelize(
      QIs
        .map(t => (t.saHash, 1))
        .combineByKey((x: Int) => x,
          (acc: Int, x: Int) => (acc + x),
          (acc1: Int, acc2: Int) => (acc1 + acc2))
        .collect)

    val saCount = saHist.combineByKey((x: Int) => x,
      (acc: Int, x: Int) => (acc + x),
      (acc1: Int, acc2: Int) => (acc1 + acc2)).map(_._2).reduce(_ + _)
      
  

    //    println("The original dataset having records=" + data.count + " was sanitized, such that each unique sensitive attribute has a minimum frequency greater than \'" + minUniqueSA + "\'. \nThe sanitized data set now has records=" + saCount)    

    val beta = args(6).toDouble
    println("\'incognito\' with \'beta=\'" + beta + " on a dataset with total records= " + saCount.toLong.toString)

    //    val SAs = saHist
    //      .map(t => new SATuple(t._1, Vector(t._1), t._2,
    //        (t._2.toDouble / saCount) * (1 - math.log((t._2.toDouble / saCount)))))

    val SAs = saHist
      .map({ t =>
        val p = t._2.toDouble / saCount;
        new SATuple(t._1, Array(t._1), t._2,
          (1 + math.min(beta, -1.0 * math.log(p))) * p //          (if (p <= math.pow(math.E, -1 * beta)) p * (1 + beta) else p * (1 - math.log(p)))
          )
      }).persist(StorageLevel.MEMORY_ONLY)

    val totalBucketEstimatedTime = System.nanoTime()
    val buckets = new IncognitoBuckets(taxonomy, saCount, beta)
   

    val bucketsIn: RDD[SATuple] = sc.parallelize(
      buckets.getBuckets(SAs, height = saTreeHeight)).persist(StorageLevel.MEMORY_ONLY)
    //    bucketsIn.saveAsObjectFile("/home/antorweep/islr/bucketsIn")

    println("bucketization time= " + (System.nanoTime() - totalBucketEstimatedTime).toDouble + "seconds [for the step]")

    println(bucketsIn.map(_.freq).sum)
    System.exit(1)
    val bucCount = bucketsIn.map(v => (-1, 1)).combineByKey((x: Int) => x,
      (acc: Int, x: Int) => acc + x, (acc1: Int, acc2: Int) => acc1 + acc2).map(_._2).reduce(_ + _)
    SAs.unpersist(false)
    println("total buckets= " + bucCount)

    val bucketsSizes = bucketsIn.map(v => new BucketSizes(ecKey = new ECKey(level = 0, sideParent = "R", side = 'x'),
      bucketCode = v.bucketCode, size = v.freq, uBound = v.uBound)).persist(StorageLevel.MEMORY_ONLY)

    println("bucketization time= " + (System.nanoTime() - startTime).toDouble / 1000000000)
    //    bucketsSizes.saveAsObjectFile("/home/antorweep/islr/bucketsSizes")
    val totalECEstimatedTime = System.nanoTime()
    val ecSizes = new Dichotomize
    val ecSizesIn = ecSizes.getECSizes(bucketsSizes)
    println(ecSizesIn.map(_.bucketCodeWithSize.map(_._2).sum).sum)
    System.exit(1)

    bucketsSizes.unpersist(false)
    println("ec time= " + (System.nanoTime() - totalECEstimatedTime).toDouble / 1000000000 + " seconds")

    val k = ecSizesIn.length
    println("total ecs= " + k)

    if (k == 1) {
      println("only one ec generated.")
      System.exit(1)
    }

    val qiCount = saCount.toLong
    val _avgNumericQI = QIs.map(v => (-1, v.qisNumeric)).combineByKey((x: Vector) => x,
      (acc: Vector, x: Vector) => Vectors.dense((new DenseVector(acc.toArray) + new DenseVector(x.toArray)).toArray),
      (acc1: Vector, acc2: Vector) => Vectors.dense((new DenseVector(acc1.toArray) + new DenseVector(acc2.toArray)).toArray))
      .map(_._2)
      .reduce((acc1, acc2) => Vectors.dense((new DenseVector(acc1.toArray) + new DenseVector(acc2.toArray)).toArray))

    val avgNumericQI = new DenseVector(_avgNumericQI.toArray).map(_ / qiCount)

    val _sdQI = QIs.map(v => (-1, Vectors.dense(
      (new DenseVector(v.qisNumeric.toArray) - avgNumericQI).map(x => math.pow(x, 2)).toArray)))
      .combineByKey((x: Vector) => x,
        (acc: Vector, x: Vector) => Vectors.dense((new DenseVector(acc.toArray) + new DenseVector(x.toArray)).toArray),
        (acc1: Vector, acc2: Vector) => Vectors.dense((new DenseVector(acc1.toArray) + new DenseVector(acc2.toArray)).toArray))
      .map(_._2)
      .reduce((acc1, acc2) => Vectors.dense((new DenseVector(acc1.toArray) + new DenseVector(acc2.toArray)).toArray))

    val sdQI = new DenseVector(_sdQI.toArray).map(_ / qiCount)

    val standarizedQIs = QIs.map({ v =>
      val zscore = Vectors.dense(((new DenseVector(v.qisNumeric.toArray) - avgNumericQI) / sdQI).toArray)
      new Data(qiID = v.qiID, qisNumeric = zscore, qisCategorical = v.qisCategorical, saHash = v.saHash)
    }).persist(StorageLevel.MEMORY_ONLY)

    //    val totalSeedInitializationTime = System.nanoTime()
    //    val kmediods = new KMedoids(standarizedQIs, taxonomy, categoricalQIHeights, pidIndex, k, qiCount)
    //    val bucketsWSAs = bucketsIn.map { x => (x.bucketCode, x.tuple) }.collect.toMap
    //
    //    val _seeds = kmediods.initialize(bucketsWSAs, ecSizesIn, numIterations = 1, runs = 1)
    //
    //    standarizedQIs.unpersist(false)
    //    println("seeds initialization time= " + (System.nanoTime() - totalSeedInitializationTime).toDouble / 1000000000 + " seconds")
    //
    //    val seeds = sc.broadcast(_seeds._1)
    //
    //    ////###########Initial Seed VISUALIZATION######################
    //    ////If we want to vizualize and validate the distribution of initial centers
    //    ////Works with upto 3-d QIs. It will generate the data. Run the R script "initialSeedPlot.R" to plot!
    //    //Utils.saveSeedsInitializationForR(standarizedQIs, k, 3, taxonomy, categoricalQIHeights, pidIndex, sc)

    val tupleBucketMap = bucketsIn
      .map(b => b.tuple.map(t => (t, b.bucketCode))).flatMap(f => f)
      .reduceByKey((x, y) => x)

    val tupleBucketGroup = tupleBucketMap
      .join(standarizedQIs.map(qi => (qi.saHash, qi)))
      .map(v => (v._2._2.qiID, v))

      //bcode, data
      .map(v => (v._2._2)).persist(StorageLevel.MEMORY_ONLY)

    println(tupleBucketMap.count, standarizedQIs.count, tupleBucketGroup.count)
    System.exit(1)

    //    //ecid, (seedid, size)
    //    val ecKeysSizes = _seeds._2.map(v => (v._1._2, v._1._1)).toMap
    //    val ecBukSizes = ecSizesIn.map(v => (v.ecKey, v.bucketCodeWithSize.toMap))
    //    //((seedid, bukid), size)
    //    val ecKeyBukCodeSizes = sc.broadcast(
    //      ecBukSizes.map({ v => (ecKeysSizes(v._1), v._2) }).map(v => v._2.map(b => ((v._1, b._1), b._2)))
    //        .flatMap(f => f).toMap)

    val totalRedistributionTime = System.nanoTime()
    val redistribute = new RedistributeFinal(ecSizesIn, numIteration = 10)
    //qid, centerID
    val _ecs = redistribute.start(tupleBucketGroup)

    println("ec=" + ecSizesIn.length)
    ecSizesIn.map(v => (v.ecKey, v.bucketCodeWithSize.map(_._2).sum)).sortBy(_._2).reverse.foreach(println)

    println("re=" + _ecs.map({ case (q, e) => (e, q) }).countByKey().size)
    _ecs.map({ case (q, e) => (e, q) }).countByKey().toArray.sortBy(_._2).reverse.foreach(println)
    println("ecSizeIn=" + ecSizesIn.length + "bucketSizeIn=" + bucketsIn.count + " x= " + ecSizesIn.length * bucketsIn.count)

    val _ecCount = _ecs.map(v => (-1, 1)).combineByKey((x: Int) => x,
      (acc: Int, x: Int) => acc + x, (acc1: Int, acc2: Int) => acc1 + acc2).map(_._2).reduce(_ + _)
    println("total redistributed data= " + _ecCount)
    tupleBucketGroup.unpersist(false)

    val ecs = QIs.map(qis => (qis.qiID, qis))
      .join(_ecs).map(v => (v._2._2, v._2._1))
      .persist(StorageLevel.MEMORY_ONLY)

    println("redistribution time= " + (System.nanoTime() - totalRedistributionTime).toDouble / 1000000000 + " seconds")

    //    val totalAnonymizationTime = System.nanoTime()
    //    val anonymize = new Recording(ecs, taxonomy)
    //    val anonymizedData = anonymize.generalize() //sc.objectFile[(Vector[String], Vector[Double], String)](outFilePath + "out/anonymizedData/" + fileName + "/*/")
    //
    //        val anonymizedDataCount = anonymizedData.map(v => (-1, 1)).combineByKey((x: Int) => x,
    //          (acc: Int, x: Int) => acc + x, (acc1: Int, acc2: Int) => acc1 + acc2).map(_._2).reduce(_ + _)
    //        println("total anonymized data= " + anonymizedDataCount)
    //
    //    println("recording time= " + (System.nanoTime() - totalAnonymizationTime).toDouble / 1000000000 + " seconds")
    //    val totalAnonymizationWritingTime = System.nanoTime() 
    //    anonymizedData.saveAsTextFile(outFilePath + "anonymized.dat")
    //    println("recording hdfs write time= " + (totalAnonymizationWritingTime).toDouble / 1000000000 + " seconds")

    QIs.unpersist(false)

    val totalEstimatedTime = System.nanoTime() - startTime;
    println("total time= " + (totalEstimatedTime).toDouble / 1000000000 + " seconds")

  }
}