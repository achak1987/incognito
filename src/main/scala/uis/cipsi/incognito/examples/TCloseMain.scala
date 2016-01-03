//package uis.cipsi.incognito.examples
//
//import org.apache.spark.rdd.RDD
//import scala.collection.mutable.ArrayBuffer
//import java.util.Arrays
//import org.apache.spark.rdd.RDD.numericRDDToDoubleRDDFunctions
//import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
//import uis.cipsi.incognito.rdd._
//import uis.cipsi.incognito.anonymization.buckets.IncognitoBuckets
//import uis.cipsi.incognito.informationLoss.InformationLoss
//import uis.cipsi.incognito.anonymization.recording.Recording
//import uis.cipsi.incognito.utils.Utils
//import org.apache.log4j.Logger
//import org.apache.log4j.Level
//import uis.cipsi.incognito.informationLoss.InformationLoss
//import uis.cipsi.incognito.anonymization.redistribution.KMedoids
//import uis.cipsi.incognito.anonymization.dichotomize.TDichotomize
//import uis.cipsi.incognito.anonymization.buckets.TCloseBuckets
//import org.apache.spark.storage.StorageLevel
//import scala.collection.mutable.HashMap
//import uis.cipsi.incognito.anonymization.redistribution.RedistributeNew2
//import org.apache.spark.mllib.linalg.Vectors
//import org.apache.spark.mllib.linalg.Vector
//import breeze.linalg.DenseVector
//
//object TCloseMain {
//  Logger.getLogger("org").setLevel(Level.OFF)
//  Logger.getLogger("akka").setLevel(Level.OFF)
//
//  def main(args: Array[String]) = {
//    val sparkMaster = args(0)
//    //Path to the folder that contain the data, taxonomy tree and data def.
//    val filePath = args(1)
//    val outFilePath = args(2)
//    val folderName = args(3)
//    val fileName = args(4)
//    val indexes = args(5).split(",").map(_.toInt)
//
//    val dataPath = filePath + folderName
//    val taxonomyPath = filePath + fileName + ".taxonomy"
//    val dataStrPath = filePath + fileName + ".structure"
//
//    val taxonomySplitChar = ","
//    val dataSplitChar = ","
//    val dataStrSplitChar = ","
//
//    val pidIndex = indexes(0)
//    val saIndex = indexes(1)
//
//    val sc = CustomSparkContext.create(sparkMaster)
//
//    val _taxonomy = sc.textFile(taxonomyPath).map(t => t.split(taxonomySplitChar))
//      .map(t => (t(0), t(1))).collectAsMap
//
//    //        _taxonomy.foreach(v => println( v._1 + "(" + v._1.hashCode() + ") -> " + v._2 + "(" + v._2.hashCode() + ")") ) 
//    val taxonomy = sc.broadcast(_taxonomy)
//
//    val dataStr = sc.textFile(dataStrPath).map(t => t.split(dataStrSplitChar))
//      .map(t => (t(0).toInt, (t(1).toInt, t(2).toInt))).collectAsMap
//
//    val _categoricalQIHeights = dataStr.filter(p => p._2._1 == 0)
//      //we re-arrange the indexes, since the numeric and char values are seperated into 2 diff. vectors
//      .map({ var i = (-1); f => i += 1; (i, f._2._2) })
//    val categoricalQIHeights = sc.broadcast(_categoricalQIHeights)
//
//    val saTreeHeight = dataStr(saIndex)._2
//
//    val startTime = System.nanoTime();
//
//    val QIs = sc.objectFile[Data](dataPath)
//      .persist(StorageLevel.MEMORY_ONLY)
//
//    val saHist = QIs
//      .map(t => (t.saHash, 1))
//      .combineByKey((x: Int) => x,
//        (acc: Int, x: Int) => (acc + x),
//        (acc1: Int, acc2: Int) => (acc1 + acc2))
//      .persist(StorageLevel.MEMORY_ONLY)
//
//    val saCount = saHist.combineByKey((x: Int) => x,
//      (acc: Int, x: Int) => (acc + x),
//      (acc1: Int, acc2: Int) => (acc1 + acc2)).map(_._2).reduce(_ + _)
//
//    val SAs = saHist
//      .map(t => new SATuple(t._1, Array(t._1), t._2, (t._2.toDouble / saCount) //          * (1 - math.log((t._2.toDouble / saCount)))
//      )).persist(StorageLevel.MEMORY_ONLY)
//
//    val t = args(6).toDouble //0.8
//    println("\'t\'-closeness with \'t=\'" + t + " on a dataset with total records= " + saCount.toLong.toString)
//
//    val totalBucketEstimatedTime = System.nanoTime()
//    val buckets = new TCloseBuckets(taxonomy, saCount)
//
//    val bucketsIn: RDD[SATuple] = sc.parallelize(
//        buckets.getBuckets(SAs, height = saTreeHeight, t = t)).persist(StorageLevel.MEMORY_ONLY)
//
//    println("bucketization time= " + (System.nanoTime() - totalBucketEstimatedTime).toDouble + "seconds [for the step]")
//    val bucCount = bucketsIn.map(v => (-1, 1)).combineByKey((x: Int) => x,
//      (acc: Int, x: Int) => acc + x, (acc1: Int, acc2: Int) => acc1 + acc2).map(_._2).reduce(_ + _)
//    if (bucCount == 0) {
//      println("No buckets could be created with the given \'t=" + t + "\' for the dataset. Try changing the value of \'t\'")
//      System.exit(1)
//    }
//
//    SAs.unpersist(false)
//    println("total buckets= " + bucCount)
//
//    println("bucketization time= " + (System.nanoTime() - startTime).toDouble / 1000000000)
//    val bucketsSizes = bucketsIn.map(v => new BucketSizes(ecKey = new ECKey(level = 0, sideParent = "R", side = '0'),
//      bucketCode = v.bucketCode, size = v.freq, uBound = v.uBound)).persist(StorageLevel.MEMORY_ONLY)
//
//    val totalECEstimatedTime = System.nanoTime()
//
//    val ecSizes = new TDichotomize(t = t)
//    val ecSizesIn = ecSizes.getECSizes(bucketsSizes)
//
//    bucketsSizes.unpersist(false)
//    println("ec time= " + (System.nanoTime() - totalECEstimatedTime).toDouble / 1000000000 + " seconds")
//
//    val k = ecSizesIn.length
//    println("total ecs= " + k)
//
//    if (k == 1) {
//      println("only one ec generated.")
//      System.exit(1)
//    }
//
//    val qiCount = saCount.toLong
//    val _avgNumericQI = QIs.map(v => (-1, v.qisNumeric)).combineByKey((x: Vector) => x,
//      (acc: Vector, x: Vector) => Vectors.dense((new DenseVector(acc.toArray) + new DenseVector(x.toArray)).toArray),
//      (acc1: Vector, acc2: Vector) => Vectors.dense((new DenseVector(acc1.toArray) + new DenseVector(acc2.toArray)).toArray))
//      .map(_._2)
//      .reduce((acc1, acc2) => Vectors.dense((new DenseVector(acc1.toArray) + new DenseVector(acc2.toArray)).toArray))
//
//    val avgNumericQI = new DenseVector(_avgNumericQI.toArray).map(_ / qiCount)
//
//    val _sdQI = QIs.map(v => (-1, Vectors.dense(
//      (new DenseVector(v.qisNumeric.toArray) - avgNumericQI).map(x => math.pow(x, 2)).toArray)))
//      .combineByKey((x: Vector) => x,
//        (acc: Vector, x: Vector) => Vectors.dense((new DenseVector(acc.toArray) + new DenseVector(x.toArray)).toArray),
//        (acc1: Vector, acc2: Vector) => Vectors.dense((new DenseVector(acc1.toArray) + new DenseVector(acc2.toArray)).toArray))
//      .map(_._2)
//      .reduce((acc1, acc2) => Vectors.dense((new DenseVector(acc1.toArray) + new DenseVector(acc2.toArray)).toArray))
//
//    val sdQI = new DenseVector(_sdQI.toArray).map(_ / qiCount)
//
//    val standarizedQIs = QIs.map({ v =>
//      val zscore = Vectors.dense(((new DenseVector(v.qisNumeric.toArray) - avgNumericQI) / sdQI).toArray)
//      new Data(qiID = v.qiID, qisNumeric = zscore, qisCategorical = v.qisCategorical, saHash = v.saHash)
//    }).persist(StorageLevel.MEMORY_ONLY)
//
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
//
//    val tupleBucketMap = bucketsIn
//      .map(b => b.tuple.toArray.map(t => (t, b.bucketCode)).toArray).flatMap(f => f)
//      .reduceByKey((x, y) => (x))
//
//    val tupleBucketGroup = tupleBucketMap
//      .join(standarizedQIs.map(qi => (qi.saHash, qi)))
//      .map(v => (v._2._2.qiID, v))
//      .reduceByKey((x, y) => x)
//      //bcode, data
//      .map(v => (v._2._2)).persist(StorageLevel.MEMORY_ONLY)
//
//    //ecid, (seedid, size)
//    val ecKeysSizes = _seeds._2.map(v => (v._1._2, v._1._1)).toMap
//    val ecBukSizes = ecSizesIn.map(v => (v.ecKey, v.bucketCodeWithSize.toMap))
//    //((seedid, bukid), size)
//    val ecKeyBukCodeSizes = sc.broadcast(
//      ecBukSizes.map({ v => (ecKeysSizes(v._1), v._2) }).map(v => v._2.map(b => ((v._1, b._1), b._2)))
//        .flatMap(f => f).toMap)
//
//    val totalRedistributionTime = System.nanoTime()
//    val redistribute = new RedistributeNew2()
//    val _ecs = redistribute.start(tupleBucketGroup, seeds, ecKeyBukCodeSizes, qiCount)
//
//    //    val _ecCount = _ecs.map(v => (-1, 1)).combineByKey((x: Int) => x,
//    //      (acc: Int, x: Int) => acc + x, (acc1: Int, acc2: Int) => acc1 + acc2).map(_._2).reduce(_ + _)
//    //    println("total redistributed data= " + _ecCount)
//    tupleBucketGroup.unpersist(false)
//
//    println("redistribution time= " + (System.nanoTime() - totalRedistributionTime).toDouble / 1000000000 + " seconds")
//
//    val ecs = QIs.map(qis => (qis.qiID, qis)).join(_ecs.map({ case (e, q) => (q, e) })).map(v => (v._2._2, v._2._1)).persist(StorageLevel.MEMORY_ONLY)
//
//    val totalAnonymizationTime = System.nanoTime()
//    val anonymize = new Recording(ecs, taxonomy)
//    val anonymizedData = anonymize.generalize() //sc.objectFile[(Vector[String], Vector[Double], String)](outFilePath + "out/anonymizedData/" + fileName + "/*/")
//
//    //    val anonymizedDataCount = anonymizedData.map(v => (-1, 1)).combineByKey((x: Int) => x,
//    //      (acc: Int, x: Int) => acc + x, (acc1: Int, acc2: Int) => acc1 + acc2).map(_._2).reduce(_ + _)
//    //    println("total anonymized data= " + anonymizedDataCount)
//
//    println("recording time= " + (System.nanoTime() - totalAnonymizationTime).toDouble / 1000000000 + " seconds")
//
//    //    val totalAnonymizationWritingTime = System.nanoTime() 
//    //    anonymizedData.saveAsTextFile(outFilePath + "anonymized.dat")
//    //    println("recording hdfs write time= " + (totalAnonymizationWritingTime).toDouble / 1000000000 + " seconds")
//
//    QIs.unpersist(false)
//
//    val totalEstimatedTime = System.nanoTime() - startTime;
//    println("total time= " + (totalEstimatedTime).toDouble / 1000000000 + " seconds")
//
//  }
//}