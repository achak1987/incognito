//package uis.cipsi.incognito.experiments
//
//import uis.cipsi.incognito.rdd.CustomSparkContext
//import uis.cipsi.incognito.rdd.{ ECKey, Data }
//import java.util.Arrays
//import breeze.linalg.Vector
//import org.apache.log4j.Logger
//import org.apache.log4j.Level
//import org.apache.spark.rdd.RDD
//import scala.collection.mutable.ArrayBuffer
//import uis.cipsi.incognito.rdd.BucketSizes
//import uis.cipsi.incognito.anonymization.dichotomize.Dichotomize
//import uis.cipsi.incognito.rdd.SATuple
//import uis.cipsi.incognito.anonymization.buckets.BetaBuckets
//import uis.cipsi.incognito.anonymization.redistribution.KMedoids
//import uis.cipsi.incognito.informationLoss.InformationLoss
//
//object Quality {
//
//  Logger.getLogger("org").setLevel(Level.OFF)
//  Logger.getLogger("akka").setLevel(Level.OFF)
//
//  def main(args: Array[String]): Unit = {
//    val sparkMaster = args(0)
//    //Path to the folder that contain the data, taxonomy tree and data def.
//    val filePath = args(1)
//    val fileName = args(2)
//    val redistributedECPath = args(3)
//    val indexes = args(4).split(",").map(_.toInt)
//    val algo = args(5).split(",")
//    val algoType = algo(0)
//    val bound = algo(1).toDouble
//    val beta = if (algoType == "beta") bound else 0.0
//    val t = if (algoType == "tclose") bound else 0.0
//
//    val dataPath = filePath + fileName
//    val taxonomyPath = dataPath + ".taxonomy"
//    val dataStrPath = dataPath + ".structure"
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
//    val data = sc.textFile(dataPath).map(_.split(dataSplitChar).map(v => v.trim)).cache()
//
//    val saHist = data.map(t => (t(saIndex).hashCode, 1)).reduceByKey(_ + _)
//    val saCount = saHist.map(_._2).sum
//
//    println("Anonymization data quality for algoType= " + algoType)
//
//    val SAs = if (algoType == "beta") {
//      saHist
//        .map({ t =>
//          val p = t._2 / saCount;
//          new SATuple(t._1, Vector(t._1), t._2, 1.0 *
//            (if (p <= math.pow(math.E, -1 * beta)) p * (1 + beta) else p * (1 - math.log(p))))
//        })
//    } else {
//      saHist
//        .map(t => new SATuple(t._1, Vector(t._1), t._2, 1.0 * (t._2 / saCount) * (1 - math.log((t._2 / saCount)))))
//    }
//
//    val buckets = new BetaBuckets(saCount)
//    buckets.getBuckets(SAs)
//
//    val bucketsIn: RDD[SATuple] = buckets.getBuckets(SAs) //sc.objectFile(outFilePath + "out/buckets/*/")
//
//    val bucCount = bucketsIn.count
//    if (bucCount == 0) {
//      println("No buckets could be created with the given \'beta=" + beta + "\' for the dataset. Try changing the value of \'beta\'")
//      System.exit(1)
//    }
//    println("total buckets= " + bucCount)
//
//    val bucketsSizes = bucketsIn.map(v => new BucketSizes(ecKey = new ECKey(level = 0, sideParent = "R", side = '0'),
//      bucketCode = v.bucketCode, size = v.freq, uBound = v.uBound))
//
//    val ecSizes = new Dichotomize    
//    val ecSizesIn = ecSizes.getECSizes(bucketsSizes)    
//
//    val k = ecSizesIn.length //ecSizesIn.map(b => (b.ecKey, 1)).reduceByKey(_ + _).map(v => 1).reduce(_ + _)
//    println("total ecs= " + k)
//
//    if (k == 1) {
//      println("only one ec generated. try changing the value of \'beta\'")
//      System.exit(1)
//    }
//
//    //Redistribution
//    val QIs = data.map(t => (t(saIndex).hashCode, t.filter({ var i = (-1); x => i += 1; i != saIndex })))
//      .map({
//        x =>
//          val nQIs = new ArrayBuffer[Double]();
//          val cQIs = new ArrayBuffer[String]();
//          val qis = x._2.map({
//            var i = (-1); qi => i += 1;
//            //We convert the primary identifier to hashcode
//            if (i == pidIndex) nQIs += qi.toString.hashCode().toDouble else if (dataStr(i)._1 == 1) nQIs += qi.toDouble else cQIs += qi.toString
//          })
//          val ncQIs = (nQIs ++ cQIs).map(_.hashCode()).toArray
//          new Data(qiID = Arrays.hashCode(ncQIs), qisNumeric = Vector(nQIs.toArray), qisCategorical = Vector(cQIs.toArray), saHash = x._1)
//      })
//
//    val qiCount = QIs.count
//    val avgNumericQI = QIs.map(_.qisNumeric).reduce(_ + _).map(_ / qiCount)
//
//    val sdQI = QIs.map(v => (v.qisNumeric - avgNumericQI).map(x => math.pow(x, 2))).reduce(_ + _).map(v => math.sqrt(v / qiCount))
//    val standarizedQIs = QIs.map({ v =>
//      val zscore = (v.qisNumeric - avgNumericQI) / sdQI;
//      new Data(qiID = Arrays.hashCode(zscore.toArray), qisNumeric = zscore, qisCategorical = v.qisCategorical, saHash = v.saHash)
//    })
//
//
//    val kmediods = new KMedoids(standarizedQIs, taxonomy, categoricalQIHeights, pidIndex, k, qiCount)
//
//val bucketsWSAs = bucketsIn.map { x => (x.bucketCode, x.tuple) }.collect.toMap
//    //    val initialCentroids: Array[Data] = kmediods.initialize(1) //sc.objectFile(outFilePath + "out/initialize/" + "/*/")
//    val seeds = kmediods.initialize(bucketsWSAs, ecSizesIn)
////    val iniCntCount = initialCentroids.count
////    if (iniCntCount < k) {
////      println("no of initial centroids = " + iniCntCount + " not equal to k= " + k)
////      System.exit(1)
////    }
//
////    
////    ecSizesIn.map(ec => (ec.ecKey, ec.bucketCode)).groupByKey.mapValues { _.toSeq }.collect.foreach(println)
////    System.exit(1)
////    val seeds = initialCentroids.map(v => (v.qisNumeric, v.qisCategorical))//kmediods.kMedoids(initialCentroids, sc.broadcast(initialCentroids.collect)).map(v => (v.qisNumeric, v.qisCategorical))
//
////
////    println("no. of seeds= " + seeds.length)
////
////    val _ecWithSeeds = sc.broadcast(ecSizesIn.map(ec => (ec.ecKey, ec.bucketCode)).groupByKey.collect.zip(seeds))
////
////    val tupleBucketMap = bucketsIn.map(b => b.tuple.map(t => (t, b.bucketCode)).toArray).flatMap(f => f)
////    val tupleBucketGroup = tupleBucketMap
////      .join(QIs.map(qi => (qi.saHash, qi)))
////
////    val tupleEC = tupleBucketGroup
////      .map(t => (t._2._1, t._2._2))
////      .map({
////        val IL = new InformationLoss(taxonomy, categoricalQIHeights)
////        val ecWithSeeds = _ecWithSeeds.value
////        x =>
////          val ecPriorities = ecWithSeeds.map(ec => (ec._1._1,
////            IL.distance(ec._2._1, (x._2.qisNumeric - avgNumericQI) / sdQI, pidIndex) +
////            IL.distance(ec._2._2, x._2.qisCategorical)))
////            .sortBy(_._2)
////          (x._2.qiID,
////            (ecPriorities, x._2))
////      })
////
////    val tupleECDistanceGroup = tupleEC.reduceByKey((v1, v2) => v1)
////      .map({ v => val out = v._2._1.map({ var i = (-1); a => i += 1; (a._1, (i, a._2, v._1)) }); out })
////      .flatMap(f => f)
////
////    val redistribute = new Redistribute(sc, tupleECDistanceGroup, ecSizesIn)
////
////    val _ecs = redistribute.start(qiCount)
////    val ecs = QIs.map(qis => (qis.qiID, qis)).join(_ecs).map(v => (v._2._2, v._2._1))
////
////    implicit val CategoricalQIModeOrdering = new Ordering[(Vector[String], Int)] {
////      override def compare(x: (Vector[String], Int), y: (Vector[String], Int)): Int = {
////        var freqCompare = x._2 - y._2
////        if (freqCompare == 0) x._1.map({ var i = (-1); x => i += 1; x.compare(y._1(i)) }).sum else freqCompare
////      }
////    }
////
////    val sdNumeriQI = ecs.map(v => (v._2.qisNumeric - avgNumericQI)
////      .map(x => math.pow(x, 2))).reduce(_ + _).map(v => math.sqrt(v / qiCount))
////    val standarizedQIsIL = ecs.map({ v =>
////      val zscore = (v._2.qisNumeric - avgNumericQI) / sdNumeriQI;
////      (v._1, new Data(qiID = Arrays.hashCode(zscore.toArray), qisNumeric = zscore,
////        qisCategorical = v._2.qisCategorical, saHash = v._2.saHash))
////    })
////
////    //    standarizedQIs.collect.foreach(println)
////    //    System.exit(1)
////
////    val centroids = standarizedQIsIL.groupByKey.mapValues({ d =>
////      val x = d.toArray
////      val valsN = d.map(_.qisNumeric)
////      val valsC = d.map(_.qisCategorical)
////      val count = valsN.size;
////      val avg = valsN.reduce(_ + _).map(_ / count)
////      val categoricalQI = valsC.map(v => (v, 1)).groupBy(v => Arrays.hashCode(v._1.map(_.hashCode).toArray))
////        .map { case (group: Int, traversable) => traversable.reduce { (a, b) => (a._1, a._2 + b._2) } }
////        .toArray.sortBy(_._2) //.reverse.map(_._1).take(1).head
////      val mode = categoricalQI.max._1
////      (avg, mode)
////    })
////
////    val ecCosts = standarizedQIsIL.join(centroids)
////      .map({
////        val IL = new uis.cipsi.incognito.informationLoss.InformationLoss(taxonomy, categoricalQIHeights)
////        e =>
////          val dist =
////            IL.distance(e._2._1.qisNumeric, e._2._2._1, pidIndex) +
////              IL.distance(e._2._1.qisCategorical, e._2._2._2)
////          (e._1, dist)
////      })
////      .reduceByKey(_ + _)
////    //    println("Costs per EC")
////    //    ecCosts.collect.foreach(println)
////    val cost = ecCosts.map(_._2).reduce(_ + _)
////    val cnt = ecCosts.count()
////    println("Avg. Cost= " + cost / cnt)
////    println("Total Cost= " + cost)
//  }
//}