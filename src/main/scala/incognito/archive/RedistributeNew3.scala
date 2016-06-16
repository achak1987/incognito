package incognito.anonymization.redistribution;
//
//
//package uis.cipsi.incognito.anonymization.redistribution
//
//import org.apache.spark.rdd.RDD
//import scala.collection.mutable.HashMap
//import org.apache.spark.SparkContext
//import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
//import uis.cipsi.incognito.rdd.{ BucketSizes, ECKey }
//import uis.cipsi.incognito.utils.Utils
//import org.apache.spark.broadcast.Broadcast
//import uis.cipsi.incognito.rdd.Data
//import uis.cipsi.incognito.rdd.ECKey
//import org.apache.spark.storage.StorageLevel
//import uis.cipsi.incognito.rdd.RedistributionStats
//import uis.cipsi.incognito.informationLoss.InformationLoss
//import scala.util.control.Breaks._
//import org.apache.spark.HashPartitioner
//import org.apache.spark.mllib.linalg.Vector
//import uis.cipsi.incognito.rdd.SATuple
//import org.apache.spark.mllib.linalg.Vectors
//import breeze.linalg.DenseVector
//import uis.cipsi.incognito.rdd.ECS
//import uis.cipsi.incognito.rdd.CentroidsAccum
//import scala.util.Random
//import scala.collection.mutable.ArrayBuffer
//
//class RedistributeNew3(QIs: RDD[Data], bucketsIn: RDD[SATuple], ecSizesIn: Array[ECS], qiCount: Long) extends Serializable {
//
//  val _avgNumericQI = QIs.map(v => (-1, v.qisNumeric))
//    .combineByKey((x: Vector) => x,
//      (acc: Vector, x: Vector) => Vectors.dense((new DenseVector(acc.toArray) + new DenseVector(x.toArray)).toArray),
//      (acc1: Vector, acc2: Vector) => Vectors.dense((new DenseVector(acc1.toArray) + new DenseVector(acc2.toArray)).toArray))
//    .map(_._2)
//    .reduce((acc1, acc2) => Vectors.dense((new DenseVector(acc1.toArray) + new DenseVector(acc2.toArray)).toArray))
//
//  val avgNumericQI = new DenseVector(_avgNumericQI.toArray).map(_ / qiCount)
//
//  val _sdQI = QIs.map(v => (-1, Vectors.dense(
//    (new DenseVector(v.qisNumeric.toArray) - avgNumericQI).map(x => math.pow(x, 2)).toArray)))
//    .combineByKey((x: Vector) => x,
//      (acc: Vector, x: Vector) => Vectors.dense((new DenseVector(acc.toArray) + new DenseVector(x.toArray)).toArray),
//      (acc1: Vector, acc2: Vector) => Vectors.dense((new DenseVector(acc1.toArray) + new DenseVector(acc2.toArray)).toArray))
//    .map(_._2)
//    .reduce((acc1, acc2) => Vectors.dense((new DenseVector(acc1.toArray) + new DenseVector(acc2.toArray)).toArray))
//
//  val sdQI = new DenseVector(_sdQI.toArray).map(_ / qiCount)
//
//  val standarizedQIs = QIs.map({ v =>
//    val zscore = Vectors.dense(((new DenseVector(v.qisNumeric.toArray) - avgNumericQI) / sdQI).toArray)
//    new Data(qiID = v.qiID, qisNumeric = zscore, qisCategorical = v.qisCategorical, saHash = v.saHash)
//  }).persist(StorageLevel.MEMORY_ONLY)
//
//  //saHash, BukCode
//  val tupleBucketMap = bucketsIn
//    .map(b => b.tuple.map(t => (t, b.bucketCode))).flatMap(f => f)
//
//  //there may be duplicate records spread across different buckets, since inognito uses overlapping buckets
//  val _tupleBucketGroup = tupleBucketMap
//    .join(standarizedQIs.map(qi => (qi.saHash, qi)))
//    .map(v => (v._2._2.qiID, v)) //saHash, (bukCode, Data)
//    //bukCode, data
//    .map(v => (v._2._2)).persist(StorageLevel.MEMORY_ONLY)
//
//  def start(numIteration: Int = 1): RDD[(ECKey, Data)] = {
//
//    val ecKeysWithNewIndex = _tupleBucketGroup.sparkContext.broadcast(ecSizesIn
//      .map(v => (v.ecKey, v.bucketCodeWithSize)))
//
//    val acCenter = _tupleBucketGroup.sparkContext.accumulable((new CentroidsAccum)
//      .zero(Array((-1, Double.MaxValue))))(new CentroidsAccum)
//
//    //(bukCode, (bukCode, qiID, (Data, norm)))  
//    val tupleBucketGroup = _tupleBucketGroup //(bukCode, Data)
//      .map(v => (v._1, (v._1, (v._2.qiID, v._2.qisNumeric, Vectors.norm(v._2.qisNumeric, 2.0)))))
//      .groupByKey
//
//    val ecs = tupleBucketGroup.mapValues({
//      val _ecForThisBucket = ecKeysWithNewIndex.value
//      //(bukCode, (qiID, Vector, Norm))
//      _values =>
//        val values = _values.map(_._2)
//        val bukCode = _values.map(_._1).max
//        val ecForThisBucket = _ecForThisBucket.map(e => (e._1, e._2(bukCode)))
//          .filter(_._2 != 0).sortBy(_._2).reverse
//
//        val k = ecForThisBucket.length
//        val r = new Random
//        val _centers = r.shuffle(values).take(1)
//        var centers = new ArrayBuffer[(Int, Vector, Double)]
//        _centers.foreach(c => centers += c)
//        var cost = 0.0
//
//        values.foreach { d =>
//          var bestDistance = Double.MaxValue
//          centers.foreach {
//            center =>
//              val dist = new InformationLoss().fastSquaredDistance(d._2,
//                d._3,
//                center._2,
//                center._3)
//              if (dist < bestDistance) { bestDistance = dist; cost += bestDistance }
//          }
//        }
//
//        var i = 0
//        var costLog = math.log(cost)
//        while (i < numIteration && i < costLog && cost > 0.0) {
//          i += 1
//          val r = new XORShiftRandom(123456789)
//          var _cost = 0.0
//          values.foreach({
//            d =>
//              var bestDistance = Double.MaxValue
//              centers.foreach(center => {
//                val dist = new InformationLoss().fastSquaredDistance(d._2,
//                  d._3,
//                  center._2,
//                  center._3)
//                if (dist < bestDistance) {
//                  bestDistance = dist
//                }
//              })
//              val p = r.nextDouble
//              val valid = p < (2.0 * k * bestDistance / cost)
//
//              if (valid) { centers += d; _cost += bestDistance }
//          })
//          cost = _cost
//        }
//
//        val _finalCenters: Array[(Int, Vector, Double)] =
//          (if (centers.length == k) {
//            val centers = r.shuffle(values).take(k)
//            centers.toArray
//          } else {
//            centers.map({ v =>
//              var bestDistance = Double.MaxValue
//              var bestNorm = Double.MinValue
//              centers.filter(c => c != v).foreach(c => {
//                val dist = new InformationLoss().fastSquaredDistance(v._2,
//                  v._3,
//                  c._2,
//                  c._3)
//                if (dist < bestDistance) {
//                  bestNorm = c._3
//                  bestDistance = dist
//                }
//              })
//              (bestNorm, 1, v)
//            }).groupBy(_._1).toArray //remove duplicates
//              .map(v => (v._1, v._2.map(_._2).sum, v._2.map(_._3).head))
//              .sortBy(_._2).reverse.map(_._3)
//          })
//
//        //At this stage we sort the centers with most points that belong to them
//        val finalCenters = values.map({
//          d =>
//            var bestDistance = Double.MaxValue
//            var bestNorm = Double.MinValue
//            var bestCenter = (1, Vectors.dense(1.0), 1.0)
//            _finalCenters.foreach(c => {
//              val dist = new InformationLoss().fastSquaredDistance(d._2,
//                d._3,
//                c._2,
//                c._3)
//              if (dist < bestDistance) {
//                bestDistance = dist
//                bestNorm = c._3
//                bestCenter = c
//
//              }
//            })
//            (bestNorm, 1, bestCenter)
//        })
//          .groupBy(_._1).toArray
//          .map(v => (v._1, v._2.map(_._2).sum, v._2.map(_._3).head))
//          .sortBy(_._2).map(_._3).reverse.take(k).toArray
//
//        //(eid, bukcode) -> size
//        val addECSizes = HashMap(new ECKey(Short.MinValue, "0", '0') -> 0)
//
//        val out = values.map({
//          d =>
//            val qid = d._1
//            var bestDistance = Double.MaxValue
//            var bestCenterIndex = new ECKey(Short.MinValue, "0", '0')
//
//            val ecs = ecForThisBucket
//              .filter({ c =>
//                if (addECSizes.get(c._1).isEmpty) { addECSizes += (c._1 -> 0); true }
//                else addECSizes(c._1) < c._2
//              }).toIterable
//
//            val filteredSeeds = finalCenters
//              .zip(ecs)
//              .map({ v => ((v._1, v._2._2), v._2._1) })
//
//            filteredSeeds.foreach(c => {
//
//              val dist = new InformationLoss().fastSquaredDistance(d._2,
//                d._3,
//                c._1._1._2,
//                c._1._1._3)
//
//              if (dist < bestDistance) {
//                bestDistance = dist
//                bestCenterIndex = c._2
//              }
//            })
//
//            addECSizes(bestCenterIndex) += 1
//            (qid, bestCenterIndex)
//        })
//        out.toArray
//    }).map(_._2)
//      .flatMap(f => f)
//      .filter(_._2.level != Short.MinValue)
//      .persist(StorageLevel.MEMORY_ONLY)
//
//    val out = QIs.map(qis => (qis.qiID, qis))
//      .join(ecs).map(v => (v._2._2, v._2._1))
//    tupleBucketGroup.unpersist(false)
//    out
//  }
//}