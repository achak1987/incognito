package incognito.anonymization.redistribution;
//
//package uis.cipsi.incognito.anonymization.redistribution
//
//import org.apache.spark.mllib.linalg.Vector
//import org.apache.spark.mllib.linalg.Vectors
//import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.rdd.RDD
//import uis.cipsi.incognito.rdd.Data
//import uis.cipsi.incognito.rdd.CentroidsAccum
//import org.apache.spark.storage.StorageLevel
//import uis.cipsi.incognito.rdd.ECS
//import scala.util.Random
//import scala.collection.mutable.ArrayBuffer
//import scala.collection.mutable.HashMap
//import uis.cipsi.incognito.rdd.ECKey
//import uis.cipsi.incognito.rdd.ECKey
//class RedistributeFinal(ecSizesIn: Array[ECS], numIteration: Int = 5) {
//  def start(_tupleBucketGroup: RDD[(Int, Data)]): RDD[(Int, ECKey)] = {
//
//    //    val ecKeysWithNewIndex = _tupleBucketGroup.sparkContext.broadcast(ecSizesIn
//    //      .zipWithIndex
//    //      .map({ case (k, v) => (v, k.bucketCodeWithSize) }))
//    val ecKeysWithNewIndex = _tupleBucketGroup.sparkContext.broadcast(ecSizesIn
//      .map(v => (v.ecKey, v.bucketCodeWithSize)))
//
//    val acCenter = _tupleBucketGroup.sparkContext.accumulable((new CentroidsAccum)
//      .zero(Array((-1, Double.MaxValue))))(new CentroidsAccum)
//
//    //norm, (bukCode, qid)
//    val _dataWithNorm = _tupleBucketGroup
//      .map(v => (Vectors.norm(v._2.qisNumeric, 2.0), (v._1, v._2.qiID)))
//
//    //data_norm, bestCenterNorm
//    val _dataNormWithBestCenterNorm = _dataWithNorm
//      //bukCode, ((bukCode,qid), norm)
//      .map(v => (v._2._1, (v._2._2, v._1)))
//      .groupByKey
//
////    val numPartitions = _dataNormWithBestCenterNorm.partitions.length
//
//    val dataNormWithBestCenterNorm = _dataNormWithBestCenterNorm
////      .repartition(numPartitions)
//      .map({
//        val _ecForThisBucket = ecKeysWithNewIndex.value
//        _values =>
//          val values = _values._2
//          val bukCode = _values._1
//          val ecForThisBucket = _ecForThisBucket.map(e => (e._1, e._2(bukCode)))
//            .filter(_._2 != 0).sortBy(_._2).reverse
//
//          val k = ecForThisBucket.length
//          val r = new Random
//          val _centers = r.shuffle(values).take(1).map(_._2)
//          var centers = new ArrayBuffer[Double]
//          _centers.foreach(c => centers += c)
//
//          var cost = 0.0
//          values.foreach { d =>
//            var bestDistance = Double.MaxValue
//            centers.foreach {
//              center =>
//                val dist = math.pow(d._2 - center, 2)
//                if (dist < bestDistance) { bestDistance = dist; cost += bestDistance }
//            }
//          }
//
//          var i = 0
//          var costLog = math.log(cost)
//          while (i < numIteration && i < costLog && cost > 0.0) {
//            i += 1
//            val r = new XORShiftRandom(123456789)
//            var _cost = 0.0
//            values.foreach({
//              d =>
//                var bestDistance = Double.MaxValue
//                centers.foreach(c => {
//                  val dist = math.pow(d._2 - c, 2)
//                  if (dist < bestDistance) {
//                    bestDistance = dist
//                  }
//                })
//                val p = r.nextDouble
//                val valid = p < (2.0 * k * bestDistance / cost)
//
//                if (valid) { centers += d._2; _cost += bestDistance }
//            })
//            cost = _cost
//          }
//
//          val _finalCenters =
//            (if (centers.length <= k) {
//              val centers = r.shuffle(values).take(k).map(_._2)
//              centers.toArray
//            } else {
//              centers.map({ v =>
//                var bestDistance = Double.MaxValue
//                var bestIndex = -1
//                var bestNorm = Double.MinValue
//                centers.filter(c => c != v).foreach(c => {
//                  val dist = math.sqrt(math.pow(v - c, 2))
//                  if (dist < bestDistance) {
//                    bestNorm = c
//                    bestDistance = dist
//                  }
//                })
//                (bestNorm, 1)
//              }).groupBy(_._1).toArray
//                .map(v => (v._1, v._2.map(_._2).sum))
//                .sortBy(_._2).reverse.take(k).map(_._1)
//            })
//
//          //At this stage we sort the centers with most points that belong to them
//          val finalCenters = values.map({
//            d =>
//              var bestDistance = Double.MaxValue
//              var bestNorm = Double.MinValue
//              _finalCenters.foreach(c => {
//                val dist = math.pow(d._2 - c, 2)
//                if (dist < bestDistance) {
//                  bestDistance = dist
//                  bestNorm = c
//
//                }
//              })
//              (bestNorm, 1)
//          })
//            .groupBy(_._1).toArray
//            .map(v => (v._1, v._2.map(_._2).sum))
//            .sortBy(_._2).reverse.toArray
//
//          //(eid, bukcode) -> size
//          val addECSizes = HashMap(new ECKey(Short.MinValue, "0", '0') -> 0)
//
//          val out = values.map({
//            d =>
//              val qid = d._1
//              var bestDistance = Double.MaxValue
//              var bestCenterIndex = new ECKey(Short.MinValue, "0", '0')
//
//              val ecs = ecForThisBucket
//                .filter({ c =>
//                  if (addECSizes.get(c._1).isEmpty) { addECSizes += (c._1 -> 0); true }
//                  else addECSizes(c._1) < c._2
//                }).toIterable
//
//              val filteredSeeds = finalCenters
//                .zip(ecs)
//                .map({ v => ((v._1._1, v._2._2), v._2._1) })
//
//              filteredSeeds.foreach(c => {
//
//                val dist = math.pow(d._2 - c._1._1, 2)
//
//                if (dist < bestDistance) {
//                  bestDistance = dist
//                  bestCenterIndex = c._2
//                }
//              })
//
//              addECSizes(bestCenterIndex) += 1
//              //bucCode + value_norm(unique), bestCenterNorm
//              (qid, bestCenterIndex)
//          })
//          out
//      })//.map(_._2)
//      .flatMap(f => f)
//      .filter(_._2.level != Short.MinValue)
//    //      .persist(StorageLevel.MEMORY_ONLY)
//
//    dataNormWithBestCenterNorm
//  }
//}
//
