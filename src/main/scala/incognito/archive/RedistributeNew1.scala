package incognito.anonymization.redistribution;
//
//
//package uis.cipsi.incognito.anonymization.redistribution
//
//import breeze.linalg.Vector
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
//class RedistributeNew1() {
//
//  def start(_tupleBucketGroup: RDD[(Int, Data)], _seeds: Broadcast[Array[(Int, (Vector[Double], Vector[String]))]],
//    ecKeyBukCodeSizes: Broadcast[Map[(Int, Int), Int]], rddCount: Long): RDD[(Int, Int)] = {
//
//    //tupleBucketGroup: (-1, bukCode), (-1, qid, numData))
//    val tupleBucketGroup = _tupleBucketGroup.map(v => ((-1, v._1), (Double.MaxValue, v._2.qiID, v._2.qisNumeric)))
//
//    //dat: (ecid, bukCode), (dist, qid, buk, point)
//    val dat = tupleBucketGroup
//      .map({
//        val ecSizes = ecKeyBukCodeSizes.value
//        x =>
//          val bukCode = x._1._2
//          val qid = x._2._2
//          val point = x._2._3
//          val _point = org.apache.spark.mllib.linalg.Vectors.dense(point.toArray)
//          val point_norm = org.apache.spark.mllib.linalg.Vectors.norm(_point, 2.0)
//          val bukForCurrSA = ecSizes.filter(e => e._1._2 == bukCode).filter(_._2 > 0)
//          val centersForCurrBuk = bukForCurrSA.map(_._1._1).toArray
//          val centers = _seeds.value.filter(c => centersForCurrBuk.contains(c._1))
//          val centerAtPrioriry = centers.map {
//            _center =>
//              val il = new InformationLoss
//              val center = _center._2._1
//              val __center = org.apache.spark.mllib.linalg.Vectors.dense(center.toArray)
//              val center_norm = org.apache.spark.mllib.linalg.Vectors.norm(__center, 2.0)
//              val _dist = il.fastSquaredDistance(__center, center_norm, _point, point_norm)
//              val dist = _dist
//              ((_center._1, qid), dist)
//          }
//          (bukCode, (centerAtPrioriry, qid, bukCode))
//      }).persist(StorageLevel.MEMORY_ONLY)
//
//    val ecs = dat
//      .groupByKey()
//      .mapValues({
//        //((ec, buk), size)
//        val vrec = ecKeyBukCodeSizes.value
//        //((ec, buk), size)
//        val addedEC = new HashMap[(Int, Int), Int]()
//        v =>
//          //((c,q), dist)
//          val centers = v.map(_._1).toArray.flatMap(f => f).toMap
//          val values = v.map(v => (v._2, v._3)).toArray
//          //qid, bukCode
//          val bestCenterForQIS = values.map { x =>
//            val qid = x._1
//            val bukCode = x._2
//            val qidCenters = centers.filter(_._1._2 == qid).toArray.sortBy(_._2)
//            var bestC = -1
//            breakable {
//              for (i <- 0 until qidCenters.length) {
//                val c = qidCenters(i)
//
//                if (vrec((c._1._1, bukCode)) > 0) {
//                  if (!addedEC.get((c._1._1, bukCode)).isEmpty) {
//                    if (addedEC((c._1._1, bukCode)) < vrec((c._1._1, bukCode))) {
//                      bestC = c._1._1
//                      val v = addedEC((c._1._1, bukCode))                     
//                      addedEC.update((c._1._1, bukCode), v + 1) 
//                      break
//                    }
//                  } else {
//                    bestC = c._1._1
//                    addedEC += ((c._1._1, bukCode) -> 1)
//                    break
//                  }
//                }
//              }
//            }
//
//            (qid, bestC)
//          }
//          bestCenterForQIS
//      })
//      .map(_._2)
//      .flatMap(f => f).persist(StorageLevel.MEMORY_ONLY)
//    ecs
//  }
//}