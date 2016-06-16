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
//
//import org.apache.spark.mllib.linalg.Vector
//
//class RedistributeNew2() {
//
//  def start(_tupleBucketGroup: RDD[(Int, Data)], _seeds: Broadcast[Array[(Int, (Vector, Array[String]))]],
//    ecKeyBukCodeSizes: Broadcast[Map[(Int, Int), Int]], rddCount: Long) //  : RDD[(Int, Int)] 
//    = {
//
//    //tupleBucketGroup: (bukCode, (bukCode, qid, numData))    
//    val tupleBucketGroup = _tupleBucketGroup
//      .map(v => (v._1, (v._1, v._2.qiID, v._2.qisNumeric)))
//    //
//    //    //    (bukCode, qid, point)
//    //    type tuple = (Int, Int, Vector)
//    //    //(bukCode, qid, Array[center, dist])
//    //    type tupleSubset = Array[(Int, Int, Array[(Int, Double)])]
//    //
//    //    val createCombiner = {
//    //      val ecSizes = ecKeyBukCodeSizes.value
//    //      val seeds = _seeds.value
//    //
//    //      (x: tuple) => {
//    //        val bukCode = x._1
//    //        val qid = x._2
//    //        val point = x._3
//    //
//    //        val _point = org.apache.spark.mllib.linalg.Vectors.dense(point.toArray)
//    //        val point_norm = org.apache.spark.mllib.linalg.Vectors.norm(_point, 2.0)
//    //
//    //        val bukForCurrSA = ecSizes.filter(e => e._1._2 == bukCode).filter(_._2 > 0)
//    //        val centersForCurrBuk = bukForCurrSA.map(_._1._1).toArray
//    //        val centers = seeds.filter(c => centersForCurrBuk.contains(c._1))
//    //        //(center, dist)
//    //        val centerAtPrioriry = centers.map {
//    //          val il = new InformationLoss
//    //          _center =>
//    //            val center = _center._2._1
//    //            val __center = org.apache.spark.mllib.linalg.Vectors.dense(center.toArray)
//    //            val center_norm = org.apache.spark.mllib.linalg.Vectors.norm(__center, 2.0)
//    //            val _dist = il.fastSquaredDistance(__center, center_norm, _point, point_norm)
//    //            val dist = _dist
//    //            (_center._1, dist)
//    //        }
//    //        Array((bukCode, qid, centerAtPrioriry))
//    //      }
//    //    }
//    //
//    //    val mergeValue = {
//    //      val ecSizes = ecKeyBukCodeSizes.value
//    //      val seeds = _seeds.value
//    //      (collector: tupleSubset, x: tuple) =>
//    //        {
//    //          val x_out = {
//    //            val bukCode = x._1
//    //            val qid = x._2
//    //            val point = x._3
//    //
//    //            val _point = org.apache.spark.mllib.linalg.Vectors.dense(point.toArray)
//    //            val point_norm = org.apache.spark.mllib.linalg.Vectors.norm(_point, 2.0)
//    //
//    //            val bukForCurrSA = ecSizes.filter(e => e._1._2 == bukCode).filter(_._2 > 0)
//    //            val centersForCurrBuk = bukForCurrSA.map(_._1._1).toArray
//    //            val centers = seeds.filter(c => centersForCurrBuk.contains(c._1))
//    //            //(center, dist)
//    //            val centerAtPrioriry = centers.map {
//    //              val il = new InformationLoss
//    //              _center =>
//    //                val center = _center._2._1
//    //                val __center = org.apache.spark.mllib.linalg.Vectors.dense(center.toArray)
//    //                val center_norm = org.apache.spark.mllib.linalg.Vectors.norm(__center, 2.0)
//    //                val _dist = il.fastSquaredDistance(__center, center_norm, _point, point_norm)
//    //                val dist = _dist
//    //                (_center._1, dist)
//    //            }
//    //            (bukCode, qid, centerAtPrioriry)
//    //          }
//    //          val out = collector ++ Array(x_out)
//    //          out
//    //        }
//    //    }
//    //
//    //    val mergeCombiners = {
//    //      //((ec, buk), size)
//    //      val ecSizes = ecKeyBukCodeSizes.value
//    //      //((ec, buk), size)
//    //      val addedEC = new HashMap[(Int, Int), Int]()
//    //      (collector1: tupleSubset, collector2: tupleSubset) => {
//    //        //Array[(bukCode, qid, Array[center, dist])]
//    //        val group = collector1 ++ collector2
//    //
//    //        val bestCenterForQIS: tupleSubset = group.map({ g =>
//    //          val bukCode = g._1
//    //          val qid = g._2
//    //          val centerAtPrioriry = g._3
//    //          var bestC = -1
//    //          breakable {
//    //            for (i <- 0 until centerAtPrioriry.length) {
//    //              val center = centerAtPrioriry(i)
//    //
//    //              if (ecSizes((center._1, bukCode)) > 0) {
//    //                if (!addedEC.get((center._1, bukCode)).isEmpty) {
//    //                  if (addedEC((center._1, bukCode)) < ecSizes((center._1, bukCode))) {
//    //                    bestC = center._1
//    //                    val v = addedEC((bestC, bukCode))
//    //                    addedEC.update((bestC, bukCode), v + 1)
//    //                    break
//    //                  }
//    //                } else {
//    //                  bestC = center._1
//    //                  addedEC += ((bestC, bukCode) -> 1)
//    //                  break
//    //                }
//    //              }
//    //            }
//    //          }
//    //          //(bukCode, qid, Array[center, dist])
//    //          (0, qid, Array((bestC, Double.MinValue)))          
//    //        })
//    //
//    //        bestCenterForQIS
//    //      }
//    //    }
//    //    //
//    //    val ecs = tupleBucketGroup
//    //    .combineByKey(createCombiner, mergeValue, mergeCombiners)
//    //    .map(v => (v._2.map({b => 
//    //      val qid = b._2;
//    //      val bestC = b._3.head._1    
//    //    (bestC, qid)}))).flatMap(f => f)
//    //
////    val ecs = tupleBucketGroup
////      .map({
////        //((ec, buk), size)
////        val ecSizes = ecKeyBukCodeSizes.value
////        //((ec, buk), size)
////        val addedEC = new HashMap[(Int, Int), Int]()
////        val seeds = _seeds.value
////        values =>
////          val bukCode = values._1
////          val qid = values._2._2
////
////          val point = values._2._3
////          val _point = org.apache.spark.mllib.linalg.Vectors.dense(point.toArray)
////          val point_norm = org.apache.spark.mllib.linalg.Vectors.norm(_point, 2.0)
////
////          val bukForCurrSA = ecSizes.filter(e => e._1._2 == bukCode).filter(_._2 > 0)
////          val centersForCurrBuk = bukForCurrSA.map(_._1._1).toArray
////          val centers = seeds.filter(c => centersForCurrBuk.contains(c._1))
////
////          var bestC = -1 
////          var bestDistance = Double.MaxValue
////          val centerAtPrioriry = centers.map {
////            val il = new InformationLoss
////            _center =>
////              val center = _center._2._1
////              val __center = org.apache.spark.mllib.linalg.Vectors.dense(center.toArray)
////              val center_norm = org.apache.spark.mllib.linalg.Vectors.norm(__center, 2.0)              
////              val _dist = il.fastSquaredDistance(__center, center_norm, _point, point_norm)
////              if(_dist < bestDistance) {
////                bestDistance = _dist
////                bestC = _center._1
////              }
////              val dist = _dist
////              ((_center._1, qid), dist)
////          }
////          (bestC, qid)
////      })
//      
//          val ecs = tupleBucketGroup.groupByKey()
//          .mapValues({
//            //((ec, buk), size)
//            val ecSizes = ecKeyBukCodeSizes.value
//            //((ec, buk), size)
//            val addedEC = new HashMap[(Int, Int), Int]()
//            val seeds = _seeds.value
//            values =>
//              val bestCenterForQIS = values.map { x =>
//                val bukCode = x._1
//                val qid = x._2
//    
//                val point = x._3
//                val _point = org.apache.spark.mllib.linalg.Vectors.dense(point.toArray)
//                val point_norm = org.apache.spark.mllib.linalg.Vectors.norm(_point, 2.0)
//    
//                val bukForCurrSA = ecSizes.filter(e => e._1._2 == bukCode).filter(_._2 > 0)
//                val centersForCurrBuk = bukForCurrSA.map(_._1._1).toArray
//                val centers = seeds.filter(c => centersForCurrBuk.contains(c._1))
//    
//                val centerAtPrioriry = centers.map {
//                  val il = new InformationLoss
//                  _center =>
//                    val center = _center._2._1
//                    val __center = org.apache.spark.mllib.linalg.Vectors.dense(center.toArray)
//                    val center_norm = org.apache.spark.mllib.linalg.Vectors.norm(__center, 2.0)
//                    val _dist = il.fastSquaredDistance(__center, center_norm, _point, point_norm)
//                    val dist = _dist
//                    ((_center._1, qid), dist)
//                }
//                var bestC = -1
//                breakable {
//                  for (i <- 0 until centerAtPrioriry.length) {
//                    val center = centerAtPrioriry(i)
//    
//                    if (ecSizes((center._1._1, bukCode)) > 0) {
//                      if (!addedEC.get((center._1._1, bukCode)).isEmpty) {
//                        if (addedEC((center._1._1, bukCode)) < ecSizes((center._1._1, bukCode))) {
//                          bestC = center._1._1
//                          val v = addedEC((center._1._1, bukCode))
//                          addedEC.update((center._1._1, bukCode), v + 1)
//                          break
//                        }
//                      } else {
//                        bestC = center._1._1
//                        addedEC += ((center._1._1, bukCode) -> 1)
//                        break
//                      }
//                    }
//                  }
//                }
//    
//                (bestC, qid)
//              }
//              bestCenterForQIS.toArray
//          })
//          .map(_._2).flatMap(f => f).persist(StorageLevel.MEMORY_ONLY)
//
//    ecs
//  }
//}