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
//
//class RedistributeNew() {
//
//  def start(_tupleBucketGroup: RDD[(Int, Data)], _seeds: Broadcast[Array[(Int, (Vector[Double], Vector[String]))]],
//    ecKeyBukCodeSizes: Broadcast[Map[(Int, Int), Int]], rddCount: Long): RDD[(Int, Int)] = {
//
//    //tupleBucketGroup: (-1, bukCode), (-1, qid, numData))
//    val tupleBucketGroup = _tupleBucketGroup.map(v => ((-1, v._1), (Double.MaxValue, v._2.qiID, v._2.qisNumeric)))
//    var remainingECs = ecKeyBukCodeSizes.value
//
//    val bRemainingECs = _tupleBucketGroup.sparkContext.broadcast(remainingECs)
//
//    val reStat = _tupleBucketGroup.sparkContext.accumulable((new RedistributionStats).zero(HashMap((-1, -1) -> 0)))(new RedistributionStats)
//       
//
//    //dat: (ecid, bukCode), (dist, qid, buk, point)
//    var dat = tupleBucketGroup
//      .map({
//        val ecSizes = bRemainingECs.value
//        val centers = _seeds.value
//        x =>
//          val bukCode = x._1._2
//          val qid = x._2._2
//          val point = x._2._3
//          val _point = org.apache.spark.mllib.linalg.Vectors.dense(point.toArray)
//          val point_norm = org.apache.spark.mllib.linalg.Vectors.norm(_point, 2.0)
//          
//          var bestIndex = -1
//          var bestDistance = Double.MaxValue
//          val il = new InformationLoss
//          val centerAtPrioriry = centers.foreach {
//            _center =>
//              val center = _center._2._1
//              val size = ecSizes((_center._1, bukCode))
//              val __center = org.apache.spark.mllib.linalg.Vectors.dense(center.toArray)
//              val center_norm = org.apache.spark.mllib.linalg.Vectors.norm(__center, 2.0)
//              val _dist = il.fastSquaredDistance(__center, center_norm, _point, point_norm)
//              val dist = _dist / size
//              if (dist < bestDistance) {
//                bestIndex = _center._1
//                bestDistance = _dist
//              }
//          }
//          //(ecID, bukCode), (Distance, QiD, bukCode, numData)
//          ((bestIndex, bukCode), (bestDistance, qid, bukCode, point))
//      }).persist(StorageLevel.MEMORY_ONLY)
//      
//
//    val ecsTmp = dat
//      .map(v => (v._1, (v._1._1, v._2)))
//      .groupByKey()
//      .mapValues({
//        val vrec = bRemainingECs.value
//        v =>
//          //(e, (d, q, b ))
//          val values = v.toArray
//          val ecGr = values.sortBy(_._2._1).zipWithIndex.filter(f => f._2 < vrec((f._1._1, f._1._2._3)))
//            //((e,b), q)
//            .map(v => ((v._1._1, v._1._2._3), v._1._2._2))
//          ecGr
//      })
//      .map(_._2)
//      .flatMap(f => f).persist(StorageLevel.MEMORY_ONLY)
//
//    ecsTmp.foreach(v => reStat += (v._1, 1))
//
//    remainingECs = remainingECs.map({ ec =>
//      val ecKey = ec._1
//      val v = ec._2
//      val rs = reStat.value.get(ecKey)
//      (ecKey, if (rs.isEmpty) v else if (rs.get >= v) 0 else if (rs.get < v) (v - rs.get).toInt else v)
//    })
//
//    //qid,ecid
//    val _completeECs = ecsTmp.map(v => (v._2, v._1._1))
//    var completeECs = _completeECs.persist(StorageLevel.MEMORY_ONLY)
//    //dat: (ecid, bukCode), (dist, qid, buk, point)
//    dat = dat.map(v => (v._2._2, v)).subtractByKey(_completeECs).map(_._2).persist(StorageLevel.MEMORY_ONLY)
//
//    var priorityEC = 0
//    val noOfSeeds = _seeds.value.size
//    while (!dat.isEmpty()) {
//      println("remainingECs= " + remainingECs.map(_._2).sum)
//      //      println(remainingECs.filter(_._2 != 0).toArray.sortBy(_._2).reverse.toSeq)
//
//      val bRemainingECs = _tupleBucketGroup.sparkContext.broadcast(remainingECs)
//      val reStat = _tupleBucketGroup.sparkContext.accumulable((new RedistributionStats).zero(HashMap((-1, -1) -> 0)))(new RedistributionStats)
//
//      priorityEC += 1
//
//      dat = dat
//        .map({
//          val ecSizes = bRemainingECs.value
//
//          val _centers = _seeds.value
//          x =>
//            val bukCode = x._1._2
//            val qid = x._2._2
//            val point = x._2._4
//            val _point = org.apache.spark.mllib.linalg.Vectors.dense(point.toArray)
//            val point_norm = org.apache.spark.mllib.linalg.Vectors.norm(_point, 2.0)
//
//            val il = new InformationLoss
//            val centers = _centers.map({
//              _center =>
//                val center = _center._2._1
//                val __center = org.apache.spark.mllib.linalg.Vectors.dense(center.toArray)
//                val center_norm = org.apache.spark.mllib.linalg.Vectors.norm(__center, 2.0)
//                val size = ecSizes((_center._1, bukCode))
//                val _dist = il.fastSquaredDistance(__center, center_norm, _point, point_norm)
//                val dist = if (size != 0) _dist / size else Double.MaxValue
//                (_center._1, dist, _dist)
//            }).sortBy(_._2)
//
//            var distance = centers(priorityEC)._3
//            var index = centers(priorityEC)._1
//
//            //            val c = ecSizes.filter(_._1._2 == bukCode).maxBy(_._2)
//            //            val index = c._1._1            
//            //            val center = _centers.filter(_._1 == index).head
//            //            val __center = org.apache.spark.mllib.linalg.Vectors.dense(center._2._1.toArray)
//            //            val center_norm = org.apache.spark.mllib.linalg.Vectors.norm(__center, 2.0)
//            //            val il = new InformationLoss
//            //            val distance = il.fastSquaredDistance(__center, center_norm, _point, point_norm)
//
//            if (centers(priorityEC)._2 == Double.MaxValue) {
//              val max = ecSizes.toArray.filter(_._1._2 == bukCode).maxBy(_._2)
//              val c = centers.filter(_._1 == max._1._1).head
//              distance = c._3
//              index = c._1
//            }
//
//            ((index, bukCode), (distance, qid, bukCode, point))
//        }).persist(StorageLevel.MEMORY_ONLY)
//
//      val ecsTmp = dat
//        .map(v => (v._1, (v._1._1, v._2)))
//        .groupByKey()
//        .mapValues({
//          val vrec = bRemainingECs.value
//          v =>
//            //(e, (d, q, b ))
//            val values = v.toArray
//            val ecGr = values.sortBy(_._2._1).zipWithIndex.filter(f => f._2 < vrec((f._1._1, f._1._2._3)))
//              //((e,b), q)
//              .map(v => ((v._1._1, v._1._2._3), v._1._2._2))
//            ecGr
//        })
//        .map(_._2)
//        .flatMap(f => f).persist(StorageLevel.MEMORY_ONLY)
//
//      ecsTmp.foreach(v => reStat += (v._1, 1))
//
//      //      println("reStat.value= " + reStat.value.map(_._2).sum)
//      //      println(reStat.value.filter(_._2 != 0).toArray.sortBy(_._2).reverse.toSeq)
//
//      remainingECs = remainingECs.map({ ec =>
//        val ecKey = ec._1
//        val v = ec._2
//        val rs = reStat.value.get(ecKey)
//        (ecKey, if (rs.isEmpty) v else if (rs.get >= v) 0 else if (rs.get < v) (v - rs.get).toInt else v)
//      })
//
//      //qid,ecid
//      val _completeECs = ecsTmp.map(v => (v._2, v._1._1))
//      completeECs = completeECs.union(_completeECs).persist(StorageLevel.MEMORY_ONLY)
//      //dat: (ecid, bukCode), (dist, qid, buk, point)
//      dat = dat.map(v => (v._2._2, v)).subtractByKey(_completeECs).map(_._2).persist(StorageLevel.MEMORY_ONLY)
//    }
//    //    println("completeECs count= " + completeECs.count)
//    completeECs
//
//  }
//}