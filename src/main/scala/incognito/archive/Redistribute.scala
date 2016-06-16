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
//
//class Redistribute() extends Serializable {
//
//  def redistribute(data: RDD[((Int, Int), (Int, (Double, Int, Int)))], ecSizes: Broadcast[Map[(Int, Int), Int]]) = {
//
//    val remainingECs = ecSizes.value
//
//    println("reminaing = " + remainingECs.values.sum)
//
//    val reStat = data.sparkContext.accumulable((new RedistributionStats).zero(HashMap((0, 0) -> 0)))(new RedistributionStats)
//    //((e,b), (e, (d, q, b )))
//    val ecsTmp = data
//      .groupByKey()
//      .mapValues({
//        v =>
//          //(e, (d, q, b ))
//          val values = v.toArray
//          val ecGr = values.sortBy(_._2._1).zipWithIndex.filter(f => f._2 < remainingECs((f._1._1, f._1._2._3)))
//            //((e,b), q)
//            .map(v => ((v._1._1, v._1._2._3), v._1._2._2))
//          ecGr
//      })
//      .map(_._2)
//      .flatMap(f => f)
//    ecsTmp.foreach(v => reStat += (v._1, 1));
//
//    (ecsTmp, reStat.value)
//  }
//
//  def start(_tupleBucketGroup: RDD[(Int, Data)], seeds: Broadcast[Array[(Int, (Vector[Double], Vector[String]))]],
//    ecKeyBukCodeSizes: Broadcast[Map[(Int, Int), Int]], rddCount: Long): RDD[(Int, Int)] = {
//
//    val tupleBucketGroup = _tupleBucketGroup.persist(StorageLevel.MEMORY_ONLY)
//    var priorityEC = 0
//    //dat: (ecid, (dist, qid, buk))    
//    var _dat = tupleBucketGroup
//      .map({
//        x =>
//          val centers = seeds.value
//          //          val point_norm = org.apache.spark.mllib.linalg.Vectors.norm(point, 2.0)
//          val centerAtPrioriry = centers.map { _center =>
//            //            val il = new InformationLoss
//            val point = x._2.qisNumeric
//            val center = _center._2._1
//            //            val center_norm = org.apache.spark.mllib.linalg.Vectors.norm(center, 2.0)
//            (_center._1,
//              if (ecKeyBukCodeSizes.value((_center._1, x._1)) == 0)
//                Double.MaxValue
//              else
//                math.sqrt((point - center).map(v => math.pow(v, 2)).sum))
//            //il.fastSquaredDistance(center, center_norm, point, point_norm) * ecKeyBukCodeSizes.value.filter(_._1._1 == _center._1).values.sum  )            
//          }.sortBy(v => v._2)
//
//          val c = centerAtPrioriry(priorityEC)
//
//          //ecID, Distance, QiD, bukCode, numData
//          (c._1, (c._2, x._2.qiID, x._1), x._2.qisNumeric)
//      })
//
//    val dat = _dat.map(v => (v._1, v._2))
//
//    //((e,b), (e, (d, q, b ))) 
//    val dataAtPri = dat
//      .map(f => ((f._1, f._2._3), (f._1, f._2)))
//
//    val out = redistribute(dataAtPri, ecKeyBukCodeSizes)
//
//    val reStat = out._2
//
//    var allCompleteRe = out._1
//
//    var remainingECs = ecKeyBukCodeSizes.value.map({ ec =>
//      val ecKey = ec._1
//      val v = ec._2
//      val rs = reStat.get(ecKey)
//      (ecKey, if (rs.isEmpty) v else if (rs.get >= v) 0 else if (rs.get < v) (v - rs.get).toInt else v)
//    })
////    println("_.dat= " + dat.count)
//    while (remainingECs.values.sum > 0) {
//      val ecsAlreadyFull = _dat.sparkContext.broadcast( remainingECs.filter(_._2 == 0).keys.toArray )
//
//      priorityEC += 1
//      _dat = _dat
//        .map({
//          x =>
//            val centers = seeds.value
//            //          val point_norm = org.apache.spark.mllib.linalg.Vectors.norm(point, 2.0)
//            val centerAtPrioriry = centers.map { _center =>
//              //            val il = new InformationLoss
//              val point = x._3
//              val center = _center._2._1
//              //            val center_norm = org.apache.spark.mllib.linalg.Vectors.norm(center, 2.0)
//              (_center._1,
//                if (ecsAlreadyFull.value.contains((_center._1, x._2._3))) -1.0
//                else if (ecKeyBukCodeSizes.value((_center._1, x._2._3)) == 0)
//                  Double.MaxValue
//                else
//                  math.sqrt((point - center).map(v => math.pow(v, 2)).sum))
//              //il.fastSquaredDistance(center, center_norm, point, point_norm) * ecKeyBukCodeSizes.value.filter(_._1._1 == _center._1).values.sum  )            
//            }.sortBy(v => v._2)
//
//            val c = centerAtPrioriry(priorityEC)
//
//            //ecID, Distance, QiD, bukCode
//            (c._1, (c._2, x._2._2, x._2._3), x._3)
//        }).filter(_._2._1 != -1.0)
////      println("_.dat= " + _dat.count)
//      val dat = _dat.map(v => (v._1, v._2))
//      //((e,b), (e, (d, q, b ))) 
//      val dataAtPri = dat
//        .map(f => ((f._1, f._2._3), (f._1, f._2)))
//      val out = redistribute(dataAtPri, dat.sparkContext.broadcast(remainingECs))
//      val reStat = out._2
//      remainingECs = remainingECs.map({ ec =>
//        val ecKey = ec._1
//        val v = ec._2
//        val rs = reStat.get(ecKey)
//        (ecKey, if (rs.isEmpty) v else if (rs.get >= v) 0 else if (rs.get < v) (v - rs.get).toInt else v)
//      })
//      allCompleteRe = allCompleteRe.union(out._1)
//    }
//
//    //qid, eid
//    allCompleteRe.map({ case (k, v) => (v, k._1) })
//  }
//
//}