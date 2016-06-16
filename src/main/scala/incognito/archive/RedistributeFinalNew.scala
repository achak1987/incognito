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
//import java.util.Arrays
//import uis.cipsi.incognito.rdd.ECKey
//class RedistributeFinalNew(ecSizesIn: Array[ECS], numIteration: Int = 5) {
//  def start(_tupleBucketGroup: RDD[(Int, Data)]): RDD[(Int, ECKey)] = {
//
//    //    val ecKeysWithNewIndex = _tupleBucketGroup.sparkContext.broadcast(ecSizesIn
//    //      .zipWithIndex
//    //      .map({ case (k, v) => (v, k.bucketCodeWithSize) }))
//    val ecKeysWithNewIndex = _tupleBucketGroup.sparkContext.broadcast(ecSizesIn
//      .map(v => (v.ecKey, v.bucketCodeWithSize)))
//
//    //norm, (bukCode, qid)
//    val _dataWithNorm = _tupleBucketGroup
//      .map(v => (Vectors.norm(v._2.qisNumeric, 2.0), (v._1, v._2.qiID)))
//
//    //data_norm, bestCenterNorm
//    val _dataNormWithBestCenterNorm = _dataWithNorm
//      //bukCode, (qid, norm)
//      .map(v => (v._2._1, (v._2._2, v._1)))
//      .groupByKey
//
//    //    val numPartitions = _dataNormWithBestCenterNorm.partitions.length
//
//    val dataNormWithBestCenterNorm = _dataNormWithBestCenterNorm
//      //      .repartition(numPartitions)
//      .map({
//        val _ecForThisBucket = ecKeysWithNewIndex.value
//        _values =>
//          val values = _values._2.toArray.sortBy(_._2)
//          val bukCode = _values._1
//          val ecForThisBucket = _ecForThisBucket.map(e => (e._1, e._2(bukCode)))
//            .filter(_._2 != 0).sortBy(_._2).reverse
//
//          val k = ecForThisBucket.length
////          val subsets = new ArrayBuffer[(Int, ECKey)]
//
//          var startIndex = 0
//          var i = -1
//          var j = 0
//          val subsets = values.map({
//            v =>
//              i += 1              
//              val ecSize = ecForThisBucket(j)
//              val endIndex = ecSize._2              
//              if (i >= startIndex + endIndex) {
//                startIndex = startIndex + endIndex
//                j += 1
//              }
//              (v._1, ecSize._1)
//          })
//
////          println("values=" + values.length)
////          for (i <- 0 until k) {
////            j += 1
////            val endIndex = ecForThisBucket(j)
////            println(startIndex + "-" + (startIndex + endIndex._2 - 1) + "=" + endIndex._2)
////            Arrays.copyOfRange(values, startIndex, startIndex + endIndex._2 - 1)
////              .foreach(v => subsets += ((v._1, endIndex._1)))
////            startIndex = startIndex + endIndex._2
////          }
////          println("subsets=" + subsets.length)
//
//          subsets
//      }).flatMap(f => f)
//
//    dataNormWithBestCenterNorm
//  }
//}
//  
//
