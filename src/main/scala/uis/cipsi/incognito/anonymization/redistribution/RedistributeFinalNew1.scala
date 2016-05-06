
package uis.cipsi.incognito.anonymization.redistribution

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import uis.cipsi.incognito.rdd.Data
import uis.cipsi.incognito.rdd.CentroidsAccum
import org.apache.spark.storage.StorageLevel
import uis.cipsi.incognito.rdd.ECS
import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import uis.cipsi.incognito.rdd.ECKey
import uis.cipsi.incognito.rdd.ECKey
import java.util.Arrays
import uis.cipsi.incognito.rdd.ECKey
import uis.cipsi.incognito.rdd.ECKey
import uis.cipsi.incognito.rdd.ECKey
import uis.cipsi.incognito.rdd.SATuple
import breeze.linalg.DenseVector
class RedistributeFinalNew1(ecSizesIn: RDD[ECS], numIteration: Int = 5) {
  def start(QIs: RDD[Data], bucketsIn: RDD[SATuple], qiCount:Long): RDD[(ECKey, Data)] = {

    val _avgNumericQI = QIs.map(v => (-1, v.qisNumeric))
      .combineByKey((x: Vector) => x,
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
    
    //SAHash, BukCode
    val tupleBucketMap = bucketsIn
      .map(b => b.tuple.map(t => (t, b.bucketCode))).flatMap(f => f)

    //there may be duplicate records spread across different buckets, since inognito uses overlapping buckets
    val _tupleBucketGroup = tupleBucketMap
      .join(standarizedQIs.map(qi => (qi.saHash, qi)))
//      .map(v => (v._2._2.qiID, v))
      //bcode, data
      .map(v => v._2).persist(StorageLevel.MEMORY_ONLY)
//      .map(v => (v._2._2)).persist(StorageLevel.MEMORY_ONLY)

//    val numPartitions = _tupleBucketGroup.partitions.length

    implicit val KeyOrdering = new Ordering[(String, ECKey, Int)] {
      override def compare(x: (String, ECKey, Int), y: (String, ECKey, Int)): Int = {
        var rtn = x._1.compare(y._1)
        if (rtn == 0) {
          rtn = x._3.compare(y._3) * -1

          if (rtn == 0) {
            rtn = x._2.level.compare(y._2.level)

            if (rtn == 0) {
              rtn = x._2.sideParent.compareTo(y._2.sideParent)

              if (rtn == 0) {
                rtn = x._2.side.compare(y._2.side)
              }
            }
          }
        }
        rtn
      }
    }

    
    //(Index, (bukCode, ecKey, size), size)
    val bukECs = ecSizesIn//_tupleBucketGroup.sparkContext.parallelize(ecSizesIn)
      .map(v => v.bucketCodeWithSize.map(b => ((b._1, v.ecKey), b._2)).toArray)
      .flatMap(f => f)//((bukCode, ecKey), size)   
      .map({ v =>
        val out = new ArrayBuffer[((String, ECKey, Int), Int)];
        for (i <- 0 until v._2) { out += (((v._1._1, v._1._2, v._2), v._2)) }; out.toArray
      })
      .flatMap(f => f)
      .sortBy(_._1)//(bukCode, ecKey, size), size   
      .zipWithIndex
      .map({ case (k, v) => (v, k) })//(Index, (bukCode, ecKey, size), size)   
      .persist(StorageLevel.MEMORY_ONLY)

    implicit val KeyOrderingBuckets = new Ordering[(Int, Double)] {
      override def compare(x: (Int, Double), y: (Int, Double)): Int = {
        var rtn = x._1.compare(y._1)
        if (rtn == 0) {
          rtn = x._2.compare(y._2)
        }

        rtn
      }
    }
    
    
    val _tupleBucketGroupNew = tupleBucketMap //SAHash, BukCode
      .join(QIs.map(qi => (qi.saHash, qi)))
      .map(_._2)//bukCode, Data
      .sortBy(_._1)
      .zipWithIndex
      .map({ case (k, v) => (v, k) })
      .persist(StorageLevel.MEMORY_ONLY)
      
//    //(bukCode, norm), qid
//    val _dataWithNormNew = _tupleBucketGroup
//      .map(v => (v._1, Vectors.norm(v._2.qisNumeric, 2.0),v._2))
//      .sortBy(_._1) //sort by bukCode
//      .zipWithIndex
//      .map({ case (k, v) => (v, k) })
//      .persist(StorageLevel.MEMORY_ONLY)

    val redistributedDataSet = _tupleBucketGroupNew
      .join(bukECs)
      .map(v => (v._2._2._1._2, v._2._1._2))

    redistributedDataSet

  }
}
  

