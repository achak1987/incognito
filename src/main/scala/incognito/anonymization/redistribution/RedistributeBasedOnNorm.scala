
package incognito.anonymization.redistribution

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import incognito.rdd.Data
import org.apache.spark.storage.StorageLevel
import incognito.rdd.ECS
import scala.collection.mutable.ArrayBuffer
import incognito.rdd.ECKey
import incognito.rdd.ECKey
import incognito.rdd.ECKey
import incognito.rdd.ECKey
import incognito.rdd.ECKey
import incognito.rdd.SATuple
import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
/**
 * @author Antorweep Chakravorty
 * @constructor a constructor to redistribute records into generated equivalence classes based
 * on the required sensitive attribute distribution and norms of the quasi identifers
 * @param data: the data set to be redistributed into equivalence classes
 * @param bucketsIn contains the buckets, their sizes and upperbound
 * @param ecSizesIn an Rdd of equivalence classes specifying the buckets and number of records that can be choosen from them
 * @param count the total number of records in the dataset
 */
class RedistributeBasedOnNorm(data: RDD[Data], bucketsIn: RDD[SATuple], ecSizesIn: RDD[ECS], count: Long) {
  /**
   * A method to redistibute records in buckets into equivalence classes.
   * The scalability is based on number of generated buckets
   * @return returns the eckey and data, the eckey represents the equivalence class to which the record belong to
   */
  def start(): RDD[(ECKey, Data)] = {

    //Measures the average of all numeric quasi identifiers
    val _avgNumericQI = data.map(v => (-1, v.qNumeric))
      .combineByKey((x: Vector) => x,
        (acc: Vector, x: Vector) => Vectors.dense((new DenseVector(acc.toArray) + new DenseVector(x.toArray)).toArray),
        (acc1: Vector, acc2: Vector) => Vectors.dense((new DenseVector(acc1.toArray) + new DenseVector(acc2.toArray)).toArray))
      .map(_._2)
      .reduce((acc1, acc2) => Vectors.dense((new DenseVector(acc1.toArray) + new DenseVector(acc2.toArray)).toArray))

    val avgNumericQI = new DenseVector(_avgNumericQI.toArray).map(_ / count)

    //Measures the standard deviation of all numeric quasi identifiers
    val _sdQI = data.map(v => (-1, Vectors.dense(
      (new DenseVector(v.qNumeric.toArray) - avgNumericQI).map(x => math.pow(x, 2)).toArray)))
      .combineByKey((x: Vector) => x,
        (acc: Vector, x: Vector) => Vectors.dense((new DenseVector(acc.toArray) + new DenseVector(x.toArray)).toArray),
        (acc1: Vector, acc2: Vector) => Vectors.dense((new DenseVector(acc1.toArray) + new DenseVector(acc2.toArray)).toArray))
      .map(_._2)
      .reduce((acc1, acc2) => Vectors.dense((new DenseVector(acc1.toArray) + new DenseVector(acc2.toArray)).toArray))

    val sdQI = new DenseVector(_sdQI.toArray).map(_ / count)

    //Standardizes all quasi identifer value 
    val standarizedQIs = data.map({ v =>
      val zscore = Vectors.dense(((new DenseVector(v.qNumeric.toArray) - avgNumericQI) / sdQI).toArray)
      new Data(id = v.id, qNumeric = zscore, qCategorical = v.qCategorical, sa = v.sa)
    }).persist(StorageLevel.MEMORY_ONLY)

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

    /*Maps the buckets and ecs together specifying which bucket how many records could be choosen
    * (Index, (bucket code, ecKey, size))
    */
    val bukECs = ecSizesIn
      .map(v => v.bucketCodeWithSize.map(b => ((b._1, v.ecKey), b._2)).toArray)
      .flatMap(f => f) //((bucket code, ecKey), size)   
      .map({ v =>
        val out = new ArrayBuffer[(String, ECKey, Int)];
        //Select the 
        for (i <- 0 until v._2) { out += ((v._1._1, v._1._2, v._2)) }; out.toArray
      })
      .flatMap(f => f) //(bucket code, ecKey, size), size   
      .sortBy(_._1)
      .zipWithIndex
      .map({ case (k, v) => (v, k) }) //Index, (bucket code, ecKey, size)   
      .persist(StorageLevel.MEMORY_ONLY)

    //Creates a record bucket map having the sa, bucketcode
    val tupleBucketMap = bucketsIn
      .map(b => b.sas.map(t => (t, b.bucketCode))).flatMap(f => f)

    /*Each record in the dataset are mapped with their SA in the bucket.
     *Duplicate records are expected for the overlapping buckets in case of incognito, since,
     * a sa could belong to multiple buckets. 
     * bucket code, data
     */
    val _tupleBucketGroup = tupleBucketMap
      .join(standarizedQIs.map(d => (d.sa, d))).map(_._2)

    implicit val KeyOrderingBuckets = new Ordering[(Int, Double)] {
      override def compare(x: (Int, Double), y: (Int, Double)): Int = {
        var rtn = x._1.compare(y._1)
        if (rtn == 0) {
          rtn = x._2.compare(y._2)
        }

        rtn
      }
    }
    /*Creates as stored store of the datasets based their bucket and norm 
     * Index (bukCode, norm, id)
    */
    val _dataWithNorm = _tupleBucketGroup
      .map(v => (v._1, Vectors.norm(v._2.qNumeric, 2.0), v._2.id))
      //Remove duplicate records which appears in multiple buckets, based on data id
      .map(v => (v._3, v)).reduceByKey({ (x, y) => (x) }).map(_._2)
      .sortBy(v => (v._1, v._2)) //sort by bukCode, norm           
      .zipWithIndex
      .map({ case (k, v) => (v, k) })
      .persist(StorageLevel.MEMORY_ONLY)

    //Joins with the dataset to return record id and eckey
    val redistributedDataSet = _dataWithNorm
      .join(bukECs)
      .map(v => (v._2._1._3, v._2._2._2))

    redistributedDataSet
    .join(data.map(v => (v.id, v)))
    .map(_._2)    

  }
}
  

