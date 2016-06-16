package incognito.anonymization.methods

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.stat.{ MultivariateStatisticalSummary, Statistics }
import org.apache.spark.mllib.linalg.Vectors
import incognito.utils.Utils

class Mondrian(_data: RDD[Array[String]], K: Int, pid: Int, sid: Int) extends Serializable {
  val data = _data.map(_.filter({ var i = (-1); v => i += 1; i != sid })).map(v => Vectors.dense(v.map(_.toDouble)))

  //pid, ECid
  var anonymizedPID = data.sparkContext.parallelize(Array((Double.MaxValue, Int.MinValue)))

  def getNormalizedData(): RDD[org.apache.spark.mllib.linalg.Vector] = {
    val summary: MultivariateStatisticalSummary = Statistics.colStats(data)
    val dataNormalized = data.map(r => Vectors.dense(r.toArray.map({
      var i = (-1); v => i += 1;
      val out = (if (i != pid && i != sid)
        (v - summary.mean(i)) / math.sqrt(summary.variance(i))
      else
        v)
      out
    })))
    dataNormalized
  }
  def partitionData(dataNormalized: RDD[org.apache.spark.mllib.linalg.Vector]): Unit = {
    val summaryNormalized: MultivariateStatisticalSummary = Statistics.colStats(dataNormalized)
    val max = summaryNormalized.max
    val min = summaryNormalized.min

    var partitionIndex = Int.MinValue
    val diff = max.toArray.foreach({
      var i = (-1);
      var maxRange = Double.MinValue
      v =>
        i += 1
        //We ignore the primary indexes
        if (i != pid) {
          val range = v - min(i)
          if (range > maxRange) {
            maxRange = range
            partitionIndex = i
          }
        }
    })

    val partitions = dataNormalized.map(v => (v(partitionIndex), v)).sortBy(_._1).zipWithIndex.map(v => (v._2, v._1._2))
    val n = partitions.count
    val medianIndex = n / 2 - 1

    val lhs = partitions.filter(_._1 < medianIndex)
    val rhs = partitions.filter(_._1 >= medianIndex)
    
    val utils = new Utils     

    if (n  <= 2 * K  ) {
      val partitionID = partitions.hashCode
//      println( (n  <= 2 * K) + "," + partitionID +",>>>>>"+ n +  "," + medianIndex + "," + lhs.count + ","+rhs.count)
      anonymizedPID = anonymizedPID.union(partitions.map(v => (v._2(pid), partitionID)))
    } else {
      partitionData(lhs.map(_._2))
      partitionData(rhs.map(_._2))
    }

  }
  def getAnonymizedData(numPartitions: Int): RDD[String] = {
    
    val ec = _data
      .map(v => (v(pid).toDouble, v)).join(anonymizedPID.filter(_._1 != Double.MinValue))
      .map(v => (v._2._2, v._2._1))
      .groupByKey(numPartitions)
      .mapValues { x =>
        val size = x.size
        val data = x.toArray.map(_.filter({ var i = (-1); v => i += 1; i != pid && i != sid }).map(_.toDouble))
          .map(v => breeze.linalg.DenseVector(v.toArray))
          .reduce(_ + _)
        val mean = Vectors.dense(data.map(_ / size).toArray)
        x.map({ v =>
          val meanWithOutIDs = mean.toArray
          (v(pid) +: meanWithOutIDs :+ v(sid)).mkString(",")
        })
      }
      .map(v => v._2.map(s => v._1 + "," + s))
      .flatMap(v => v)
    ec
  }

}