package uis.cipsi.incognito.anonymization.methods

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.stat.{ MultivariateStatisticalSummary, Statistics }
import org.apache.spark.mllib.linalg.Vectors

class Mondrian(data: RDD[org.apache.spark.mllib.linalg.Vector], K: Int, pid: Int, sid: Int) extends Serializable {
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
        //We ignore the primary and SA indexes
        if (i != pid && i != sid) {
          val range = v - min(i)
          if (range > maxRange) {
            maxRange = range
            partitionIndex = i
          }
        }
    })

    val partitions = dataNormalized.map(v => (v(partitionIndex), v)).sortBy(_._1).zipWithIndex.map(v => (v._2, v._1._2))
    val medianIndex = partitions.count / 2
    val lhs = partitions.filter(_._1 < medianIndex)
    val rhs = partitions.filter(_._1 >= medianIndex)

    val lhsCount = lhs.count
    val rhsCount = rhs.count

    if (lhsCount <= K) {
      anonymizedPID = anonymizedPID.union(lhs.map(v => (v._2(pid), lhs.hashCode)))
    } else {
      partitionData(lhs.map(_._2))
    }

    if (rhsCount <= K) {
      anonymizedPID = anonymizedPID.union(rhs.map(v => (v._2(pid), rhs.hashCode)))
    } else {
      partitionData(rhs.map(_._2))
    }
  }
  def getAnonymizedData(numPartitions: Int): RDD[String] = {
    val ec = data
      .map(v => (v(pid), v)).join(anonymizedPID.filter(_._1 != Double.MinValue))
      .map(v => (v._2._2, v._2._1))
      .groupByKey(numPartitions)
      .mapValues { x =>
        val size = x.size
        val data =  x.toArray
        .map(v =>  breeze.linalg.DenseVector(v.toArray))
        .reduce(_+_) 
        val mean = Vectors.dense(data.map(_ / size).toArray)
        x.map({v => 
          val meanWithOutIDs = mean.toArray.filter({var i=(-1); f => i+=1; i != pid || i != sid})
          (v(pid) +: meanWithOutIDs :+ v(sid)).mkString(",")            
        })        
      }
    .map(_._2)
    .flatMap(v => v)
    ec
  }

}