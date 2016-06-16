package incognito.anonymization.buckets

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import incognito.rdd.SATuple
import scala.collection.mutable.ArrayBuffer
import scala.collection.Map
import incognito.rdd.SATuple
import incognito.rdd.SATuple
import org.apache.spark.SparkContext
import incognito.rdd.SATuple
import incognito.archive.BucketsAccum
import org.apache.spark.Accumulable
import incognito.rdd.SATuple
import org.apache.spark.Accumulator
import incognito.utils.Utils
import java.util.Arrays
import incognito.rdd.Data

/**
 * @author Antorweep Chakravorty
 * @constructor a constructor to create buckets for the TCloseness Algorithm
 * @param _taxonomy contains a map of child -> parent relationship of sensitive attribute values
 * @param rddCount total number of records in the dataset
 * @param beta threshold value for maximum allowed probability change for sensitive attribute values appearing in any equivalance class
 */

class TCloseBuckets(_taxonomy: Broadcast[Map[String, String]], rddCount: Double, t: Double)
    extends Buckets(_taxonomy, rddCount, t) {

  //Taxonomy tree of the sensitive attribute
  val taxonomy = _taxonomy.value

  def cet(sum: Double, min: Double, cHeight: Int, tHeight: Int): Double = {
    1.0 * cHeight / tHeight * (sum - min)
  }

  def getParent(node: String, level: Int): String = {
    var p = taxonomy(node)
    var _l = 1
    while (_l < level) {
      _l += 1
      p = taxonomy(p)
    }
    p
  }

  def getChilds(parent: String, level: Int): Array[String] = {
    var l = level - 1
    var _childs = taxonomy.filter(_._2 == parent).map(_._1).toArray.distinct
    while (l >= 0) {
      _childs = taxonomy.filter(v => !_childs.contains(v._2)).map(_._1).toArray.distinct
      l = l - 1
    }
    _childs
  }

  /**
   * A method to partition the sensitive attribute values into buckets
   */

  def partition(probNode: RDD[(String, Double)],
                t: Double, level: Int, height: Int,
                acCp: Accumulable[Array[SATuple], SATuple],
                acRootU: Accumulator[Double]) = {

    val probs = probNode
      .map(v => ((getParent(v._1, level), getParent(v._1, level - 1)), (Array(v._1), v._2)))
      .combineByKey((x: (Array[String], Double)) => (x._1, (x._2, x._2)),
        (x: (Array[String], (Double, Double)), y: (Array[String], Double)) => {
          val min = if (x._2._2 < y._2) x._2._2 else y._2
          val sum = x._2._1 + y._2
          val childKeys = x._1 ++ y._1
          (childKeys, (sum, min))
        },
        (x: (Array[String], (Double, Double)), y: (Array[String], (Double, Double))) => {
          val min = if (x._2._2 < y._2._2) x._2._2 else y._2._2
          val sum = x._2._1 + y._2._1
          val childKeys = x._1 ++ y._1
          (childKeys, (sum, min))
        })
      .map({ v =>
        val parent = v._1
        val childs = v._2._1
        val sum = v._2._2._1
        val min = v._2._2._2
        val u = cet(sum, min, level, height)
        (parent._1, (u, (childs, sum)))
      })
      .groupByKey
      .map({ v =>
        val parent = v._1
        val values = v._2.toArray
        val U = values.map(_._1).sum

        if (level == height)
          acRootU += U

        val childs = values.map(_._2._1).flatMap(f => f)
        val sum = values.map(_._2._2).sum
        val valid = U >= t
        if (!valid) {
          val tuple = new SATuple(parent, childs, math.round(sum * rddCount).toInt, U)
          acCp += tuple
        }
        childs.map(child => (valid, (child, U)))
      })

    probs.flatMap(f => f)
  }

  override def getBuckets(data: RDD[Data], height: Int = -1): RDD[SATuple] = {
    //Creates a sensitive attribute histogram
    val saHist = data.map(t => (t.sa, 1)).reduceByKey(_ + _)
      .map({ t =>
        val p = t._2.toDouble / rddCount;
        new SATuple(t._1, Array(t._1), t._2,
          (1 + math.min(this.t, -1.0 * math.log(p))) * p)
      })

    val acCp = saHist.sparkContext.accumulable((new BucketsAccum)
      .zero(Array(new SATuple(Short.MinValue.toString, Array(Short.MinValue.toString), 0, 0.0))))(new BucketsAccum)
    val acRootU = saHist.sparkContext.accumulator(0.0)

    val SAsSorted = saHist.map({ dat =>
      //Gets the parent node
      var key = -1
      (key, (dat.sas, dat.freq, 1.0 * dat.freq / rddCount, dat.uBound))
    }).map(s => s._2._1.map(v => (v, s._2._2)).toArray).flatMap(f => f)

    var firstItr = true

    var level = height
    var probNode = SAsSorted.map(s => (s._1, s._2.toDouble / rddCount.toDouble))

    val phi = partition(probNode, t, level, height, acCp, acRootU)

    var incompletePhi = phi.filter(_._1).map(_._2)
      .join(SAsSorted).map(v => (v._1, v._2._2.toDouble / rddCount.toDouble))

    while (!incompletePhi.isEmpty()) {
      level = level - 1
      val phi = partition(incompletePhi, t, level, height, acCp, acRootU)
      incompletePhi = phi.filter(_._1).map(_._2)
        .join(SAsSorted)
        .map(v => (v._1, v._2._2.toDouble / rddCount.toDouble))
    }
    incompletePhi.isEmpty()

    val out = acCp.value
      .filter(_.bucketCode != Short.MinValue)
      //Accumulators may create duplicates, here we remove the duplicate entries before returning
      .groupBy({ v => val utils = new Utils(); val x = utils.hashId(v.sas.map(utils.hashId).sorted.mkString); x })
      .map(v => v._2.head).toArray
      .map(v => (v.sas, v.freq, v.uBound))
      .zipWithIndex
      .map({ case (k, v) => new SATuple(v.toString, k._1, k._2, k._3) })
      //Remove, if any dummy buckets
      .filter(_.freq > 0)

    saHist.sparkContext.parallelize(out)
  }
}
