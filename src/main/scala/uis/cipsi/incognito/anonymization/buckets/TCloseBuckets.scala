package uis.cipsi.incognito.anonymization.buckets

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import uis.cipsi.incognito.rdd.SATuple
import scala.collection.mutable.ArrayBuffer
import scala.collection.Map
import uis.cipsi.incognito.rdd.SATuple
import uis.cipsi.incognito.rdd.SATuple
import org.apache.spark.SparkContext
import uis.cipsi.incognito.rdd.SATuple
import uis.cipsi.incognito.rdd.BucketsAccum
import org.apache.spark.Accumulable
import uis.cipsi.incognito.rdd.SATuple
import org.apache.spark.Accumulator
import uis.cipsi.incognito.utils.Utils
import java.util.Arrays

class TCloseBuckets(_taxonomy: Broadcast[Map[String, String]], rddCount: Double) extends Serializable {
  val taxonomy = _taxonomy.value //.map(t => (t._1.hashCode(), t._2.hashCode()))

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

  def getBuckets(SAs: RDD[SATuple], height: Int = -1, t: Double): Array[SATuple] = {

    val acCp = SAs.sparkContext.accumulable((new BucketsAccum)
      .zero(Array(new SATuple(Short.MinValue.toString, Array(Short.MinValue.toString), 0, 0.0))))(new BucketsAccum)
    val acRootU = SAs.sparkContext.accumulator(0.0)

    val SAsSorted = SAs.map({ dat =>
      //Gets the parent node
      var key = -1
      (key, (dat.tuple, dat.freq, 1.0 * dat.freq / rddCount, dat.uBound))
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

    //    val U = acRootU.value

    val out = acCp.value
      .filter(_.bucketCode != Short.MinValue)
      //Accumulators may create duplicates, here we remove the duplicate entries before returning
      //      .groupBy(v => v.bucketCode + v.tuple.sorted)
      .groupBy({v => val x = Arrays.hashCode( v.tuple.map(_.hashCode()).sorted); x})
      .map(v => v._2.head).toArray
      .map(v => (v.tuple, v.freq, v.uBound))
      .zipWithIndex
      //      .map({ case (k, v) => new SATuple(v.toInt, k._1, k._2, U) })
      .map({ case (k, v) => new SATuple(v.toString, k._1, k._2, k._3) })

    out
  }
}
