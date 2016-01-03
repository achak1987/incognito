package uis.cipsi.incognito.anonymization.dichotomize

import uis.cipsi.incognito.rdd.BucketSizes
import breeze.linalg.Vector
import org.apache.commons.math3.ml.distance._
import org.apache.spark.rdd.RDD
import uis.cipsi.incognito.rdd.ECKey
import uis.cipsi.incognito.utils.Utils
import uis.cipsi.incognito.rdd.ECS
import org.apache.spark.storage.StorageLevel
import org.apache.spark.Accumulable
import scala.util.control.Breaks
import uis.cipsi.incognito.rdd.ECsAccum

class TDichotomize(t: Double) {

  val mybreaks = new Breaks
  import mybreaks.{ break, breakable }

  def div(x: RDD[(ECKey, (Array[Int], Array[Int], Array[Double]))], phiProbHist: Array[Double], t: Double, U: Double, acCp: Accumulable[Array[ECS], ECS]) = {
    val rtn = x.filter({
      y =>
        var valid = true
        val ecKey = y._1
        val buks = y._2._1
        val sizes = Vector(y._2._2)
        val uBounds = Vector(y._2._3)
        val lhs = sizes.map(s => math.floor(s / 2).toInt)
        val rhs = sizes - lhs
        val lhsCount = lhs.sum * 1.0
        val rhsCount = rhs.sum * 1.0

        if (lhsCount > 0.0 && rhsCount > 0.0) {

          val lhsProbs = (lhs.map(_.toDouble) / lhsCount) //.map(v => math.round(v * 100) / 100 )
          val rhsProbs = (rhs.map(_.toDouble) / rhsCount) //.map(v => math.round(v * 100) / 100 )

          val emd = new EarthMoversDistance
          val D1 = emd.compute(lhsProbs.toArray, phiProbHist)
          val D2 = emd.compute(rhsProbs.toArray, phiProbHist)

//          println("d1= " + D1 + ", d2= " + D2 + " + u= " + U + " > " + t)
          //          val lFalse = (new Utils()).round(D1 + U, 2) > (new Utils()).round(t, 2)
          //          val rFalse = (new Utils()).round(D2 + U, 2) > (new Utils()).round(t, 2)
          //          
          val lFalse = (new Utils()).round(D1, 2) > (new Utils()).round(t, 2)
          val rFalse = (new Utils()).round(D2, 2) > (new Utils()).round(t, 2)

          if (lFalse || rFalse) {
            valid = false
          }
        } else
          valid = false

        if (!valid) {
          val bukWSize = buks.map({ var i = (-1); v => i += 1; (v, sizes(i)) }).toMap
          acCp += new ECS(ecKey, bukWSize)
        }

        valid
    })
      .map({ y =>
        val ecKey = y._1
        val buks = y._2._1
        val sizes = Vector(y._2._2)
        val uBounds = Vector(y._2._3)
        val lhs = sizes.map(s => math.floor(s / 2).toInt)
        val rhs = sizes - lhs

        val nECKeyLHS = new ECKey(ecKey.level + 1, (ecKey.sideParent + ecKey.side).hashCode().toString, '0')
        val nECKeyRHS = new ECKey(ecKey.level + 1, (ecKey.sideParent + ecKey.side).hashCode().toString, '1')
        val out = {
          val l = Array((nECKeyLHS, (buks, lhs.toArray, uBounds.toArray)))
          val r = Array((nECKeyRHS, (buks, rhs.toArray, uBounds.toArray)))
          l ++ r
        }
        out
      })
    rtn.flatMap(f => f)
  }

  def getECSizes(bucketsSizes: RDD[BucketSizes]) = {
    val acCp = bucketsSizes.sparkContext.accumulable((new ECsAccum)
      .zero(Array(new ECS(new ECKey(-1, "-1", '0'), Map(-1 -> -1)))))(new ECsAccum)

    val phiCount = bucketsSizes.map(v => (-1, v.size)).combineByKey((x: Int) => x,
      (acc: Int, x: Int) => (acc + x),
      (acc1: Int, acc2: Int) => (acc1 + acc2)).map(_._2).reduce(_ + _)

    val phiProbHist = bucketsSizes.map(v => 1.0 * v.size / phiCount).collect()

    val U = bucketsSizes.map(v => (-1, v.uBound)).combineByKey((x: Double) => x,
      (acc: Double, x: Double) => (x),
      (acc1: Double, acc2: Double) => (acc2)).map(_._2).min
    val x = bucketsSizes.map(b => (b.ecKey, b)).groupByKey()
      .mapValues({ b =>
        val bA = b
        val buks = b.map(_.bucketCode).toArray
        val sizes = b.map(_.size).toArray
        val uBounds = b.map(_.uBound).toArray
        (buks, sizes, uBounds)
      })
    val y = div(x, phiProbHist, t, U, acCp)
    var aIN = y.persist(StorageLevel.MEMORY_ONLY)

    breakable {
      while (true) {
        val y = div(aIN, phiProbHist, t, U, acCp).persist(StorageLevel.MEMORY_ONLY)
        if (y.isEmpty()) break
        aIN = y.persist(StorageLevel.MEMORY_ONLY)

      }
    }

    acCp.value.distinct.filter(_.ecKey.level != -1)
  }
}