

package uis.cipsi.incognito.anonymization.dichotomize

import org.apache.spark.rdd.RDD
import org.apache.spark._
import uis.cipsi.incognito.rdd.ECKey
import uis.cipsi.incognito.rdd.BucketSizes
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.rdd.EmptyRDD
import breeze.linalg.Vector
import uis.cipsi.incognito.rdd.BucketSizes
import uis.cipsi.incognito.utils.Utils
import uis.cipsi.incognito.rdd.BucketSizes
import uis.cipsi.incognito.rdd.BucketSizes
import uis.cipsi.incognito.rdd.BucketSizes
import uis.cipsi.incognito.rdd.ECS
import org.apache.spark.storage.StorageLevel
import uis.cipsi.incognito.rdd.ECsAccum
import uis.cipsi.incognito.rdd.ECS
import uis.cipsi.incognito.rdd.ECKey
import uis.cipsi.incognito.rdd.ECS
import scala.collection.mutable.ArrayBuffer
import uis.cipsi.incognito.rdd.ECS
import scala.util.control.Breaks

class Dichotomize() extends Serializable {
  
  val mybreaks = new Breaks
  import mybreaks.{break, breakable}

  def div(x: RDD[(ECKey, (Array[Int], Array[Int], Array[Double]))], acCp: Accumulable[Array[ECS], ECS]) = {
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

          val lhsProbs = (lhs.map(_.toDouble) / lhsCount)
          val rhsProbs = (rhs.map(_.toDouble) / rhsCount)

          val lFalse = lhsProbs.toArray.filter({
            var i = (-1); s => i += 1;
            (new Utils()).round(s, 2) > (new Utils()).round(uBounds(i), 2)
          }).length
          val rFalse = rhsProbs.toArray.filter({
            var i = (-1); s => i += 1;
            (new Utils()).round(s, 2) > (new Utils()).round(uBounds(i), 2)
          }).length

          if (lFalse > 0.0 || rFalse > 0.0) {
            valid = false
          }
        } else
          valid = false

        if (!valid) {
          val bukWSize = buks.map({ var i = (-1); v => i += 1; (v, sizes(i)) }).toMap
          acCp += new ECS(ecKey, bukWSize)
        }
        valid
    }).map({ y =>

      val ecKey = y._1
      val buks = y._2._1
      val sizes = Vector(y._2._2)
      val uBounds = Vector(y._2._3)
      val nECKeyLHS = new ECKey(ecKey.level + 1, (ecKey.sideParent + ecKey.side).hashCode().toString, '0')
      val nECKeyRHS = new ECKey(ecKey.level + 1, (ecKey.sideParent + ecKey.side).hashCode().toString, '1')
      val lhs = sizes.map(s => math.floor(s / 2).toInt)
      val rhs = sizes - lhs
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

    val vaCp = new ArrayBuffer[ECS]

    val x = bucketsSizes.map(b => (b.ecKey, b)).groupByKey()
      .mapValues({ b =>
        val bA = b
        val buks = b.map(_.bucketCode).toArray
        val sizes = b.map(_.size).toArray
        val uBounds = b.map(_.uBound).toArray
        (buks, sizes, uBounds)
      }).persist(StorageLevel.MEMORY_ONLY)

    val y = div(x, acCp)
    var aIN = y

    breakable {
      while (true) {

        val y = div(aIN, acCp)
        if (y.isEmpty()) break
        aIN = y
        
      }
    }
    

    acCp.value.distinct.filter(_.ecKey.level != -1)
  }
}