package uis.cipsi.incognito.anonymization.dichotomize

import uis.cipsi.incognito.rdd.BucketSizes
import breeze.linalg.Vector
import org.apache.spark.rdd.RDD
import uis.cipsi.incognito.rdd.ECKey
import uis.cipsi.incognito.utils.Utils
import uis.cipsi.incognito.rdd.ECS
import org.apache.spark.storage.StorageLevel
import org.apache.spark.Accumulable
import scala.util.control.Breaks
import uis.cipsi.incognito.rdd.ECsAccum
import java.util.Arrays
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import uis.cipsi.incognito.utils.EarthMoversDistance
//import org.apache.commons.math3.ml.distance.EarthMoversDistance
import org.apache.spark.broadcast.Broadcast

class TDichotomize(t: Double) extends Serializable {

  def div(_x: RDD[(ECKey, (Array[String], Array[Int], Array[Double], Array[Double]))]): RDD[(Boolean, (ECKey, String, Int, Double, Double))] = {

    //    val numPartitions = _x.sparkContext.getConf.get("spark.default.parallelism").toInt

//    val x = _x.map(b => (b.ecKey, b)).groupByKey()
//      .mapValues({ b =>
//        val bA = b
//        val buks = b.map(_.bucketCode).toArray
//        val sizes = b.map(_.size).toArray
//        val uBounds = b.map(_.uBound).toArray
//        (buks, sizes, uBounds)
//      }) //.repartition(numPartitions)

    val out = _x.map({
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
          
          val phiProbHist = y._2._4

          val emd = new EarthMoversDistance
          val D1 = emd.compute(lhsProbs.toArray, phiProbHist)
          val D2 = emd.compute(rhsProbs.toArray, phiProbHist)
     
//          println(sizes.sum + "," +sizes)
//          println(lhs.sum + "," +lhs)
//          println(rhs.sum + "," +rhs)
//          println(y._2._4.sum + "," +y._2._4.toSeq)
//          
           val U = uBounds.sum
//          println(lhsProbs)          
//          println(rhsProbs)
//          println(lhsProbs + rhsProbs)
//          System.exit(1)

          //Kullback–Leibler divergence
          //          val D1 = phiProbHist.map({ var i = (-1); v => i += 1; v * math.log(v / lhsProbs(i)) }).sum
          //          val D2 = phiProbHist.map({ var i = (-1); v => i += 1; v * math.log(v / rhsProbs(i)) }).sum

//                              println(U +","+D1 +"," + (D1 < t) + "," + D2 +"," +(D2 < t) + "," + t)

          if ((U + D1 > t) || (U + D2 > t)) {
            valid = false
          }
        } else
          valid = false

        val out = ({
          if (!valid) {
            val p = buks.map({ var i = (-1); v => i += 1; (ecKey, v, sizes(i), uBounds(i), y._2._4(i)) }).map(v => (valid, v))
            p
          } else {
            val nECKeyLHS = new ECKey(ecKey.level + 1, (ecKey.sideParent + ecKey.side).hashCode().toString, '0')
            val nECKeyRHS = new ECKey(ecKey.level + 1, (ecKey.sideParent + ecKey.side).hashCode().toString, '1')
            val l = buks.map({ var i = (-1); v => i += 1; (nECKeyLHS, v, lhs(i), uBounds(i), y._2._4(i)) })
            val r = buks.map({ var i = (-1); v => i += 1; (nECKeyRHS, v, rhs(i), uBounds(i), y._2._4(i)) })
            val c = (Array(l) ++ Array(r)).flatMap(f => f)
            val out = c.map(v => (valid, v))
            out
          }
        })
        out
    }).flatMap(f => f)
    out
  }

  def getECSizes(bucketsSizes: RDD[BucketSizes]): RDD[ECS] = {

    val nBucketsSizes = bucketsSizes.map({ b => (b.ecKey, b) }).groupByKey().mapValues({ b =>
      val bA = b
      val buks = b.map(_.bucketCode).toArray
      val sizes = b.map(_.size).toArray
      val uBounds = b.map(_.uBound).toArray
      val size = sizes.sum
      val pHist = sizes.map(_.toDouble / size)
      (buks, sizes, uBounds, pHist)
    })

    var y = div(nBucketsSizes )
    var leafs = y.filter(!_._1).map(_._2)

    while (!y.isEmpty()) {
      val nLeafs = y.filter(_._1).map(v => (v._2._1, v._2)).groupByKey()
      .mapValues({ b =>
      val buks = b.map(_._2).toArray
      val sizes = b.map(_._3).toArray
      val uBounds = b.map(_._4).toArray
      val pHist = b.map(_._5).toArray
      (buks, sizes, uBounds, pHist)
      })
      y = div(nLeafs)
      leafs = leafs.union(y.filter(!_._1).map(_._2))
      nLeafs.unpersist(false)
    }

    val out = leafs.map(v => (v._1, (v._2, v._3)))
      .groupByKey
      .mapValues(f => (f.toMap))
      .map(v => new ECS(v._1, v._2))

    out
  }
}