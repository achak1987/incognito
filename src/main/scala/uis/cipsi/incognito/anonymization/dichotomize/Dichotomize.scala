

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
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import uis.cipsi.incognito.rdd.ECS

class Dichotomize() extends Serializable {

  def div(_x: RDD[BucketSizes]): RDD[(Boolean, BucketSizes)] = {

//    val numPartitions = _x.sparkContext.getConf.get("spark.default.parallelism").toInt
    
    val x = _x.map(b => (b.ecKey, b)).groupByKey()
      .mapValues({ b =>
        val bA = b
        val buks = b.map(_.bucketCode).toArray
        val sizes = b.map(_.size).toArray
        val uBounds = b.map(_.uBound).toArray
        (buks, sizes, uBounds)
      })//.repartition(numPartitions)

    
    val out = x.map({
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

        val out = ({
          if (!valid) {
            val p = buks.map({ var i = (-1); v => i += 1; new BucketSizes(ecKey, v, sizes(i), uBounds(i)) }).map(v => (valid, v))
            p
          } else {
            val nECKeyLHS = new ECKey(ecKey.level + 1, (ecKey.sideParent + ecKey.side).hashCode().toString, '0')
            val nECKeyRHS = new ECKey(ecKey.level + 1, (ecKey.sideParent + ecKey.side).hashCode().toString, '1')
            val l = buks.map({ var i = (-1); v => i += 1; new BucketSizes(nECKeyLHS, v, lhs(i), uBounds(i)) })
            val r = buks.map({ var i = (-1); v => i += 1; new BucketSizes(nECKeyRHS, v, rhs(i), uBounds(i)) })
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

    var y = div(bucketsSizes)
    var leafs = y.filter(!_._1).map(_._2)
    
    while (!y.isEmpty()) {
      val nLeafs = y.filter(_._1).map(_._2).cache
      y = div(nLeafs)
      leafs = leafs.union(y.filter(!_._1).map(_._2))
      nLeafs.unpersist(false)      
    }
    
    
    val out = leafs.map(v => (v.ecKey, (v.bucketCode, v.size)))
                   .groupByKey
                   .mapValues(f => (f.toMap))
                   .map(v => new ECS(v._1, v._2))
        

    out
  }
}