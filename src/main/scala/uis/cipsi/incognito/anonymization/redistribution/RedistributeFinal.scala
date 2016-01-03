
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
class RedistributeFinal(ecSizesIn: Array[ECS], numIteration: Int = 5) extends Serializable {
  def start(_tupleBucketGroup: RDD[(Int, Data)]): RDD[(Int, Int)] = {

    val ecKeysWithNewIndex = _tupleBucketGroup.sparkContext.broadcast(ecSizesIn
      .zipWithIndex
      .map({ case (k, v) => (v, k.bucketCodeWithSize) }))

    val acCenter = _tupleBucketGroup.sparkContext.accumulable((new CentroidsAccum)
      .zero(Array((-1, Double.MaxValue))))(new CentroidsAccum)

    //norm, (bukCode, qid)
    val _dataWithNorm = _tupleBucketGroup
      .map(v => (Vectors.norm(v._2.qisNumeric, 2.0), (v._1, v._2.qiID)))
      .persist(StorageLevel.MEMORY_ONLY)

    val dataWithNorm = _dataWithNorm.map(v => (v._2._1, v._1))

    //data_norm, bestCenterNorm
    val dataNormWithBestCenterNorm = dataWithNorm
      //bukCode, (bukCode, norm)
      .map(v => (v._1, (v._1, v._2)))
      .groupByKey
      .mapValues({
        val ecKeys = ecKeysWithNewIndex.value
        values =>
          val bukCode = values.map(_._1).head
          val ecForThisBucket = ecKeys.map(e => (e._2(bukCode))).filter(_ != 0).sorted.reverse
          val k = ecForThisBucket.length
          val r = new Random
          val _centers = r.shuffle(values).take(1).map(_._2)
          var centers = new ArrayBuffer[Double]
          centers += _centers.head

          var cost = 0.0
          values.foreach { d =>
            var bestDistance = Double.MaxValue
            centers.foreach {
              center =>
                val dist = math.pow(d._2 - center, 2)
                if (dist < bestDistance) { bestDistance = dist; cost += bestDistance }
            }
          }

          var i = 0
          var costLog = math.log(cost)
          while (i < numIteration && i < costLog && cost > 0.0) {
            i += 1
            val r = new XORShiftRandom(123456789)
            var _cost = 0.0
            values.foreach({
              d =>
                var bestDistance = Double.MaxValue
                centers.foreach(c => {
                  val dist = math.pow(d._2 - c, 2)
                  if (dist < bestDistance) {
                    bestDistance = dist
                  }
                })
                val p = r.nextDouble
                val valid = p < (2.0 * k * bestDistance / cost)

                if (valid) { centers += d._2; _cost += bestDistance }
            })
            cost = _cost
          }

          val _finalCenters =
            (if (centers.length < k) {
              val centers = r.shuffle(values).take(k).map(_._2)
              centers.toArray
            } else {
              centers.map({ v =>
                var bestDistance = Double.MaxValue
                var bestIndex = -1
                var bestNorm = Double.MinValue
                centers.filter(c => c != v).foreach(c => {
                  val dist = math.sqrt(math.pow(v - c, 2))
                  if (dist < bestDistance) {
                    bestNorm = c
                    bestDistance = dist
                  }
                })
                (bestNorm, 1)
              }).groupBy(_._1).toArray
                .map(v => (v._1, v._2.map(_._2).sum))
                .sortBy(_._2).reverse.take(k).map(_._1)
            })

          //At this stage we sort the centers with most points that belong to them
          val finalCenters = values.map({
            d =>
              var bestDistance = Double.MaxValue
              var bestNorm = Double.MinValue
              _finalCenters.foreach(c => {
                val dist = math.pow(d._2 - c, 2)
                if (dist < bestDistance) {
                  bestDistance = dist
                  bestNorm = c

                }
              })
              (bestNorm, 1)
          })
            .groupBy(_._1).toArray
            .map(v => (v._1, v._2.map(_._2).sum))
            .sortBy(_._2).reverse.toArray

          //(seed_norm, size)
          val seedsWithCenters = finalCenters
            .zip(ecForThisBucket)
            .map(v => (v._1._1, v._2))
            .zipWithIndex

         //(eccode, bukcode) -> size
          val addECSizes = HashMap(Int.MinValue -> Int.MinValue)
          val out = values.map({
            d =>
              var bestDistance = Double.MaxValue
              var bestCenterIndex = Int.MinValue
              seedsWithCenters.foreach(c => {
                if (addECSizes.get(c._2).isEmpty)
                  addECSizes += (c._2 -> 0)

                val dist = math.pow(d._2 - c._1._1, 2)
             
                if (dist < bestDistance) {
                  if (addECSizes(c._2) < c._1._2) {
                    bestDistance = dist
                    bestCenterIndex = c._2
                  }
                }
              })
              if(bestCenterIndex == -2147483648) {
                println(d._1, d._2, values.size)
                println(addECSizes.toSeq)
                println(ecForThisBucket.toSeq)
                println(ecForThisBucket.sum)
                println(values.toSeq)
                System.exit(1)
              }
              addECSizes(bestCenterIndex) += addECSizes(bestCenterIndex) + 1
              //bucCode + value_norm(unique), bestCenterNorm
              (d._1 + d._2, bestCenterIndex)
          })
//          addECSizes.foreach(println)
          out
      }).map(_._2).flatMap(f => f)
      .filter(_._2 != Double.MinValue)
      .persist(StorageLevel.MEMORY_ONLY)

    val dataGrouped =
      //norm, (bukCode, qid)
      _dataWithNorm
        .map(v => (v._1 + v._2._1, v._2._2))
        .join(dataNormWithBestCenterNorm)
        //(qid, eid)
        .map(v => (v._2._1, v._2._2))

    /*
    //########################## START: Section to generated initial seeds in parallel from the whole dataset ################
    dataWithNorm
      .takeSample(false, num = 1, seed = 123456789)
      .foreach(c => acCenter += c)

    val bcCenters = _tupleBucketGroup.sparkContext.broadcast(acCenter.value)
    val acCost = _tupleBucketGroup.sparkContext.accumulator(0.0)
    dataWithNorm
      .map({
        val centers = bcCenters.value; v =>
          var bestDistance = Double.MaxValue
          centers.foreach({
            center =>
              val dist = math.pow(v._2 - center._2, 2)
              if (dist < bestDistance) bestDistance = dist
          })
          bestDistance
      })
      .foreach(v => acCost += v)

    var i = 0
    var costLog = math.log(acCost.value)
    while (i < numIteration && i < costLog && acCost.value > 0.0) {
      i += 1
      val cost = acCost.value
      acCost.value_=(0.0)
      val bcCenters = _tupleBucketGroup.sparkContext.broadcast(acCenter.value)

      dataWithNorm.foreach({
        val r = new XORShiftRandom(123456789)
        val centers = bcCenters.value
        d =>
          var bestDistance = Double.MaxValue
          centers.foreach(c => {
            val dist = math.pow(d._2 - c._2, 2)
            if (dist < bestDistance) {
              bestDistance = dist
            }
          })
          val p = r.nextDouble
          val valid = p < (2.0 * k * bestDistance / cost)

          if (valid) { acCenter += d; acCost += bestDistance }
      })
    }

    //You might have same key as it is the bucket code and multiple tuples belongs  to a single buckets.
    //When comparing use both the key and value (norm) 
    val centers = _tupleBucketGroup.sparkContext.parallelize(acCenter.value.distinct.filter(c => c._1 != -1))
    val bCenters = _tupleBucketGroup.sparkContext.broadcast(acCenter.value.distinct.filter(c => c._1 != -1))

    val finalCenters = centers.map({ v =>
      val cCenter = bCenters.value
      var bestDistance = Double.MaxValue
      var bestIndex = -1
      cCenter.filter(c => c._1 != v._1).foreach(c => {
        val dist = math.sqrt(math.pow(v._2 - c._2, 2))
        if (dist < bestDistance) {
          bestIndex = c._1
          bestDistance = dist
        }
      })
      (v._1, bestIndex)
    }).sortBy(_._2, false).take(k)

    println("finalCenters= " + finalCenters.length)
    finalCenters.foreach(println)
    dataWithNorm.unpersist(false)
    //########################## END: Section to generated initial seeds in parallel from the whole dataset ################
    */

    dataGrouped
  }
}

