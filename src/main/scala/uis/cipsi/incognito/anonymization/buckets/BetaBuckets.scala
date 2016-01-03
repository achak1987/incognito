package uis.cipsi.incognito.anonymization.buckets

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import uis.cipsi.incognito.rdd.SATuple
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.Accumulable
import uis.cipsi.incognito.rdd.BucketsAccum

class BetaBuckets(rddCount: Double) extends Serializable {

  def partition(SAs: RDD[SATuple], rddCount: Double, acCp: Accumulable[Array[SATuple], SATuple]): Unit = {

    val SAsSorted = SAs.map({ dat =>
      //Assign the same dummy key for all
      var key = -1
      (key, (dat.tuple, dat.freq, dat.freq.toDouble / rddCount, dat.uBound))
    })
      /*All nodes under the same immediate parent are grouped together
      Each group is defined by a parent SA and contains child SAs, frequencies, probabilities and upperbounds.
      The group is sorted by the upper bound of the SA in them */
      .groupByKey

    val buckets =
      SAsSorted.foreach({ _dat =>
        val dat = _dat._2
        //For each group, we might be able to create multiple buckets
        //We store the intermediate buckets in a group
        var saBucket = new ArrayBuffer[Int]
        //Freq. of the intermediate buckets
        var saBucketFreqs = 0
        //Contains all the child SA, freq, prob, uBound under the given parent
        val childTuples = dat.toArray
        //Keeps track of the probabilities of SA values added to a bucket
        var probSum = 0.0
        //The minimum allowed upper bound for probability change in a bucket
        var minUpperBound = Double.MaxValue
        //We iterate through each of child SA under the given group
        childTuples.sortBy(_._4).map { child =>
          //The SA values in the groups is stored sperately
          val SA = child._1
          //The total frequency of the group
          var freq = child._2

          //If the upper bound of the group is smaller than the current upper bound
          //Irrespective of no. of nodes added to a bucket, 
          //its uBound always remains as that of the note with min. freq. in the bucket
          minUpperBound = if (child._4 < minUpperBound) child._4 else minUpperBound

          //For each iteration we keep track of the probability of the bucket
          //Since we move lower height to higher height to higher height, the 1st ittr. has the prob. of the leaf node
          //As we move along, the prob. of the bucket increase with more number of nodes being added to it
          probSum = probSum + child._3
          //If the new to be added child brings the overall probability of the bucket beyond its uBound
          if (probSum > minUpperBound) {
            //We store the intermediate overlapping buckets created for the current group
            val tuple = SATuple(-1, saBucket.toArray, saBucketFreqs, minUpperBound)
            acCp += tuple

            //We clear the tmp. intermediate bucket, to start with a new intermediate bucket for this group
            saBucket.clear
            saBucketFreqs = 0
            probSum = 0.0
            minUpperBound = Double.MaxValue
          }
          saBucket = saBucket ++ SA.toArray
          saBucketFreqs = saBucketFreqs + freq
          if (probSum == 0.0) probSum += child._3
        }
        //We store the remaining buckets created for the current group
        val tuple = SATuple(-1, saBucket.toArray,
          saBucketFreqs, minUpperBound)
        acCp += tuple        
      })    
  }

  def getBuckets(SAs: RDD[SATuple]): Array[SATuple] = {
    val acCp = SAs.sparkContext.accumulable((new BucketsAccum)
      .zero(Array(new SATuple(Short.MaxValue, Array(-1), -1, 0.0))))(new BucketsAccum)

    val itrheight = 0
    //Get all the  buckets
    val phi = partition(SAs, rddCount, acCp)
            
    val out = acCp.value
    .distinct.filter(_.bucketCode != Short.MaxValue)
    .map(v => (v.tuple, v.freq, v.uBound))
    .zipWithIndex
    .map({ case (k, v) => new SATuple(v.toInt, k._1, k._2, k._3) })
  
    out
  }
}