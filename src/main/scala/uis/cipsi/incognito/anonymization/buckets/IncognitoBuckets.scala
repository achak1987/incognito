

package uis.cipsi.incognito.anonymization.buckets

import scala.collection.mutable.ArrayBuffer
import scala.collection.Map
import breeze.linalg.Vector
import uis.cipsi.incognito.rdd.SATuple
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import uis.cipsi.incognito.rdd.SATuple
import uis.cipsi.incognito.rdd.BucketsAccum
import uis.cipsi.incognito.rdd.SATuple
import uis.cipsi.incognito.rdd.BucketsAccum
import org.apache.spark.Accumulable

class IncognitoBuckets(_taxonomy: Broadcast[Map[String, String]], rddCount: Double, beta: Double) extends Serializable {
  val taxonomy = _taxonomy.value.map(t => (t._1.hashCode(), t._2.hashCode()))

  def partition(SAs: RDD[SATuple], acCp: Accumulable[Array[SATuple], SATuple]): RDD[SATuple] = {
    val SAsSorted = SAs.map({ dat =>
      //Gets the parent node
      var key = taxonomy(dat.bucketCode)
      (key, (dat.tuple, dat.freq, dat.freq.toDouble / rddCount, dat.uBound, key))
    })
      /*All nodes under the same immediate parent are grouped together
      Each group is defined by a parent SA and contains child SAs, frequencies, probabilities and upperbounds.
      The group is sorted by the upper bound of the SA in them */
      .groupByKey
println("===")
    val buckets =
      //Itterates over each parent SA that contains multiple child SAs
      SAsSorted.mapValues({ dat =>
        println("....")
        //We store all the created buckets in a group based on the generated intermediate buckets
        val tBuckets = new ArrayBuffer[SATuple]
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

        var delta = 0

        val parentSA = childTuples.map(_._5).max        

        //We iterate through each of child SA under the given group
        childTuples
          .sortBy(_._4)
          .map { child =>
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
              //freq of SAs that can be still added to the bucket without taking it beyond it uBound
              delta = math.floor(minUpperBound * rddCount).toInt - saBucketFreqs

              //we measure the prob of this additional freq 
              val deltaProb = delta / rddCount

              //If adding a new SA to the bucket doesnot decreasing the upperBound of the current bucket
              //              if (deltaProb * (1 - math.log(deltaProb)) < minUpperBound)
              if ((1 + math.min(beta, -1.0 * math.log(deltaProb))) * deltaProb < minUpperBound)
                delta = 0
              //If additional freq can be added 
              
              if (delta > 0) {
                //We create an overlapping bucket, by adding a part of a node to the current intermediate bucket. 
                //The remaining freq. of the node will be used later to create its own intermediate bucket
                saBucket = saBucket ++ SA.toArray
                //We increments the freq. of the intermediate bucket with the additional no. of freq. added to it
                saBucketFreqs = saBucketFreqs + delta
              }
              
              
              //We store the intermediate overlapping buckets created for the current group
              val tuple = SATuple(parentSA, saBucket.toArray,
                saBucketFreqs, minUpperBound)

              
              if (saBucketFreqs == (minUpperBound * rddCount).toInt) {
                acCp += tuple
              } else {
                tBuckets += tuple
              }

              //We clear the tmp. intermediate bucket, to start with a new intermediate bucket for this group
              saBucket.clear
              saBucketFreqs = 0
              probSum = 0.0
              minUpperBound = Double.MaxValue
            }

            //If the new to be added child does not bring the overall probability of the bucket beyond its uBound
            //It might be that, part of the current node was already added to an earlier intermediate bucket
            //            println("freq=" + freq + " - delta=" + delta + "==" + (freq - delta))
            val freqN = freq - delta
            val probN = freqN / rddCount
            saBucket = saBucket ++ SA.toArray
            saBucketFreqs = saBucketFreqs + freqN
            if (probSum == 0.0) { probSum += probN }

            //If this new prob is less than the uBound
            //            if (probN * (1 - math.log(probN)) < minUpperBound) {
            if ((1 + math.min(beta, -1.0 * math.log(probN))) * probN < minUpperBound) {
              //We reassign uBound
              minUpperBound = (1 + math.min(beta, -1.0 * math.log(probN))) * probN
            }

            delta = 0
          }
        //We store the remaining intermediate overlapping buckets created for the current group
        val tuple = SATuple(parentSA, saBucket.toArray,
          saBucketFreqs, minUpperBound)          
        if (saBucketFreqs == (minUpperBound * rddCount).toInt) {          
          acCp += tuple
        } else {
          tBuckets += tuple
        }

        //All incomplete buckets for the current group is returned
        tBuckets.toArray
      }).map(_._2)
    //We flatted the Array, by its parent

    buckets.flatMap(tuple => tuple)
  }

  def getBuckets(SAs: RDD[SATuple], height: Int = -1) = {
    val acCp = SAs.sparkContext.accumulable((new BucketsAccum)
      .zero(Array(new SATuple(-1, Array(-1), -1, 0.0))))(new BucketsAccum)

    var phi = partition(SAs, acCp)
    //We navigate through all the heights of the SA value taxonomy tree
    var itrheight = height
    //We start from the leaf nodes    
    phi.isEmpty()
    while (itrheight > 0) {
      //The temporary buckets through which still additional freq. can be added
      //Get all the temporary buckets            
      val incomplete = phi
      println("ic " + incomplete.count() + "," + acCp.value.length)
      phi = partition(incomplete, acCp)
      phi.isEmpty()
      itrheight -= 1
    }
    phi.isEmpty()
    //
    //    phi
    //      .filter { dat => dat.freq < (dat.uBound * rddCount).toInt }
    //      .foreach { tuple => acCp += tuple }    

    val out = acCp.value.distinct.filter(_.bucketCode != -1)

    out
      .map(v => (v.tuple, v.freq, v.uBound))
      .zipWithIndex
      .map({ case (k, v) => new SATuple(v.toInt, k._1, k._2, k._3) })
  }
}