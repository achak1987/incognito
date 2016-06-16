

package incognito.anonymization.buckets

import scala.collection.mutable.ArrayBuffer
import scala.collection.Map
import breeze.linalg.Vector
import incognito.rdd.SATuple
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import incognito.rdd.SATuple
import incognito.archive.BucketsAccum
import incognito.rdd.SATuple
import incognito.archive.BucketsAccum
import org.apache.spark.Accumulable
import incognito.archive.XORShiftRandom
import incognito.rdd.SATuple
import java.util.Arrays
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import incognito.rdd.SATuple
import org.apache.spark.storage.StorageLevel
import incognito.rdd.Data
import incognito.utils.Utils

/**
 * @author Antorweep Chakravorty
 * @constructor a constructor to create overlapping buckets for the Incognito Algorithm
 * @param _taxonomy contains a map of child -> parent relationship of sensitive attribute values
 * @param rddCount total number of records in the dataset
 * @param beta threshold value for maximum allowed probability change for sensitive attribute values appearing in any equivalance class
 */

class IncognitoBuckets(_taxonomy: Broadcast[Map[String, String]], rddCount: Double, beta: Double)
    extends Buckets(_taxonomy, rddCount, beta) {

  /**
   * A method to partition the sensitive attribute values into buckets
   * @param SAs the sensitive attribute RDD, grouped into bucket
   * @param acCP stores the records in memory with the same bucket code for records belonging to the complete bucket
   * @return returns a RDD of records having bucket codes representing the incomplete buckets they belong
   */
  def partition(SAs: RDD[SATuple], acCp: Accumulable[Array[SATuple], SATuple]): RDD[SATuple] = {

    val SAsSorted = SAs.map({
      dat =>
        val taxonomy = _taxonomy.value
        //Gets the parent node
        var key = taxonomy(dat.bucketCode)
        (key, (dat.sas, dat.freq, dat.freq.toDouble / rddCount, dat.uBound, key))
    })
      /*All nodes under the same immediate parent are grouped together
      Each group is defined by a parent SA and contains child SAs, frequencies, probabilities and upperbounds.
      The group is sorted by the upper bound of the SA in them */
      .groupByKey

    val buckets =
      //Itterates over each parent SA that contains multiple child SAs
      SAsSorted.mapValues({ dat =>
        //We store all the created buckets in a group based on the generated intermediate buckets
        val tBuckets = new ArrayBuffer[SATuple]
        //For each group, we might be able to create multiple buckets
        //We store the intermediate buckets in a group
        var saBucket = new ArrayBuffer[String]
        //Freq. of the intermediate buckets
        var saBucketFreqs = 0
        //Contains all the child SA, freq, prob, uBound under the given parent
        val childTuples = dat
          .toArray
          .sortBy(_._4)
        //Keeps track of the probabilities of SA values added to a bucket
        var probSum = 0.0
        //The minimum allowed upper bound for probability change in a bucket
        var minUpperBound = Double.MaxValue

        var delta = 0

        val parentSA = childTuples.map(_._5).max

        //We iterate through each of child SA under the given group
        childTuples
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

              //              println(delta + "," + freq + "," + saBucketFreqs + "," + math.floor(minUpperBound * rddCount).toInt)
              //we measure the prob of this additional freq 
              val deltaProb = delta.toDouble / rddCount

              //If adding a new SA to the bucket doesnot decreasing the upperBound of the current bucket
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

              if (saBucketFreqs == math.floor(minUpperBound * rddCount).toInt) {
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
            val probN = freqN.toDouble / rddCount
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
        if (saBucketFreqs == math.floor(minUpperBound * rddCount).toInt) {
          acCp += tuple
        } else {
          tBuckets += tuple
        }

        //All incomplete buckets for the current group is returned
        tBuckets.toArray
      }).map(_._2)
    //We flatted the Array, by its parent
    val out = buckets
      .flatMap(tuple => tuple)

    out
  }

  /**
   * A method to create the buckets from sensitive attribute RDD
   * @param data the data set to be anonymized
   * @param height the current height of the taxonomy tree at which the partition phase runs
   * @return returns an rdd of sensitive attribute values with a bucket code representing the bucket they belong
   */
  override def getBuckets(data: RDD[Data], height: Int = -1): RDD[SATuple] = {
    //Creates a sensitive attribute histogram
    val saHist = data.map(t => (t.sa, 1)).reduceByKey(_ + _)
      .map({ t =>
        val p = t._2.toDouble / rddCount;
        new SATuple(t._1, Array(t._1), t._2,
          (1 + math.min(beta, -1.0 * math.log(p))) * p)
      })

    val acCp = saHist.sparkContext.accumulable((new BucketsAccum)
      .zero(Array(new SATuple(Short.MinValue.toString, Array("-1"), 0, 0.0))))(new BucketsAccum)

    var incomplete = partition(saHist, acCp).persist(StorageLevel.MEMORY_ONLY)
    //We navigate through all the heights of the SA value taxonomy tree
    var itrheight = height
    //We start from the leaf nodes    
    while (itrheight > 0) {
      itrheight -= 1
      //The temporary buckets through which still additional freq. can be added
      //Get all the temporary buckets            
      incomplete = partition(incomplete, acCp).persist(StorageLevel.MEMORY_ONLY)
    }

    //Add remaining buckets 
    incomplete.foreach(v => acCp += v)

    val out = acCp.value.filter(_.bucketCode != Short.MinValue.toString)
      //Accumulators may create duplicates, here we remove the duplicate entries before returning
      .groupBy({ v => val utils = new Utils(); val x = utils.hashId(v.sas.map(utils.hashId).sorted.mkString); x })
      .map(v => v._2.head).toArray
      .map(v => (v.sas, v.freq, v.uBound))
      .zipWithIndex
      .map({ case (k, v) => new SATuple(v.toString, k._1, k._2, k._3) })
      //Remove, if any dummy buckets
      .filter(_.freq > 0)
      
    incomplete.unpersist(false)
    saHist.sparkContext.parallelize(out)
  }
}