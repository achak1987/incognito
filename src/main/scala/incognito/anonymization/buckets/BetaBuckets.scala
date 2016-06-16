package incognito.anonymization.buckets

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import incognito.rdd.SATuple
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.Accumulable
import incognito.archive.BucketsAccum
import java.util.Arrays
import org.apache.spark.broadcast.Broadcast
import incognito.rdd.Data
import scala.collection.Map
import incognito.utils.Utils
/**
 * @author Antorweep Chakravorty
 * @constructor a constructor to create buckets for the beta-likeness Algorithm
 * @param rddCount total number of records in the dataset
 * @param beta threshold value for maximum allowed probability change for sensitive attribute values appearing in any equivalance class
 */

class BetaBuckets(rddCount: Double, beta: Double) extends Buckets(rddCount = rddCount, beta = beta) {

  /**
   * A method to partition the sensitive attribute values into buckets
   * @param SAs the sensitive attribute RDD, grouped into bucket
   * @param acCP stores the records in memory with the same bucket code for records belonging to the same bucket
   * @return returns a RDD of records having bucket codes representing the incomplete buckets they belong
   */
  def partition(SAs: RDD[SATuple], acCp: Accumulable[Array[SATuple], SATuple]): RDD[SATuple] = {

    val SAsSorted = SAs.map({ dat =>
      //Assign the same dummy key for all
      var key = -1
      (key, (dat.sas, dat.freq, dat.freq.toDouble / rddCount, dat.uBound))
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
        var saBucket = new ArrayBuffer[String]
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
            val tuple = SATuple("-1", saBucket.toArray, saBucketFreqs, minUpperBound)
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
        val tuple = SATuple("-1", saBucket.toArray,
          saBucketFreqs, minUpperBound)
        acCp += tuple
      })

    /*We store all the created buckets in a group based on the generated intermediate buckets
         * Since buckets are generated in one iteration, no incomplete buckets are generated
         */
    val tBuckets = new ArrayBuffer[SATuple]
    tBuckets.toArray
    //We flatted the Array, by its parent
    val out = SAsSorted.sparkContext.parallelize(tBuckets)

    out
  }

  /**
   * A method to create the buckets from sensitive attribute RDD
   * @param data the data set to be anonymized
   * @param height the current height of the taxonomy tree at which the partition phase runs
   * @return returns a rdd of sensitive attribute values with a bucket code representing the bucket they belong
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

    val itrheight = 0
    //Get all the  buckets
    val phi = partition(saHist, acCp)

    val out = acCp.value
      .filter(_.bucketCode != Short.MinValue)
      //Accumulators may create duplicates, here we remove the duplicate entries before returning
      .groupBy({ v =>
        val utils = new Utils(); val x = utils.hashId(v.sas.map(utils.hashId).sorted.mkString); x
      })
      .map(v => v._2.head).toArray
      .map(v => (v.sas, v.freq, v.uBound))
      .zipWithIndex
      .map({ case (k, v) => new SATuple(v.toString, k._1, k._2, k._3) })
      //Remove, if any dummy buckets
      .filter(_.freq > 0)
    saHist.sparkContext.parallelize(out)
  }
}