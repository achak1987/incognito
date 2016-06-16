package incognito.anonymization.dichotomize

import org.apache.spark.rdd.RDD
import incognito.rdd.BucketSizes
import incognito.rdd.ECS

/**
 * @author Antorweep Chakravorty
 * @constructor a constructor to create equivalence calsses
 * @param t value for maximum allowed probability change for sensitive attribute values appearing in any equivalence class
 */

abstract class Dichotomize(t: Double=0.0) extends Serializable {
  /**
   * A method to create equivalence classes
   * @param bucketSizes a RDD having all buckets and their sizes with a same initial equivalence key. During the dichotomization phase, eckeys are updated and how many records from each bucket could be choosen into how many equvalence clasess is also determined
   * @return returns the equivalence classes and the buckets that create it
   */
  def getECSizes(bucketsSizes: RDD[BucketSizes]): RDD[ECS]
}