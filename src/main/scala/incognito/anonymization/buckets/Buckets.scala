package incognito.anonymization.buckets

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.Accumulable
import incognito.rdd.SATuple
import scala.collection.Map
import incognito.rdd.Data

/**
 * @author Antorweep Chakravorty
 * @constructor a constructor to create buckets
 * @param _taxonomy contains a map of child -> parent relationship of sensitive attribute values
 * @param rddCount total number of records in the dataset
 * @param threshold value for maximum allowed probability change for sensitive attribute values appearing in any equivalence class
 */

abstract class Buckets(_taxonomy: Broadcast[Map[String, String]] = null, rddCount: Double, beta: Double = 0.0) extends Serializable {
	/**
	 * A method to create the buckets from sensitive attribute RDD
	 * @param data the data set to be anonymized 
	 * @param height the current height of the taxonomy tree at which the partition phase runs
	 * @return returns an rdd of sensitive attribute values with a bucket code representing the bucket they belong
	 */
	def getBuckets(data: RDD[Data], height: Int = -1): RDD[SATuple]
}