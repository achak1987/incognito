package incognito.rdd

import org.apache.spark.mllib.linalg.Vector

/**
 * @author Antorweep Chakravorty
 * @constructor a constructor to create a data structure for storing the data that has to be anonymized
 * @param id stores a unique identifier for each record in the dataset
 * @param pID an optional attribute that stores the personal identifier
 * @param qNumeric an array that stores the numeric quasi identifiers
 * @param qCategorical an array that stores the string quasi identifiers
 * @param sa stores the sensitive attribute value
 */
case class Data(id: Long, pID: String="", qNumeric: Vector, qCategorical: Array[String], sa: String) {}

/**
 * @author Antorweep Chakravorty
 * @constructor a constructor to create a data structure for storing the sensitive attribute buckets
 * @param bucketCode stores an unique identifer representing each bucket
 * @param tuple stores an array of all unique sensitive attribute values belonging to the bucket
 * @param freq stores the sum of frequencies of all sensitive attributes belonging to the bucket
 * @param uBound stores the upper bound threshold for the bucket
 */
case class SATuple(bucketCode: String, sas: Array[String], freq: Int, uBound: Double) {
  override def toString(): String = bucketCode + ",[" + sas.mkString(",") +"]," + freq +"," + uBound
}


/**
 * @author Antorweep Chakravorty
 * @constructor a constructor that generates a unique key for equivalence classes
 * @param level represents the level at which an equivalence class is created during the dichotomization process
 * @param sideParent represents the parent ECKey of the equivalence class in the dichotomization tree
 * @param side represents the left or right side of the binary tree at which the equivalence class is present during the dichotomization process
 */
case class ECKey(level: Int, sideParent: String, side: Char)

/**
 * @author Antorweep Chakravorty
 * @constructor a constructor to determine sizes and number of equivalence classes that can be created from the buckets
 * @param ecKey the key of the equivalence class to which records from buckets can be allocated during the dichotomization phase.
 * @param size determines the number of records that can be choosen for the bucket into the equivalence class
 * @param uBound the upper bound threshold
 */
case class BucketSizes(ecKey: ECKey, bucketCode: String, size: Int, uBound: Double) {}

/**
 * @author Antorweep Chakravorty
 * @constructor to store equivalence classes, the buckets from which records can be chooses to maintain the required 
 * sensitive attribute distribution and the number records that can be chooses from those buckets
 * @param ecKey unique id representing each equivalence class
 * @param bucketCodeWithSize a map of bucketCode and number of records that can be chooses from them
 */
case class ECS(ecKey: ECKey, bucketCodeWithSize: Map[String, Int]) {}


