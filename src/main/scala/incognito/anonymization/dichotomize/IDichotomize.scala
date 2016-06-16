

package incognito.anonymization.dichotomize

import org.apache.spark.rdd.RDD
import org.apache.spark._
import incognito.rdd.ECKey
import incognito.rdd.BucketSizes
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.rdd.EmptyRDD
import breeze.linalg.Vector
import incognito.rdd.BucketSizes
import incognito.utils.Utils
import incognito.rdd.BucketSizes
import incognito.rdd.BucketSizes
import incognito.rdd.BucketSizes
import incognito.rdd.ECS
import org.apache.spark.storage.StorageLevel
import incognito.archive.ECsAccum
import incognito.rdd.ECS
import incognito.rdd.ECKey
import incognito.rdd.ECS
import scala.collection.mutable.ArrayBuffer
import incognito.rdd.ECS
import scala.util.control.Breaks
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import incognito.rdd.ECS

/**
 * @author Antorweep Chakravorty
 * @constructor a constructor to dichotomize buckets into equivalence classes
 */

class IDichotomize() extends Dichotomize {
  /**
   * A iterative method to divide the bucket frequencies into two approx. equal halfs at each itteration/level
   * @param _x rdd with eckey, bucket code, frequency and upperbound.
   * Initially all buckets are grouped into once ec and as we dichotomize it, new ecs with different sizes are created
   */
  def div(_x: RDD[BucketSizes]): RDD[(Boolean, BucketSizes)] = {

    //    val numPartitions = _x.sparkContext.getConf.get("spark.default.parallelism").toInt

    /*Groups the buckets by their eckey (initial key is the same for all) and 
    generates a rdd with a arrays of all buketcodes, their sizes  and ubounds
    */
    val x = _x.map(b => (b.ecKey, b)).groupByKey()
      .mapValues({ b =>
        val bA = b
        val buks = b.map(_.bucketCode).toArray
        val sizes = b.map(_.size).toArray
        val uBounds = b.map(_.uBound).toArray
        (buks, sizes, uBounds)
      }) //.repartition(numPartitions)

    //A return variable that stores equivalence classes that are complete and cannot be further divided and others that can be
    val out = x.map({
      y =>
        //Boolean to determine whether determined equivalence classes are allowed
        var valid = true
        val ecKey = y._1
        val buks = y._2._1
        val sizes = Vector(y._2._2)
        val uBounds = Vector(y._2._3)
        /*Determines the left and right hand side frequencies of equivalence classes at each iteration
        of the binary dichotomization phase
        */
        val lhs = sizes.map(s => math.floor(s / 2).toInt)
        val rhs = sizes - lhs
        val lhsCount = lhs.sum * 1.0
        val rhsCount = rhs.sum * 1.0

        //If both of the newly determined equivalence classes have frequencies
        if (lhsCount > 0.0 && rhsCount > 0.0) {

          //Calculate the probability of the equivalence classes
          val lhsProbs = (lhs.map(_.toDouble) / lhsCount)
          val rhsProbs = (rhs.map(_.toDouble) / rhsCount)

          //Determines if the created left hand side equivalence class fulfils the proportionality requirements/upper bound contrains
          val lFalse = lhsProbs.toArray.filter({
            var i = (-1); s => i += 1;
            (new Utils()).round(s, 2) > (new Utils()).round(uBounds(i), 2)
          }).length
          //Determines if the created right hand side equivalence class fulfils the proportionality requirements/upper bound contrains
          val rFalse = rhsProbs.toArray.filter({
            var i = (-1); s => i += 1;
            (new Utils()).round(s, 2) > (new Utils()).round(uBounds(i), 2)
          }).length

          //If all frequencies chooses from the buckets into the left and right equivalence classes doesnot satisfy the conditions
          if (lFalse > 0.0 || rFalse > 0.0) {
            valid = false
          }
        } else
          valid = false

        val utils = new Utils
        val out = ({
          //If generated equivalence classes are invalid, we return their parent with a valid false condition, which states that that equivalence class cannot be further divided
          if (!valid) {
            val out = buks.map({ var i = (-1); v => i += 1; new BucketSizes(ecKey, v, sizes(i), uBounds(i)) })
              .map(v => (valid, v))
            out
          } //If generated equivalence classes are valid, we return their left and right childs with a valid true condition, which states that we can now try dichotomizing the childs
          else {
            val nECKeyLHS = new ECKey(ecKey.level + 1, utils.hashId(ecKey.sideParent + ecKey.side).toString, '0')
            val nECKeyRHS = new ECKey(ecKey.level + 1, utils.hashId(ecKey.sideParent + ecKey.side).toString, '1')
            val l = buks.map({ var i = (-1); v => i += 1; new BucketSizes(nECKeyLHS, v, lhs(i), uBounds(i)) })
            val r = buks.map({ var i = (-1); v => i += 1; new BucketSizes(nECKeyRHS, v, rhs(i), uBounds(i)) })
            val c = (Array(l) ++ Array(r)).flatMap(f => f)
            val out = c.map(v => (valid, v))
            out
          }
        })
        out
    }).flatMap(f => f)
    //Returned as Boolean, BucketSizes. The Boolean variable "true" specifying that the ECs could be checked whether they can be further divided and "false" specifying that they are complete. 
    out
  }

  /**
   * A method to create equivalence classes
   * @param bucketSizes a RDD having all buckets and their sizes with a same initial equivalence key. During the dichotomization phase, eckeys are updated and how many records from each bucket could be choosen into how many equvalence clasess is also determined
   * @return returns the equivalence classes and the buckets that create it
   */
  override def getECSizes(bucketsSizes: RDD[BucketSizes]): RDD[ECS] = {

    //All buckets have the same eckey and are tested if they can be dichotomized
    var y = div(bucketsSizes)
    //We store the equivalence classes that are complete and are not allowed to be further divided
    var leafs = y.filter(!_._1).map(_._2)

    //We iteratively call the div method on the buckets until no equivalence classes can be created
    while (!y.isEmpty()) {
      //We filter out the equivalence classes that are allowed to be further divided
      val nLeafs = y.filter(_._1).map(_._2).cache
      y = div(nLeafs)

      leafs = leafs.union(y.filter(!_._1).map(_._2))
      nLeafs.unpersist(false)
    }

    //We return the generated equivalence classes reprented by their keys and a group of buckets and the number of records that can be drawn from them respectively.
    val out = leafs.map(v => (v.ecKey, (v.bucketCode, v.size)))
      .groupByKey
      .mapValues(f => (f.toMap))
      .map(v => new ECS(v._1, v._2))

    out
  }
}