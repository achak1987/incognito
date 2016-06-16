
package incognito.anonymization.recording

import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import scala.collection.Map
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import incognito.rdd.{ ECKey, Data }
import incognito.utils.Utils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import breeze.linalg.DenseVector
import incognito.rdd.ECKey

/**
 * @author Antorweep Chakravorty
 * @constructor a constructor to record equivalence classes so that their quasi identifers are same as each other
 * @param ecs data sets with eckey defining the equivalence class they belong to
 * @param _taxonomy specifies the taxonomy tree of the string type quasi identifiers
 */
class Recording(ecs: RDD[(ECKey, Data)], _taxonomy: Broadcast[Map[String, String]]) {
  val taxonomy = _taxonomy.value
  implicit val CategoricalQIOrdering = new Ordering[Array[String]] {
    override def compare(x: Array[String], y: Array[String]): Int = {
      x.map({ var i = (-1); x => i += 1; x.compare(y(i)) }).sum
    }
  }

  def getMinMax(ec: Array[Array[String]]) = {
    val values = ec
    val min = values.min
    val max = values.max
    min.map({ var i = (-1); r => i += 1; (r + "-" + max(i)) })
  }

  def generalize(): RDD[String] = {
    //    val taxKeyHashMap = HashMap(0 -> "")
    //        taxonomy.keys.foreach({ k => val utils = new Utils; taxKeyHashMap += ((utils.hashId(k), k)) })    

    val avgECQIs = ecs.map(ec => (ec._1, (ec._2.qNumeric, 1)))
      .reduceByKey((x, y) => (Vectors.dense((new DenseVector(x._1.toArray) + new DenseVector(y._1.toArray)).toArray), x._2 + y._2))
      .map(ec => (ec._1, new DenseVector(ec._2._1.toArray).map(_ / ec._2._2)))

    //    val categoricalQI = ecs.map(ec => (ec._1, ec._2.qisCategorical))
    //      .groupByKey()
    //      .mapValues({ qis =>
    //        (new Utils()).getCategoricalQIMedian(qis.toArray)
    //        //        getMinMax(qis.map(_.toArray).toArray)
    //      })

    ecs.map(v => (v._1, (v._2.pID, v._2.qCategorical.mkString(","), v._2.sa)))
      //      .join(categoricalQI)
      .join(avgECQIs)
      //      .map(ec => (ec._2._1._2, ec._2._2, taxKeyHashMap(ec._2._1._1)))
      //      .map(v => (v._1.toArray ++ v._2.toArray).mkString(",") + "," + v._3)
      .map(v => v._2._1._1 + "," + v._2._2.toArray.mkString(",") + "," + v._2._1._2 + "," + v._2._1._3)

  }
}