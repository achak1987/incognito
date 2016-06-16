
package incognito.utils

import scala.util.Random
import java.io.File
import java.io.PrintWriter
import org.apache.spark.rdd.RDD
import scala.collection.Map
import incognito.rdd.Data
import breeze.linalg.Vector
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext
import incognito.archive.KMedoids
import java.math.BigDecimal
import java.math.RoundingMode
import com.google.common.hash.Hashing

class Utils extends Serializable {

  val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('1' to '9')

  val key = new ArrayBuffer[Int]
  private val n = new java.util.concurrent.atomic.AtomicLong

  def next = n.getAndIncrement()

  def uniqueHashKey(length: Int = 22): Int = {
    val newKey = (1 to length).map(x => chars(Random.nextInt(chars.length))).mkString.hashCode()
    if (!key.contains(newKey)) { key += newKey; newKey } else uniqueHashKey()
  }

  /**
   * A method to generate 64-bit identifier for a given variable using Google's Guava library
   * @param str string to hash
   */
  def hashId(obj: String): Long = {
    Hashing.md5().hashString(obj).asLong
  }


  def shortCode(): String = {
    val id = 15
    val size = (math.log10(id) + 4).toInt
    val timestamp: Long = System.currentTimeMillis
    Random.alphanumeric.take(Random.nextInt(size) + 1).mkString + timestamp.toString
  }

  def deleteLocalFile(path: String) = {
    val fileTemp = new File(path)
    if (fileTemp.exists) {
      fileTemp.delete()
    }
  }

  def writeToLocalFile(data: Array[String], path: String) = {
    val pw = new PrintWriter(new File(path))
    data.foreach(d => pw.write(d + "\n"))
    pw.close
  }

  def getCategoricalQIMedian(ec: Array[Array[String]]): Array[String] = {
    val nRows = ec.length
    val nCols = ec.head.length

    val ecT: Array[Array[String]] = new Array[Array[String]](nCols)

    for (c <- Range(0, nCols)) {
      val vals = new Array[String](nRows)
      for (r <- Range(0, nRows)) {
        vals(r) = ec(r)(c)
      }
      ecT(c) = vals
    }

    val medianQIs = ecT.map({ cols =>
      val sCols = cols.sortBy { x => x }
      sCols((nRows / 2).toInt)
    })
    medianQIs
  }

  def gcd(a: Int, b: Int): Int = {
    if (b == 0) a
    else
      gcd(b, a % b)
  }

  def gcd(input: Array[Int]): Int = {
    var result = input(0);
    for (i <- 1 until input.length)
      result = gcd(result, input(i));
    result
  }

  def round(value: Double, places: Int = 2): Double = {
    if (places < 0) throw new IllegalArgumentException();

    var bd = new BigDecimal(value);
    bd = bd.setScale(places, RoundingMode.HALF_UP);
    bd.doubleValue();
  }
}