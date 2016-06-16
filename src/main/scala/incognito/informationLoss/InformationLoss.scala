
package incognito.informationLoss

import breeze.linalg.Vector
import scala.collection.Map
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import incognito.rdd.Data
import com.github.fommil.netlib.{BLAS => NetlibBLAS, F2jBLAS}
import com.github.fommil.netlib.BLAS.{getInstance => NativeBLAS}
class InformationLoss {

  /**
   * Compute euclidean distance (l2 norm).
   */

  def euclideanDistance(x: Vector[Double], y: Vector[Double]) = {
    math.sqrt((x - y).map(v => math.pow(v, 2)).sum)
  }

  def distance(x: Vector[Double], y: Vector[Double], pidIndex: Int): Double = {
    //Remove the primary identifier from any distance calculations
    val out = 
      euclideanDistance(x.map({ var i = (-1); v => i += 1; if (i == pidIndex) 0.0 else v }), y.map({ var i = (-1); v => i += 1; if (i == pidIndex) 0.0 else v }))
    math.pow(out, 2)
  }

  def distance(x: Vector[Double], _y: Broadcast[Array[Vector[Double]]], pidIndex: Int): Double = {
    val y = _y.value
    //Remove the primary identifier from any distance calculations
    val out = y.map(c =>
      euclideanDistance(x.map({ var i = (-1); v => i += 1; if (i == pidIndex) 0.0 else v }), c.map({ var i = (-1); v => i += 1; if (i == pidIndex) 0.0 else v }))).min
    math.pow(out, 2)
  }
  
    def distance(x: Vector[String], y: Vector[String], _taxonomy: Broadcast[Map[String, String]],
    _categoricalQIHeights: Broadcast[Map[Int, Int]]): Double = {
        
  val taxonomy = _taxonomy.value
  val categoricalQIHeights = _categoricalQIHeights.value
    val _out =x.map({
      var i = (-1); v => i += 1;
      var commonHeight = 0.0
      var xNode = v
      var yNode = y(i)

      while (xNode != yNode) {
        xNode = taxonomy(xNode)
        yNode = taxonomy(yNode)

        commonHeight += 1
      }
      commonHeight / categoricalQIHeights(i)
    }).toArray.sum
    val out = _out
//        println("out= " + out)
    out
  }

  def distance(x: Vector[String], _y: Broadcast[Array[Vector[String]]], _taxonomy: Broadcast[Map[String, String]],
    _categoricalQIHeights: Broadcast[Map[Int, Int]]): Double = {
      
  val taxonomy = _taxonomy.value
  val categoricalQIHeights = _categoricalQIHeights.value
    val y = _y.value
    val _out = y.map(c => x.map({
      var i = (-1); v => i += 1;
      var commonHeight = 0.0
      var xNode = v
      var yNode = c(i)

      while (xNode != yNode) {
        xNode = taxonomy(xNode)
        yNode = taxonomy(yNode)

        commonHeight += 1
      }
      commonHeight / categoricalQIHeights(i)
    }).toArray).map(_.sum)
    val out = _out.min /// sum
    //    println("out= " + out)
    out
  }

//  def index(x: Data, y: Array[(Data, Int)], pidIndex: Int, _taxonomy: Broadcast[Map[String, String]],
//    _categoricalQIHeights: Broadcast[Map[Int, Int]]): Int = {
//    //Remove the primary identifier from any distance calculations
//    val out = y.map({ c =>
//      val dist =( if(!x.qisNumeric.equals(c._1.qisNumeric) && !x.qisCategorical.equals(c._1.qisCategorical) )
//        distance(x.qisNumeric, c._1.qisNumeric, pidIndex) + distance(x.qisCategorical, c._1.qisCategorical, _taxonomy, _categoricalQIHeights)
//        else Double.MaxValue )
//      (c._2, dist)
//    }).minBy(_._2)._1
//    out
//  }

//  def getCenter(nc: Array[(Data, Int)], pidIndex: Int, k: Int, _taxonomy: Broadcast[Map[String, String]],
//    _categoricalQIHeights: Broadcast[Map[Int, Int]]) = {
//    val _out = nc.map({ c => (index(c._1, nc, pidIndex, _taxonomy, _categoricalQIHeights), c._1) }).groupBy(_._1).map(v => (v._1, v._2.length)).toArray.sortBy(_._2).reverse.take(k).map(_._1)
//    val out = nc.filter(v => _out.contains(v._2)).map(_._1)
//    out    
//  }
  
  
  private lazy val EPSILON = {
    var eps = 1.0
    while ((1.0 + (eps / 2.0)) != 1.0) {
      eps /= 2.0
    }
    eps
  }
  
  def dot(x: org.apache.spark.mllib.linalg.Vector, y: org.apache.spark.mllib.linalg.Vector): Double = {
    require(x.size == y.size,
      "BLAS.dot(x: Vector, y:Vector) was given Vectors with non-matching sizes:" +
      " x.size = " + x.size + ", y.size = " + y.size)
    (x, y) match {
      case (dx: org.apache.spark.mllib.linalg.DenseVector, dy: org.apache.spark.mllib.linalg.DenseVector) =>
        dot(dx, dy)
      case (sx: org.apache.spark.mllib.linalg.SparseVector, dy: org.apache.spark.mllib.linalg.DenseVector) =>
        dot(sx, dy)
      case (dx: org.apache.spark.mllib.linalg.DenseVector, sy: org.apache.spark.mllib.linalg.SparseVector) =>
        dot(sy, dx)
      case (sx: org.apache.spark.mllib.linalg.SparseVector, sy: org.apache.spark.mllib.linalg.SparseVector) =>
        dot(sx, sy)
      case _ =>
        throw new IllegalArgumentException(s"dot doesn't support (${x.getClass}, ${y.getClass}).")
    }
  }
  
//  /**
//   * dot(x, y)
//   */
//  private def dot(x: org.apache.spark.mllib.linalg.DenseVector, y: org.apache.spark.mllib.linalg.DenseVector): Double = {
//    val n = x.size
//    F2jBLAS.ddot(n, x.values, 1, y.values, 1)
//  }

  /**
   * dot(x, y)
   */
  private def dot(x: org.apache.spark.mllib.linalg.SparseVector, y: org.apache.spark.mllib.linalg.DenseVector): Double = {
    val xValues = x.values
    val xIndices = x.indices
    val yValues = y.values
    val nnz = xIndices.size

    var sum = 0.0
    var k = 0
    while (k < nnz) {
      sum += xValues(k) * yValues(xIndices(k))
      k += 1
    }
    sum
  }

  /**
   * dot(x, y)
   */
  private def dot(x: org.apache.spark.mllib.linalg.SparseVector, y: org.apache.spark.mllib.linalg.SparseVector): Double = {
    val xValues = x.values
    val xIndices = x.indices
    val yValues = y.values
    val yIndices = y.indices
    val nnzx = xIndices.size
    val nnzy = yIndices.size

    var kx = 0
    var ky = 0
    var sum = 0.0
    // y catching x
    while (kx < nnzx && ky < nnzy) {
      val ix = xIndices(kx)
      while (ky < nnzy && yIndices(ky) < ix) {
        ky += 1
      }
      if (ky < nnzy && yIndices(ky) == ix) {
        sum += xValues(kx) * yValues(ky)
        ky += 1
      }
      kx += 1
    }
    sum
  }

  def fastSquaredDistance(
      v1: org.apache.spark.mllib.linalg.Vector,
      norm1: Double,
      v2: org.apache.spark.mllib.linalg.Vector,
      norm2: Double,
      precision: Double = 1e-6): Double = {
    val n = v1.size
    require(v2.size == n)
    require(norm1 >= 0.0 && norm2 >= 0.0)
    val sumSquaredNorm = norm1 * norm1 + norm2 * norm2
    val normDiff = norm1 - norm2
    var sqDist = 0.0
    /*
     * The relative error is
     * <pre>
     * EPSILON * ( \|a\|_2^2 + \|b\\_2^2 + 2 |a^T b|) / ( \|a - b\|_2^2 ),
     * </pre>
     * which is bounded by
     * <pre>
     * 2.0 * EPSILON * ( \|a\|_2^2 + \|b\|_2^2 ) / ( (\|a\|_2 - \|b\|_2)^2 ).
     * </pre>
     * The bound doesn't need the inner product, so we can use it as a sufficient condition to
     * check quickly whether the inner product approach is accurate.
     */
    val precisionBound1 = 2.0 * EPSILON * sumSquaredNorm / (normDiff * normDiff + EPSILON)
    if (precisionBound1 < precision) {
      sqDist = sumSquaredNorm - 2.0 * dot(v1.toSparse, v2.toSparse)
    } else if (v1.isInstanceOf[org.apache.spark.mllib.linalg.SparseVector] || v2.isInstanceOf[org.apache.spark.mllib.linalg.SparseVector]) {
      val dotValue = dot(v1.toSparse, v2.toSparse)
      sqDist = math.max(sumSquaredNorm - 2.0 * dotValue, 0.0)
      val precisionBound2 = EPSILON * (sumSquaredNorm + 2.0 * math.abs(dotValue)) /
        (sqDist + EPSILON)
      if (precisionBound2 > precision) {
        sqDist = org.apache.spark.mllib.linalg.Vectors.sqdist(v1, v2)
      }
    } else {
      sqDist = org.apache.spark.mllib.linalg.Vectors.sqdist(v1, v2)
    }
    sqDist
  }

}



