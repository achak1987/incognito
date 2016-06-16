package incognito.anonymization.redistribution;
//package uis.cipsi.incognito.anonymization.redistribution
//
//import breeze.linalg.{ Vector, DenseVector, squaredDistance }
//import org.apache.spark.{ SparkConf, SparkContext }
//import org.apache.spark.SparkContext._
//import org.apache.spark.rdd.RDD
//
///**
// * K-means clustering.
// *
// * This is an example implementation for learning how to use Spark. For more conventional use,
// * please refer to org.apache.spark.mllib.clustering.KMeans
// */
//class SparkKMeans() {
//
//  def closestPoint(p: Vector[Double], centers: Array[Vector[Double]]): Int = {
//    var bestIndex = 0
//    var closest = Double.PositiveInfinity
//
//    for (i <- 0 until centers.length) {
//      val tempDist = squaredDistance(p, centers(i))
//      if (tempDist < closest) {
//        closest = tempDist
//        bestIndex = i
//      }
//    }
//
//    bestIndex
//  }
//
//  def localKMeans(data: Array[Vector[Double]], K: Int, convergeDist: Double = 0.001): Array[(Int, Vector[Double])] = {
//
//    val r = scala.util.Random
//    val kPoints = r.shuffle(data.toList).take(K).toArray
//    val closest = data.map(p => (closestPoint(p, kPoints), (p, 1)))
//
//    val pointStats = closest
//      .groupBy(v => v._1).map(v => (v._1, v._2.map(pc => (pc._2)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))))
//
//    val newPoints = pointStats.map { pair =>
//      (pair._1, pair._2._1 * (1.0 / pair._2._2), pair._2._2)
//    }.toArray.sortBy(_._3).reverse.take(K).map(v => (v._1, v._2))
//
//    newPoints
//
//  }
//}