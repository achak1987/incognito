package incognito.anonymization.redistribution;
//package uis.cipsi.incognito.anonymization.redistribution
//
//import breeze.linalg.{Vector, DenseVector, squaredDistance}
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.SparkContext._
//import org.apache.spark.rdd.RDD
//
//class SparkKMeans1()  {
//
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
//    def localKMeans(data: RDD[Vector[Double]], K: Int, convergeDist: Double = 0.001): Array[(Int, Vector[Double])] = {
//
// 
//    val kPoints = data.takeSample(withReplacement = true, K, 42).toArray
//    var tempDist = 1.0
//
//    while(tempDist > convergeDist) {
//      val closest = data.map (p => (closestPoint(p, kPoints), (p, 1)))
//
//      val pointStats = closest.reduceByKey{case ((p1, c1), (p2, c2)) => (p1 + p2, c1 + c2)}
//
//      val newPoints = pointStats.map {pair =>
//        (pair._1, pair._2._1 * (1.0 / pair._2._2))}.collectAsMap()
//
//      tempDist = 0.0
//      for (i <- 0 until K) {
//        tempDist += squaredDistance(kPoints(i), newPoints(i))
//      }
//
//      for (newP <- newPoints) {
//        kPoints(newP._1) = newP._2
//      }
//    }
//
//    kPoints.zipWithIndex.map({case (k,v) => (v,k)})
//  }
//}
