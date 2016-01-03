package uis.cipsi.incognito.rdd

import org.apache.spark.mllib.linalg.Vector
case class SATuple(bucketCode: Int, tuple: Array[Int], freq: Int, uBound: Double) {}
case class Data(qiID: Int, qisNumeric: Vector, qisCategorical: Array[String], saHash: Int) {}

case class ECKey(level: Int, sideParent: String, side: Char)
case class BucketSizes(ecKey: ECKey, bucketCode: Int, size: Int, uBound: Double) {}
case class ECS(ecKey: ECKey, bucketCodeWithSize: Map[Int, Int]) {}


