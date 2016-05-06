package uis.cipsi.incognito.rdd

import org.apache.spark.mllib.linalg.Vector

case class SATuple(bucketCode: String, tuple: Array[String], freq: Int, uBound: Double) {}
case class Data(qiID: Int, qisNumeric: Vector, qisCategorical: Array[String], saHash: String) {}

case class ECKey(level: Int, sideParent: String, side: Char)
case class BucketSizes(ecKey: ECKey, bucketCode: String, size: Int, uBound: Double) {}
case class ECS(ecKey: ECKey, bucketCodeWithSize: Map[String, Int]) {}


