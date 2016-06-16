package incognito.archive

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import incognito.rdd.Data
import scala.collection.Map
import java.nio.ByteBuffer
import java.util.{ Random => JavaRandom }
import scala.util.hashing.MurmurHash3
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.linalg.Vectors
import incognito.rdd.ECS
import breeze.linalg.DenseVector
import java.util.{Random => JavaRandom}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

class KMedoids(data: RDD[Data], _taxonomy: Broadcast[Map[String, String]],
  _categoricalQIHeights: Broadcast[Map[Int, Int]], pidIndex: Int, k: Int, rddCount: Long)
   {
  def initialize(bucketsWSAs: Map[Int, Array[Int]], ecSizesIn: Array[ECS], numIterations: Int = 1, runs: Int = 1) = {
    // Cluster the data into two classes using KMeans
    val dat = data.persist(StorageLevel.MEMORY_ONLY)
    val parsedData = dat.map(s => Vectors.dense(s.qNumeric.toArray))
    val numClusters = k
    val seed = 123456789L
    val ecSizesSorted = ecSizesIn.map(v => (v.ecKey, v.bucketCodeWithSize.values.sum)).sortBy(_._2).reverse
    val model = org.apache.spark.mllib.clustering.KMeans.train(parsedData, numClusters, numIterations, runs, org.apache.spark.mllib.clustering.KMeans.K_MEANS_PARALLEL, seed)
    val cluster = model.predict(parsedData).persist(StorageLevel.MEMORY_ONLY)

    val nData = dat.map(s => s.qNumeric).zip(cluster).map({ case (k, v) => (v, (k, 1)) })
      .reduceByKey((x, y) => (Vectors.dense(
        (new DenseVector(x._1.toArray) + new DenseVector(y._1.toArray)).toArray), x._2 + y._2))

    val centers = nData.map(v => (v._1, (Vectors.dense(v._2._1.toArray.map(_ / v._2._2)), Array("")))).collect()

    val clustersBySize = nData.map(v => (v._1, v._2._2)).collect
    if (clustersBySize.size != ecSizesSorted.size) {
      println("Issue in KMedoids center generation (clustersBySize.size= " + clustersBySize.size + ") and (ecSizesSorted.size= " + ecSizesSorted.size + ")")
      System.exit(1)
    }

    val ecs = clustersBySize.map({ var i = (-1); v => i += 1; val ec = ecSizesSorted(i); ((v._1, ec._1), ec._2) }).toMap

    (centers, ecs)
  }

  def initializeRandom(bucketsWSAs: Map[Int, Array[Int]], ecSizesIn: Array[ECS], numIterations: Int = 1, runs: Int = 1) = {
    // Cluster the data into two classes using KMeans
    val dat = data.persist(StorageLevel.MEMORY_ONLY)
    val parsedData = dat.map(s => Vectors.dense(s.qNumeric.toArray))
    val numClusters = k
    val seed = 123456789L
    val ecSizesSorted = ecSizesIn.map(v => (v.ecKey, v.bucketCodeWithSize.values.sum)).sortBy(_._2).reverse
    val model = org.apache.spark.mllib.clustering.KMeans.train(parsedData, numClusters, numIterations, runs, org.apache.spark.mllib.clustering.KMeans.RANDOM, seed)

    val finalCentroids = model.clusterCenters.zipWithIndex.map({ case (k, v) => (v, Vectors.dense(k.toArray)) })
    //    val parsedData1 = finalCentroids.map(s => Vector(s.toArray))
    //    val cluster = (new SparkKMeans).localKMeans(parsedData1, K = k, convergeDist = 0.0001)
    //    val clustersBySize = cluster.map(v => (v._1, 1)).groupBy(_._1).toArray.map(v => (v._1, v._2.map(_._2).sum)).sortBy(_._2).reverse
    val clustersBySize = finalCentroids
    if (clustersBySize.size != ecSizesSorted.size) {
      println("Issue in KMedoids center generation (clustersBySize.size= " + clustersBySize.size + ") and (ecSizesSorted.size= " + ecSizesSorted.size + ")")
      System.exit(1)
    }

    val ecs = clustersBySize
      .map({ var i = (-1); v => i += 1; val ec = ecSizesSorted(i); ((v._1, ec._1), ec._2) }).toMap

    val centers = clustersBySize.map(v => (v._1, (v._2, Array(""))))
    dat.unpersist(false)
    (centers, ecs)
  }

  //  def initializeCustomNew(sampleSize: Int = 1, ecSizesIn: Array[ECS], initializationSteps: Int) = {
  //    val ecSizesSorted = ecSizesIn.map(v => (v.ecKey, v.bucketCodeWithSize.values.sum)).sortBy(_._2).reverse
  //    val dat = data.persist(StorageLevel.MEMORY_AND_DISK).map { x => x.qisNumeric }
  //    val cluster = (new SparkKMeans1).localKMeans(dat, K = k, convergeDist = 0.01)
  //    val clustersBySize = cluster.map(v => (v._1, 1)).groupBy(_._1).toArray.map(v => (v._1, v._2.map(_._2).sum)).sortBy(_._2).reverse
  //    if (clustersBySize.size != ecSizesSorted.size) {
  //      println("Issue in KMedoids center generation (clustersBySize.size= " + clustersBySize.size + ") and (ecSizesSorted.size= " + ecSizesSorted.size + ")")
  //      System.exit(1)
  //    }
  //
  //    val ecs = clustersBySize.map({ var i = (-1); v => i += 1; val ec = ecSizesSorted(i); ((v._1, ec._1), ec._2) }).toMap
  //
  //    val centers = cluster.map(v => (v._1, (v._2, Vector(""))))
  //
  //    (centers, ecs)
  //  }
  //
  //  def initializeCustom(sampleSize: Int = 1, ecSizesIn: Array[ECS], initializationSteps: Int) = {
  //    val dat = data.persist(StorageLevel.MEMORY_ONLY)
  //    val n = rddCount //data.count
  //    val fraction = math.max(1.0, sampleSize / n.toDouble)
  //    val sc = data.sparkContext
  //    val _initialCentroid = sc.broadcast(data.sample(false, fraction).take(sampleSize)) //We take the first record
  //
  //    val centroidAcc = sc.accumulable(Array(new Data(Int.MinValue, Vector(-1.0), Vector("-1.0"), Int.MaxValue)))(CentroidsAccum)
  //    _initialCentroid.value.foreach(c => centroidAcc += c)
  //
  //    val _cost = sc.accumulator(0.0)
  //
  //    var c = dat
  //      .map({
  //        val initialCentroid = _initialCentroid.value
  //        x =>
  //          val point = x.qisNumeric
  //          val _point = org.apache.spark.mllib.linalg.Vectors.dense(point.toArray)
  //          val point_norm = org.apache.spark.mllib.linalg.Vectors.norm(_point, 2.0)
  //          val il = new InformationLoss
  //          val dist = initialCentroid.map({ ic =>
  //            val center = ic.qisNumeric
  //            val __center = org.apache.spark.mllib.linalg.Vectors.dense(center.toArray)
  //            val center_norm = org.apache.spark.mllib.linalg.Vectors.norm(__center, 2.0)
  //            val dist = il.fastSquaredDistance(__center, center_norm, _point, point_norm)
  //            //            il.distance(x.qisNumeric, ic.qisNumeric, pidIndex) +
  //            //              il.distance(x.qisCategorical, ic.qisCategorical, _taxonomy, _categoricalQIHeights)
  //            dist
  //          }).min
  //          (x, dist)
  //      }).persist(StorageLevel.MEMORY_ONLY)
  //
  //    //    _initialCentroid.unpersist(blocking = false)
  //    c.foreach(d => _cost += d._2)
  //    var cost = _cost.value
  //    _cost.value_=(0.0)
  //
  //    val costLog = math.log(cost).toInt
  //    var i = 0
  //    //    println("costLog= " + costLog)
  //    //    val maxItr = 1
  //    val seed = 123456789L
  //
  //    //    while (i < costLog) {
  //    while (i < initializationSteps) {
  //
  //      i += 1
  //      val rand = new XORShiftRandom(seed)
  //      c.filter({ v =>
  //        val dist = v._2
  //        rand.nextDouble <= 2.0 * k * dist / cost
  //      }).foreach({ c => centroidAcc += c._1 })
  //
  //      //      println(">" + centroidAcc.value.length + ", cost= " + cost)
  //      c = dat.map({
  //        val initialCentroid = centroidAcc.value.filter(_.qiID != Int.MinValue)
  //
  //        x =>
  //          val point = x.qisNumeric
  //          val _point = org.apache.spark.mllib.linalg.Vectors.dense(point.toArray)
  //          val point_norm = org.apache.spark.mllib.linalg.Vectors.norm(_point, 2.0)
  //          val il = new InformationLoss
  //          val dist = initialCentroid.map({ ic =>
  //            val center = ic.qisNumeric
  //            val __center = org.apache.spark.mllib.linalg.Vectors.dense(center.toArray)
  //            val center_norm = org.apache.spark.mllib.linalg.Vectors.norm(__center, 2.0)
  //            val dist = il.fastSquaredDistance(__center, center_norm, _point, point_norm)
  //            //            il.distance(x.qisNumeric, ic.qisNumeric, pidIndex) +
  //            //              il.distance(x.qisCategorical, ic.qisCategorical, _taxonomy, _categoricalQIHeights)
  //            dist
  //          }).min
  //          (x, dist)
  //      }).persist(StorageLevel.MEMORY_ONLY)
  //
  //      _cost.value_=(0.0)
  //      c.foreach(d => _cost += d._2)
  //      cost = _cost.value
  //    }
  //
  //    val ecSizesSorted = ecSizesIn.map(v => (v.ecKey, v.bucketCodeWithSize.values.sum)).sortBy(_._2).reverse
  //    val finalCentroids = centroidAcc.value.filter(_.qiID != Int.MinValue)
  //    //    println("finalCentroids= " + finalCentroids.length)
  //    val parsedData = finalCentroids.map(s => s.qisNumeric)
  //    val cluster = (new SparkKMeans).localKMeans(parsedData, K = k, convergeDist = 0.0001)
  //    val clustersBySize = cluster.map(v => (v._1, 1)).groupBy(_._1).toArray.map(v => (v._1, v._2.map(_._2).sum)).sortBy(_._2).reverse
  //    if (clustersBySize.size != ecSizesSorted.size) {
  //      println("Issue in KMedoids center generation (clustersBySize.size= " + clustersBySize.size + ") and (ecSizesSorted.size= " + ecSizesSorted.size + ")")
  //      System.exit(1)
  //    }
  //
  //    val ecs = clustersBySize.map({ var i = (-1); v => i += 1; val ec = ecSizesSorted(i); ((v._1, ec._1), ec._2) }).toMap
  //
  //    val centers = cluster.map(v => (v._1, (v._2, Vector(""))))
  //
  //    (centers, ecs)
  //  }
  //
  //  def findClosest(
  //    centers: TraversableOnce[VectorWithNorm],
  //    point: VectorWithNorm): (Int, Double) = {
  //    var bestDistance = Double.PositiveInfinity
  //    var bestIndex = 0
  //    var i = 0
  //    val il = new InformationLoss
  //    centers.foreach { center =>
  //      // Since `\|a - b\| \geq |\|a\| - \|b\||`, we can use this lower bound to avoid unnecessary
  //      // distance computation.
  //      var lowerBoundOfSqDist = center.norm - point.norm
  //      lowerBoundOfSqDist = lowerBoundOfSqDist * lowerBoundOfSqDist
  //      if (lowerBoundOfSqDist < bestDistance) {
  //        val distance: Double = il.fastSquaredDistance(center.vector, center.norm, point.vector, point.norm)
  //        if (distance < bestDistance) {
  //          bestDistance = distance
  //          bestIndex = i
  //        }
  //      }
  //      i += 1
  //    }
  //    (bestIndex, bestDistance)
  //  }
}

class VectorWithNorm(val vector: org.apache.spark.mllib.linalg.Vector, val norm: Double) {

  def this(vector: org.apache.spark.mllib.linalg.Vector) = this(vector, Vectors.norm(vector, 2.0))

  def this(array: Array[Double]) = this(Vectors.dense(array))

  /** Converts the vector to a dense vector. */
  def toDense: VectorWithNorm = new VectorWithNorm(Vectors.dense(vector.toArray), norm)
}

class XORShiftRandom(init: Long) extends JavaRandom(init) {

  private var seed = XORShiftRandom.hashSeed(init)

  // we need to just override next - this will be called by nextInt, nextDouble,
  // nextGaussian, nextLong, etc.
  override protected def next(bits: Int): Int = {
    var nextSeed = seed ^ (seed << 21)
    nextSeed ^= (nextSeed >>> 35)
    nextSeed ^= (nextSeed << 4)
    seed = nextSeed
    (nextSeed & ((1L << bits) - 1)).asInstanceOf[Int]
  }

  override def setSeed(s: Long) {
    seed = XORShiftRandom.hashSeed(s)
  }

  object XORShiftRandom {
    /** Hash seeds to have 0/1 bits throughout. */
    def hashSeed(seed: Long): Long = {
      val bytes = ByteBuffer.allocate(java.lang.Long.SIZE).putLong(seed).array()
      val lowBits = MurmurHash3.bytesHash(bytes)
      val highBits = MurmurHash3.bytesHash(bytes, lowBits)
      (highBits.toLong << 32) | (lowBits.toLong & 0xFFFFFFFFL)
    }
  }
}