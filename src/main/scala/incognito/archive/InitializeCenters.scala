package incognito.anonymization.redistribution;
//package uis.cipsi.incognito.anonymization.redistribution
//
//import uis.cipsi.incognito.informationLoss.InformationLoss
//import scala.collection.mutable.ArrayBuffer
//import uis.cipsi.incognito.rdd.Data
//import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.rdd.RDD
//import scala.collection.Map
//import org.apache.spark.storage.StorageLevel
//import java.util.{ Random => JavaRandom }
//import scala.util.hashing.MurmurHash3
//import java.nio.ByteBuffer
//
//class InitializeCenters(k: Int,
//    runs: Int,
//    initializationSteps: Int,
//    seed: Long,
//    _taxonomy: Broadcast[Map[String, String]],
//    _categoricalQIHeights: Broadcast[Map[Int, Int]],
//    pidindex: Int) extends Serializable {
//
//  /**
//   * Initialize `runs` sets of cluster centers using the k-means|| algorithm by Bahmani et al.
//   * (Bahmani et al., Scalable K-Means++, VLDB 2012). This is a variant of k-means++ that tries
//   * to find with dissimilar cluster centers by starting with a random center and then doing
//   * passes where more centers are chosen with probability proportional to their squared distance
//   * to the current cluster set. It results in a provable approximation to an optimal clustering.
//   *
//   * The original paper can be found at http://theory.stanford.edu/~sergei/papers/vldb12-kmpar.pdf.
//   */
//  def initKMeansParallel(data: RDD[Data]): Array[Array[Data]] = {
//    // Initialize empty centers and point costs.
//    val centers = Array.tabulate(runs)(r => ArrayBuffer.empty[Data])
//    var costs = data.map(_ => Array.fill(runs)(Double.PositiveInfinity))
//
//    // Initialize each run's first center to a random point.
//    val seed = new XORShiftRandom(this.seed).nextInt()
//    val sample = data.takeSample(true, runs, seed).toSeq
//    val newCenters = Array.tabulate(runs)(r => ArrayBuffer(sample(r)))
//
//    /** Merges new centers to centers. */
//    def mergeNewCenters(): Unit = {
//      var r = 0
//      while (r < runs) {
//        centers(r) ++= newCenters(r)
//        newCenters(r).clear()
//        r += 1
//      }
//    }
//
//    // On each step, sample 2 * k points on average for each run with probability proportional
//    // to their squared distance from that run's centers. Note that only distances between points
//    // and new centers are computed in each iteration.
//    var step = 0
//    while (step < initializationSteps) {
//      val bcNewCenters = data.context.broadcast(newCenters)
//      val preCosts = costs
//      costs = data.zip(preCosts).map {
//        case (point, cost) =>
//          Array.tabulate(runs) { r =>
//            math.min(KMeans.pointCost(bcNewCenters.value(r), point, _taxonomy, _categoricalQIHeights, pidindex), cost(r))
//          }
//      }.persist(StorageLevel.MEMORY_ONLY)
//      val sumCosts = costs
//        .aggregate(new Array[Double](runs))(
//          seqOp = (s, v) => {
//            // s += v
//            var r = 0
//            while (r < runs) {
//              s(r) += v(r)
//              r += 1
//            }
//            s
//          },
//          combOp = (s0, s1) => {
//            // s0 += s1
//            var r = 0
//            while (r < runs) {
//              s0(r) += s1(r)
//              r += 1
//            }
//            s0
//          })
//      preCosts.unpersist(blocking = false)
//      val chosen = data.zip(costs).mapPartitionsWithIndex { (index, pointsWithCosts) =>
//        val rand = new XORShiftRandom(seed ^ (step << 16) ^ index)
//        pointsWithCosts.flatMap {
//          case (p, c) =>
//            val rs = (0 until runs).filter { r =>
//              rand.nextDouble() < 2.0 * c(r) * k / sumCosts(r)
//            }
//            if (rs.length > 0) Some(p, rs) else None
//        }
//      }.collect()
//      mergeNewCenters()
//      chosen.foreach {
//        case (p, rs) =>
//          rs.foreach(newCenters(_) += p)
//      }
//      step += 1
//    }
//
//    mergeNewCenters()
//    costs.unpersist(blocking = false)
//
//    // Finally, we might have a set of more than k candidate centers for each run; weigh each
//    // candidate by the number of points in the dataset mapping to it and run a local k-means++
//    // on the weighted centers to pick just k of them
//    val bcCenters = data.context.broadcast(centers)
//    val weightMap = data.flatMap { p =>
//      Iterator.tabulate(runs) { r =>
//        ((r, KMeans.findClosest(bcCenters.value(r), p, _taxonomy, _categoricalQIHeights, pidindex)._1), 1.0)
//      }
//    }.reduceByKey(_ + _).collectAsMap()
//    val finalCenters = (0 until runs).par.map { r =>
//      val myCenters = centers(r).toArray
//      val myWeights = (0 until myCenters.length).map(i => weightMap.getOrElse((r, i), 0.0)).toArray
//      val initialCentroidWiIndex = myCenters.zipWithIndex
//      val centroids = (new InformationLoss).getCenter(initialCentroidWiIndex, pidindex, k, _taxonomy, _categoricalQIHeights)
//      centroids
//      //      LocalKMeans.kMeansPlusPlus(r, myCenters, myWeights, k, 30)
//    }
//
//    finalCenters.toArray
//  }
//
//  object KMeans {
//    /**
//     * Returns the index of the closest center to the given point, as well as the squared distance.
//     */
//    def findClosest(
//      centers: TraversableOnce[Data],
//      point: Data,
//      _taxonomy: Broadcast[Map[String, String]],
//      _categoricalQIHeights: Broadcast[Map[Int, Int]],
//      pidIndex: Int): (Int, Double) = {
//      var bestDistance = Double.PositiveInfinity
//      var bestIndex = 0
//      var i = 0
//      centers.foreach { center =>
//        // Since `\|a - b\| \geq |\|a\| - \|b\||`, we can use this lower bound to avoid unnecessary
//        // distance computation.
//        var lowerBoundOfSqDist = ((new InformationLoss).distance(point.qisNumeric, center.qisNumeric, pidIndex)
//          + (new InformationLoss).distance(point.qisCategorical, center.qisCategorical, _taxonomy, _categoricalQIHeights))
//        if (lowerBoundOfSqDist < bestDistance) {
//          val distance: Double = (new InformationLoss).distance(point.qisNumeric, center.qisNumeric, pidIndex)
//          +(new InformationLoss).distance(point.qisCategorical, center.qisCategorical, _taxonomy, _categoricalQIHeights)
//          if (distance < bestDistance) {
//            bestDistance = distance
//            bestIndex = i
//          }
//        }
//        i += 1
//      }
//      (bestIndex, bestDistance)
//    }
//
//    /**
//     * Returns the K-means cost of a given point against the given cluster centers.
//     */
//    def pointCost(
//      centers: TraversableOnce[Data],
//      point: Data,
//      _taxonomy: Broadcast[Map[String, String]],
//      _categoricalQIHeights: Broadcast[Map[Int, Int]],
//      pidIndex: Int): Double =
//      findClosest(centers, point, _taxonomy, _categoricalQIHeights, pidIndex)._2
//  }
//}