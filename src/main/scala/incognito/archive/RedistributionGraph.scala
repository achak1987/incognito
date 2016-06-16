package incognito.anonymization.redistribution;
//package uis.cipsi.incognito.anonymization.redistribution
//import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.rdd.RDD
//import org.apache.spark.mllib.linalg.Vector
//import org.apache.spark.mllib.linalg.Vectors
//import uis.cipsi.incognito.rdd.Data
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.graphx._
//class RedistributionGraph {
//  def start(_tupleBucketGroup: RDD[(Int, Data)], _seeds: Broadcast[Array[(Int, (Vector, Array[String]))]],
//    ecKeyBukCodeSizes: Broadcast[Map[(Int, Int), Int]], rddCount: Long) = {
//    val sc = _tupleBucketGroup.sparkContext
//    val isACenter = true
//    //ecid, vector
//    //the ecid size is reversed so that they to makee them unique while unioning
//    val seedsRDD = sc.parallelize(_seeds.value.map(v => (-1 * v._1.toLong, v._2._1)))
//    
//    val tupleRDD = _tupleBucketGroup.map({v => 
//      val bukCode = v._1.toLong
//      val point = v._2.qisNumeric
//      (bukCode, point)})
//
//    val nodes: RDD[(VertexId, Vector)] = seedsRDD.union(tupleRDD).persist(StorageLevel.MEMORY_ONLY)
//
//    val edges:RDD[Edge[(Int, Double)]] = tupleRDD.map {
//      val seeds = _seeds.value
//      val ecSizes = ecKeyBukCodeSizes.value
//      point =>
//        val out = seeds.map({ seed =>
//          val s = seed._2._1
//          val eid = seed._1.toLong
//          val p = point._2
//          val bcode = point._1
//          val size = ecSizes((eid.toInt, bcode.toInt))
//          Edge(-1 * eid, bcode, (size, Vectors.sqdist(s, p)))
//        })
//        out
//    }.flatMap(f => f)
//    
//    val graph = Graph(nodes, edges)
//    
//    println(graph.numEdges)
//    println(graph.numVertices)
//    graph.triplets.sortBy(_.attr._2, ascending=false).map(triplet =>
//    "There were " + triplet.attr.toString + " flights from " + triplet.srcAttr + " to " + triplet.dstAttr + ".")
//    .take(10)
//    .foreach(println)
//
//    System.exit(1)
//    
//    sc.parallelize( Array((1,1)) )
//  }
//}