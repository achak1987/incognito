package uis.cipsi.incognito.experiments

import org.apache.log4j.Logger
import org.apache.log4j.Level
import uis.cipsi.incognito.rdd.CustomSparkContext
import uis.cipsi.incognito.rdd.ECKey
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.stat.{ MultivariateStatisticalSummary, Statistics }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.AccumulatorParam
import uis.cipsi.incognito.rdd.Data


object QualityNew {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  def main(args: Array[String]): Unit = {
    val sparkMaster = args(0)
    val filePathIn = args(1)

    val sc = CustomSparkContext.create(sparkMaster)

    val data = sc.objectFile[(ECKey, Data)](filePathIn)

    val range = data.groupByKey
      .mapValues { x =>
        val s = x.head.qisNumeric.size
        val max = Array.fill(s) { Double.MinValue }
        val min = Array.fill(s) { Double.MaxValue }
        x.foreach { y =>
          for (i <- 0 until s) {
            if (y.qisNumeric(i) > max(i))
              max(i) = y.qisNumeric(i)

            if (y.qisNumeric(i) < min(i))
              min(i) = y.qisNumeric(i)
          }
        }
        (max, min, x.size)
      }

    val observations = range.map(x => Array(Vectors.dense(x._2._1), Vectors.dense(x._2._2))).flatMap(f => f)
    val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)
    val globalMax = summary.max.toArray
    val globalMin = summary.min.toArray
    
    val localVar = range.map({v => val diff = v._2._1.map({var i=(-1); x => i+=1; x - v._2._2(i)  }); (diff, v._2._3)  })
    val globalVar = globalMax.map({var i=(-1); v => i+=1; val diff = v - globalMin(i); diff })
    
    localVar.foreach(v => println("size= " + v._2, v._1.toSeq))
    println("globalVar")
    println(globalVar.toSeq)    
    
    val IL =  localVar.map(v => v._1.map({var i = (-1); y => i+=1; (y/globalVar(i) * v._2)  }).sum ).sum
    
    println("IL=" + IL)
  }

}