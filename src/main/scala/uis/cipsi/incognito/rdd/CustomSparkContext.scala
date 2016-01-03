

package uis.cipsi.incognito.rdd

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.tools.nsc.io.Jar
import scala.tools.nsc.io.File
import scala.tools.nsc.io.Directory
import scala.Option.option2Iterable
import scala.reflect.io.Path.string2path
import uis.cipsi.incognito.anonymization.buckets.BetaBuckets
import uis.cipsi.incognito.anonymization.buckets.IncognitoBuckets
import uis.cipsi.incognito.anonymization.buckets.TCloseBuckets
import uis.cipsi.incognito.anonymization.dichotomize.Dichotomize
import uis.cipsi.incognito.anonymization.dichotomize.TDichotomize
import uis.cipsi.incognito.anonymization.redistribution.KMedoids
import uis.cipsi.incognito.informationLoss.InformationLoss
import uis.cipsi.incognito.anonymization.redistribution.RedistributeNew2

/**
 * @author antorweepchakravorty
 *
 */

object CustomSparkContext {
  def create(sparkMaster: String): SparkContext = {
    //creating spark context
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Incognito")
    
    sparkConf.setMaster(sparkMaster)
    sparkConf.set("spark.cores.max", "6")
    sparkConf.set("spark.executor.memory", "6gb")

    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.registerKryoClasses(Array(
        classOf[SATuple], classOf[Data], classOf[ECKey], classOf[BucketSizes],
        classOf[BetaBuckets], classOf[IncognitoBuckets], classOf[TCloseBuckets],
        classOf[Dichotomize], classOf[TDichotomize],         
        classOf[KMedoids], classOf[RedistributeNew2]       
    ))
    
    
    if (!SparkContext.jarOfClass(this.getClass).isEmpty) {
      //If we run from eclipse, this statement doesnt work!! Therefore the else part
      sparkConf.setJars(SparkContext.jarOfClass(this.getClass).toSeq)
    } else {
      val jar = Jar
      val classPath = this.getClass.getResource("/" + this.getClass.getName.replace('.', '/') + ".class").toString()
      println(classPath)
      val sourceDir = classPath.substring("file:".length, classPath.indexOf("/bin") + "/bin".length).toString()

      jar.create(File("/tmp/Incognito-1.0.jar"), Directory(sourceDir), "Incognito")
      sparkConf.setJars("/tmp/Incognito-1.0.jar":: Nil)
    }
    val sc = new SparkContext(sparkConf)
//    sc.hadoopConfiguration.set("fs.tachyon.impl", "tachyon.hadoop.TFS")
    sc
  }

}