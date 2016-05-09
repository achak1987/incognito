

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
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import uis.cipsi.incognito.anonymization.redistribution.RedistributeFinalNew1
import uis.cipsi.incognito.anonymization.redistribution.RedistributeFinalNew1
import uis.cipsi.incognito.anonymization.recording.Recording

/**
 * @author antorweepchakravorty
 *
 */

class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[SATuple])
    kryo.register(classOf[Data])
    kryo.register(classOf[ECKey])
    kryo.register(classOf[BucketSizes])
    kryo.register(classOf[ECS])
    kryo.register(classOf[BetaBuckets])
    kryo.register(classOf[IncognitoBuckets])
    kryo.register(classOf[TCloseBuckets])
    kryo.register(classOf[Dichotomize])
    kryo.register(classOf[TDichotomize])
    kryo.register(classOf[KMedoids])
    kryo.register(classOf[RedistributeFinalNew1])
  }
}

object CustomSparkContext {
  def create(sparkMaster: String, numPartitions: String = "10"): SparkContext = {

    // Make sure to set these properties *before* creating a SparkContext!
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "uis.cipsi.incognito.rdd.MyRegistrator")

    //creating spark context
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Incognito")

        sparkConf.setMaster(sparkMaster)
//        sparkConf.set("spark.cores.max", "80")
//        sparkConf.set("spark.executor.memory", "25g")
        
        sparkConf.set("spark.default.parallelism", numPartitions)

//    sparkConf.registerKryoClasses(Array(
//      classOf[SATuple], classOf[Data], classOf[ECKey], classOf[BucketSizes],
//      classOf[BetaBuckets], classOf[IncognitoBuckets], classOf[TCloseBuckets],
//      classOf[Dichotomize], classOf[TDichotomize],
//      classOf[KMedoids], classOf[RedistributeFinalNew1], classOf[Recording]))

    //    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")    

    if (!SparkContext.jarOfClass(this.getClass).isEmpty) {
      //If we run from eclipse, this statement doesnt work!! Therefore the else part
      sparkConf.setJars(SparkContext.jarOfClass(this.getClass).toSeq)
    } else {
      val jar = Jar
      val classPath = this.getClass.getResource("/" + this.getClass.getName.replace('.', '/') + ".class").toString()
      println(classPath)
      val sourceDir = classPath.substring("file:".length, classPath.indexOf("/bin") + "/bin".length).toString()

      jar.create(File("/tmp/Incognito-1.0.jar"), Directory(sourceDir), "Incognito")
      sparkConf.setJars("/tmp/Incognito-1.0.jar" :: Nil)
    }
    val sc = new SparkContext(sparkConf)
    sc
  }

}