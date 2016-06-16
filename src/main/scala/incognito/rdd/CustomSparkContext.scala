

package incognito.rdd

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.tools.nsc.io.Jar
import scala.tools.nsc.io.File
import scala.tools.nsc.io.Directory
import scala.Option.option2Iterable
import scala.reflect.io.Path.string2path
import incognito.anonymization.buckets.BetaBuckets
import incognito.anonymization.buckets.IncognitoBuckets
import incognito.anonymization.buckets.TCloseBuckets
import incognito.anonymization.dichotomize.Dichotomize
import incognito.anonymization.dichotomize.TDichotomize
import incognito.archive.KMedoids
import incognito.informationLoss.InformationLoss
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import incognito.anonymization.recording.Recording

/**
 * @author Antorweep Chakravorty
 * Creates a custom spark context
 */

object CustomSparkContext {
	def create(sparkMaster: String = "local", numPartitions: String = "10"): SparkContext = {

		//creating spark context
		val sparkConf = new SparkConf()
		sparkConf.setAppName("Incognito")
		sparkConf.set("spark.default.parallelism", numPartitions)

		if (!SparkContext.jarOfClass(this.getClass).isEmpty) {
			//If we run from eclipse, this statement doesnt work!! Therefore the else part
			sparkConf.setJars(SparkContext.jarOfClass(this.getClass).toSeq)
		} else {
			//if we run from eclipse
			sparkConf.setMaster(sparkMaster)
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