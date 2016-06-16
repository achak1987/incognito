package incognito.utils

import org.apache.commons.math3.ml.distance.DistanceMeasure
import org.apache.commons.math3.util.FastMath

class EarthMoversDistance extends DistanceMeasure {
  
    /** Serializable version identifier. */
    val serialVersionUID = -5406732779747414922L;
    
        /** {@inheritDoc} */
    def compute(a: Array[Double], b: Array[Double]): Double =   {
        if (a.length != b.length)  {
          println("Mis-matched array distribution sizes")
          System.exit(1)
        }
        var lastDistance: Double = 0.0;
        var totalDistance: Double = 0.0;
        for (i <- 0 until a.length) {
            val currentDistance = (a(i) + lastDistance) - b(i);
            totalDistance += FastMath.abs(currentDistance);
            lastDistance = currentDistance;
        }
        return totalDistance;
    }
}
