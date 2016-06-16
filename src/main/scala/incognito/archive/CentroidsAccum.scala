package incognito.archive

import org.apache.spark.AccumulableParam

/*
 * Allows a mutable HashMap[ECKey, Int] to be used as an accumulator in Spark.
 * Whenever we try to put (k, v2) into an accumulator that already contains (k, v1), the result
 * will be a HashMap containing (k, v1 + v2).
 *
 * Would have been nice to extend GrowableAccumulableParam instead of redefining everything, but it's
 * private to the spark package.
 */
class CentroidsAccum extends AccumulableParam[Array[(Int, Double)], (Int, Double)] {

  def addAccumulator(acc: Array[(Int, Double)], elem: (Int, Double)): Array[(Int, Double)] = {
    val out = acc :+ elem
    out
  }

  /*
   * This method is allowed to modify and return the first value for efficiency.
   *
   * @see org.apache.spark.GrowableAccumulableParam.addInPlace(r1: R, r2: R): R
   */
  def addInPlace(acc1: Array[(Int, Double)], acc2: Array[(Int, Double)]): Array[(Int, Double)] = {
    val out = acc1 ++ acc2//.foreach(elem => addAccumulator(acc1, elem))
//    acc1
    out
  }

  /*
   * @see org.apache.spark.GrowableAccumulableParam.zero(initialValue: R): R
   */
  def zero(init: Array[(Int, Double)]): Array[(Int, Double)] = {   
    init
  }
}