package incognito.archive

import org.apache.spark.AccumulableParam
import incognito.rdd.SATuple

/*
 * Allows a mutable HashMap[ECKey, Int] to be used as an accumulator in Spark.
 * Whenever we try to put (k, v2) into an accumulator that already contains (k, v1), the result
 * will be a HashMap containing (k, v1 + v2).
 *
 * Would have been nice to extend GrowableAccumulableParam instead of redefining everything, but it's
 * private to the spark package.
 */
class BucketsAccum extends AccumulableParam[Array[SATuple], SATuple] {

  def addAccumulator(acc: Array[SATuple], elem: SATuple): Array[SATuple] = {
    val out = acc :+ elem
    out
  }

  /*
   * This method is allowed to modify and return the first value for efficiency.
   *
   * @see org.apache.spark.GrowableAccumulableParam.addInPlace(r1: R, r2: R): R
   */
  def addInPlace(acc1: Array[SATuple], acc2: Array[SATuple]): Array[SATuple] = {
    val out = acc1 ++ acc2
//    acc1
    out
  }

  /*
   * @see org.apache.spark.GrowableAccumulableParam.zero(initialValue: R): R
   */
  def zero(init: Array[SATuple]): Array[SATuple] = {   
    init
  }
}