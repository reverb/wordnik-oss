package com.wordnik.util.perf

import concurrent.{ExecutionContext, Future}
import com.yammer.metrics.Metrics
import java.util.concurrent.TimeUnit
import util.control.Exception.allCatch
import com.yammer.metrics.core.MetricName

/**
 * A utility for fine-grained code instrumentation.
 * Simply pass a function to the Profile class with a name and the Profiler will keep track of performance statistics.
 * It registers timings and has support for scala's futures
 *
 * <pre>
 * // For application level metrics
 * Profile.global("delete user")) {
 *   // do something
 * }
 *
 * // or for instance level metrics
 * val profiler = Profile[UserDao]
 * profiler("delete user") {
 *   // do something
 * }
 * </pre>
 *
 */
object Profile {

  private class Global

  /**
   * Provides a global profiler to group application level metrics together
   * @return a global profiler instance
   */
  lazy val global: Profiler = apply[Global]

  /**  A metrics collector */
  trait Profiler {
    /**
     * Profile the provided block of code
     * @param name The name for this metric
     * @param scope An optional scope for this metric
     * @param thunk The block of code to profile
     * @param executor An executor to use when the provided block returns a future
     * @tparam T The result type of the code block
     * @return The result of the code block
     */
    def apply[T](name: String, scope: String = null)(thunk: ⇒ T)(implicit executor: ExecutionContext): T

    /**
     * Profile the provided block of code
     * @param name The name for this metric
     * @param scope An optional scope for this metric
     * @param thunk The block of code to profile
     * @param executor An executor to use when the provided block returns a future
     * @tparam T The result type of the code block
     * @return The result of the code block
     */
    def profile[T](name: String, scope: String = null)(thunk: ⇒ T)(implicit executor: ExecutionContext): T =
      apply(name, scope)(thunk)
  }
  private class DefaultProfiler(group: String, `type`: String) extends Profiler {
    def apply[T](name: String, scope: String = null)(thunk: ⇒ T)(implicit executor: ExecutionContext): T = {
      val timer = Metrics.newTimer(new MetricName(group, `type`, name, scope), TimeUnit.MILLISECONDS, TimeUnit.MINUTES)
      val p = timer.time()
      var isSync = true

      allCatch.andFinally( if (isSync) p.stop() ) {
        val r = thunk
        r match {
          case f: Future[_] =>
            isSync = false
            f onComplete { case _ => p.stop() }
          case _ =>
        }
        r
      }
    }
  }

  /**
   * Create a timer configured for getting rates in milliseconds and averages in minutes
   *
   * @param mf type information manifest
   * @tparam S The class this profiler applies to
   * @return a profiler which can be used sync or async
   */
  def apply[S](implicit mf: Manifest[S]): Profiler =
    new DefaultProfiler(packageName(mf.runtimeClass), mf.runtimeClass.getSimpleName.replaceAll("\\$$", ""))

  /**
   * Create a timer configured for getting rates in milliseconds and averages in minutes
   *
   * @param `type` the subgroup of this timer
   * @param group the top level group of this timer
   * @return a Profiler which can be used sync or async
   */
  def apply(`type`: String, group: String = packageName(getClass)): Profiler = new DefaultProfiler(group, `type`)

  private[this] def packageName(cl: Class[_]) = if (cl.getPackage == null) "reverb" else cl.getPackage.getName
}