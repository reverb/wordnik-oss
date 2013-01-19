package com.wordnik.util.perf

import concurrent.{ExecutionContext, Future}
import com.yammer.metrics.Metrics
import java.util.concurrent.TimeUnit
import util.control.Exception.allCatch
import com.yammer.metrics.core.MetricName


object Profile {

  class Global

  def global: Profiler = apply[Global]

  trait Profiler {
    def apply[T](name: String, scope: String = null)(thunk: ⇒ T)(implicit executor: ExecutionContext): T
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