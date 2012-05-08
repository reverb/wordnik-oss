package com.wordnik.test.util.perf

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

import com.wordnik.util.perf.HealthSnapshot

/**
 * @author ayush
 * @since 5/7/12 7:07 PM
 *
 */
@RunWith(classOf[JUnitRunner])
class HealthTest extends FlatSpec with ShouldMatchers {
  behavior of "Health"

  it should "retrieve a Health Snapshot" in {
    val health = HealthSnapshot.get()
    assert(health.getMemory().percentUsed > 0.0)

  }

  it should "compute live thread count" in {
    val health = HealthSnapshot.get()

    val liveThreadCount1 = health.liveThreadCount
    new Thread(new Runnable {
      def run() { Thread.sleep(5000) }
    }).start()

    assert(HealthSnapshot.get().liveThreadCount - liveThreadCount1 == 1)
  }

  it should "compute daemon thread count" in {
    val health = HealthSnapshot.get()

    val daemonThreadCount1 = health.daemonThreadCount
    val t = new Thread(new Runnable {
      def run() { Thread.sleep(5000) }
    })
    t.setDaemon(true)
    t.start()

    assert(HealthSnapshot.get().daemonThreadCount - daemonThreadCount1 == 1)
  }


}
