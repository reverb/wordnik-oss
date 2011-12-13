package com.wordnik.test.util.perf

import com.wordnik.util.perf._

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class ProfileTest extends FlatSpec with ShouldMatchers {
  behavior of "Profile"

  it should "record a profile then reset" in {
    Profile("testing", Thread.sleep(100))
    assert(Profile.getCounters(None).size >= 1)
    Profile.reset
    assert(Profile.getCounters(None).size == 0)
  }

  it should "get counter size" in {
    Profile.reset
    Profile("testing", Thread.sleep(100))
    val counters = Profile.getCounters(Some("testing"))
    assert(counters.size == 1)
  }

  it should "verify counter count" in {
    Profile.reset
    Profile("testing", Thread.sleep(100))
    val counters = Profile.getCounters(Some("testing"))
    assert(counters(0).totalDuration >= 100 && counters(0).totalDuration <= 110)
  }

  it should "get counter duration" in {
    Profile.reset
    Profile("testing", Thread.sleep(100))
    val counters = Profile.getCounters(Some("testing"))
    assert(counters(0).totalDuration >= 100 && counters(0).totalDuration <= 110)
  }

  it should "add two counters" in {
    Profile.reset
    Profile("testing-1", Thread.sleep(100))
    Profile("testing-2", Thread.sleep(50))
    var counters = Profile.getCounters(Some("testing-1"))
    assert(counters.size == 1)
    assert(counters(0).totalDuration >= 100 && counters(0).totalDuration <= 110)

    counters = Profile.getCounters(Some("testing-2"))
    assert(counters.size == 1)
    assert(counters(0).totalDuration >= 50 && counters(0).totalDuration <= 60)

    counters = Profile.getCounters(None)
    assert(counters.size == 2)
  }

  it should "subtract two counters" in {
    val counter1 = new ProfileCounter("test1")
    counter1.count = 1
    counter1.totalDuration = 100
    counter1.minDuration = 10
    counter1.maxDuration = 10000.0
    counter1.avgDuration = 0.0

    val counter2 = new ProfileCounter("test1")
    counter2.count = 100

    counter2.totalDuration = 2000
    counter2.minDuration = 9
    counter2.maxDuration = 100.0
    counter2.avgDuration = 0.0

    val output = counter2.subtract(counter1)
    assert(output.count == 99)
    assert(output.minDuration == 9)
    assert(output.maxDuration == 10000.0)
  }
}