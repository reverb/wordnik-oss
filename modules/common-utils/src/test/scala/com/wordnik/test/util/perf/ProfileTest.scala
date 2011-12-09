package com.wordnik.test.util.perf

import com.wordnik.util.perf.Profile

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
}