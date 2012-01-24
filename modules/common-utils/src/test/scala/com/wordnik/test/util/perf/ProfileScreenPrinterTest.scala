package com.wordnik.test.util.perf


import com.wordnik.util.perf._

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class ProfileScreenPrinterTest extends FlatSpec with ShouldMatchers {
  behavior of "ProfileScreenPrinter"

  it should "record a profile then reset" in {
    Profile("testing", Thread.sleep(100))
    assert(Profile.getCounters(None).size >= 1)
    println(ProfileScreenPrinter.toString)
  }
}