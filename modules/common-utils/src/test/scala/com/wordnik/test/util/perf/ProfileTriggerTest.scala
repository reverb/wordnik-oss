package com.wordnik.test.util.perf

import com.wordnik.util.perf._

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class ProfileTriggerTest extends FlatSpec with ShouldMatchers {
  behavior of "Profile"

  it should "test the profile trigger framework" in {
    Profile.triggers.clear

    var hasTriggerFired = false

    // create a function and have it fire on trigger completion
    val triggerFunction = new Function1[ProfileCounter, Unit] {
      def apply(counter: ProfileCounter): Unit = {
        hasTriggerFired = true
        counter
      }
    }

    Profile.triggers += triggerFunction
    Profile("testing, this should set the boolean to true", Thread.sleep(10))

    assert(hasTriggerFired == true)
  }

  it should "fire a profile trigger conditionally" in {
    Profile.triggers.clear

    var hasTriggerFired = false
    val triggerFunction = new Function1[ProfileCounter, Unit] {
      def apply(counter: ProfileCounter): Unit = {
        if (counter.key.startsWith("important")) hasTriggerFired = true
        counter
      }
    }
    Profile.triggers += triggerFunction

    Profile("unimportant metric to count", Thread.sleep(10))
    assert(hasTriggerFired == false)
    
    Profile("important metric to count", Thread.sleep(10))
    assert(hasTriggerFired == true)
  }
}