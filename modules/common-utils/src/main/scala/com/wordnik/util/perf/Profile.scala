package com.wordnik.util.perf

import javax.xml.bind.annotation._

import scala.reflect.BeanProperty
import scala.collection.mutable.{ HashMap, ListBuffer, HashSet }

object Profile {
  var counters = new HashMap[String, ProfileCounter]
  var triggers = new HashSet[Function1[ProfileCounter, Unit]]

  def reset = counters = new HashMap[String, ProfileCounter]
  def getCounters(filter: Option[String]): List[ProfileCounter] = {
    val counterList = new ListBuffer[ProfileCounter]
    filter match {
      case Some(filter) => for ((key, value) <- counters) if (key.startsWith(filter)) counterList += value
      case None => counterList.appendAll(counters.values)
    }
    counterList.sortWith(_.key > _.key).toList
  }

  def apply[T](name: String, f: => T, output: Boolean): T = {
    val p = Profile(name)
    try f
    finally {
      p.finish
      output match {
        case true => println(name, p.getDuration)
        case _ =>
      }
    }
  }

  def apply[T](name: String, f: => T): T = {
    val p = Profile(name)
    try f
    finally p.finish
  }

  def processTriggers(counter: ProfileCounter) = {
    triggers.foreach(t => t.apply(counter))
  }
}

@XmlRootElement
class ProfileCounter(@BeanProperty var key: String) {
  @BeanProperty var count = 0L
  @BeanProperty var totalDuration = 0L
  @BeanProperty var minDuration = Long.MaxValue
  @BeanProperty var maxDuration = 0.0
  @BeanProperty var avgDuration = 0.0

  def this() = this(null)
  def incrementHits(duration: Long) = {
    this.synchronized {
      count += 1
      totalDuration += duration
      if (duration < minDuration) minDuration = duration
      if (duration > maxDuration) maxDuration = duration
      avgDuration = totalDuration.toDouble / count.toDouble
    }
  }

  def subtract(what: ProfileCounter): ProfileCounter = {
    val sub = new ProfileCounter(key)
    sub.count = count - what.count
    sub.totalDuration = totalDuration - what.totalDuration
    sub.minDuration = {
      if (minDuration <= what.minDuration) minDuration
      else what.minDuration
    }
    sub.maxDuration = {
      if (maxDuration >= what.maxDuration) maxDuration
      else what.maxDuration
    }
    sub.avgDuration = sub.totalDuration / sub.count
    sub
  }

  override def toString = {
    val sb = new StringBuilder
    sb.append("{key : \"").append(key).append("\"")
      .append(", count : ").append(count)
      .append(", totalDuration : ").append(totalDuration)
      .append(", minDuration : ").append(minDuration)
      .append(", avgDuration : ").append(avgDuration)
      .append(", maxDuration : ").append(maxDuration)
      .append("}")
    sb.toString
  }
}

case class Profile(key: String) {
  val start = System.currentTimeMillis
  var end = 0L
  def finish = {
    end = System.currentTimeMillis
    Profile.counters.contains(key) match {
      case false => {
        val counter = new ProfileCounter(key)
        Profile.counters += key -> counter
      }
      case _ =>
    }
    val counter = Profile.counters(key)
    counter.incrementHits(getDuration)
    Profile.processTriggers(counter)
    getDuration
  }
  def getDuration: Long = {
    end match {
      case 0L => 0L
      case _ => end - start
    }
  }
}