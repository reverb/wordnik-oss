package com.wordnik.util.perf

/**
 * Copyright 2011 Wordnik, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.lang.management.ManagementFactory
import javax.xml.bind.annotation.XmlRootElement
import reflect.BeanProperty

/**
 * @author ayush
 * @since 11/15/11 5:52 PM
 *
 */
@XmlRootElement(name = "Health")
class Health() {

  @BeanProperty var liveThreadCount: Int = _
  @BeanProperty var peakThreadCount: Int = _
  @BeanProperty var startedThreadCount: Long = _
  @BeanProperty var daemonThreadCount: Int = _
  @BeanProperty var memory: Memory = _
}

@XmlRootElement(name = "Memory")
class Memory() {

  @BeanProperty var allocated: Long = _
  @BeanProperty var max: Long = _
  @BeanProperty var free: Long = _
  @BeanProperty var used: Long = _
  @BeanProperty var percentUsed: Double = _
}

object HealthSnapshot {
  private val MB = 1048576

  def get() = {

    // compute current memory consumption
    val memory = new Memory()

    val runtime = Runtime.getRuntime
    memory.allocated = runtime.totalMemory() / MB
    memory.free = runtime.freeMemory() / MB
    memory.used = (memory.allocated - memory.free)
    memory.max = runtime.maxMemory() / MB

    val percent = ((memory.used.toDouble / memory.max.toDouble) * 100).toString
    memory.percentUsed = (if (percent.length() > 5) percent.substring(0, 5) else percent).toDouble

    val health = new Health()
    health.memory = memory

    // compute thread allocation
    val threadMxBean = ManagementFactory.getThreadMXBean
    health.liveThreadCount = threadMxBean.getThreadCount
    health.peakThreadCount = threadMxBean.getPeakThreadCount
    health.startedThreadCount = threadMxBean.getTotalStartedThreadCount
    health.daemonThreadCount = threadMxBean.getDaemonThreadCount

    health
  }
}
