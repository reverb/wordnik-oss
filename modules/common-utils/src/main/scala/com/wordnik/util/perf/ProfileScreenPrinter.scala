package com.wordnik.util.perf

import java.text.DecimalFormat

object ProfileScreenPrinter {
  val dashDivider = "---------------------------------------------------------------------------------------\n"
  def dump = {
    println(toString)
  }

  override def toString: String = {
    val buf = new StringBuilder
    buf.append("\n[Recorded profile statistics]\n");
    buf.append(format("count")).append(" |");
    buf.append(format("avg_time")).append(" |");
    buf.append(format("total_time")).append(" |");
    buf.append(format("min_time")).append(" |");
    buf.append(format("max_time")).append(" |");
    buf.append(format("call name")).append("\n");
    buf.append(dashDivider)

    Profile.getCounters(None).foreach(counter => {
      buf.append(format(counter.count)).append(" |")
      buf.append(format(counter.avgDuration)).append(" |")
      buf.append(format(counter.totalDuration)).append(" |")
      buf.append(format(counter.minDuration)).append(" |")
      buf.append(format(counter.maxDuration)).append(" | ")
      buf.append(format(counter.key))
      buf.append("\n");
    })
    buf.toString
  }

  def format(value: String): String = {
    val buf = new StringBuffer()
    buf.append({
      if (value == null) ""
      else value
    })
    (buf.length to 10).foreach(i => buf.append(" "))
    buf.toString
  }

  def format(value: Double): String = {
    val nf = new DecimalFormat("######.#");
    val buf = new StringBuffer()
    buf.append(nf.format(value))
    (buf.length to 10).foreach(i => buf.append(" "))
    buf.toString
  }

  def format(value: Long): String = {
    val nf = new DecimalFormat("######")
    val buf = new StringBuffer()
    buf.append(nf.format(value))
    (buf.length to 10).foreach(i => buf.append(" "))
    buf.toString
  }

  def format(value: Int): String = {
    val nf = new DecimalFormat("######");
    val buf = new StringBuffer()
    buf.append(nf.format(value))
    (buf.length to 10).foreach(i => buf.append(" "))
    buf.toString
  }
}