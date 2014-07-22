package com.wordnik.sbt

import _root_.sbt._
import classpath.ClasspathUtilities
import Project.Initialize
import Keys._

object DistPlugin extends Plugin {

  object DistKeys {
    val dist = TaskKey[File]("dist", "Build a distribution, assemble the files, create a launcher and make an archive.")
    val stage = TaskKey[Seq[File]]("stage", "Build a distribution, assemble the files and create a launcher.")
    val assembleJarsAndClasses = TaskKey[Seq[File]]("assemble-jars-and-classes", "Assemble jars and classes")
  }

  import DistKeys._
  val Dist = config("dist")

  private def assembleJarsAndClassesTask: Initialize[Task[Seq[File]]] =
    (fullClasspath in Runtime, excludeFilter in Dist, target in Dist) map { (cp, excl, tgt) =>
      IO.delete(tgt)
      val (libs, dirs) = cp.map(_.data).toSeq partition ClasspathUtilities.isArchive
      val jars = libs.descendantsExcept("*", excl) x flat(tgt / "lib")
      val classesAndResources = dirs flatMap { dir =>
        val files = dir.descendantsExcept("*", excl)
        files x rebase(dir, tgt / "lib")
      }

      (IO.copy(jars) ++ IO.copy(classesAndResources)).toSeq
    }


  private def createLauncherScriptTask(base: File, name: String, libFiles: Seq[File], logger: Logger): File = {
    val f = base / "bin" / name
    if (!f.getParentFile.exists()) f.getParentFile.mkdirs()
    IO.write(f, createScriptString(base, name, libFiles))
    "chmod +x %s".format(f.getAbsolutePath) ! logger
    f
  }

  private def stageTask: Initialize[Task[Seq[File]]] =
    (assembleJarsAndClasses in Dist, target in Dist, name in Dist, streams) map { (libFiles, tgt, nm, s) =>
      val launch = createLauncherScriptTask(tgt, nm, libFiles, s.log)
      val logsDir = tgt / "logs"
      if (!logsDir.exists()) logsDir.mkdirs()
      libFiles ++ Seq(launch, logsDir)
    }

  private def distTask: Initialize[Task[File]] =
    (stage in Dist, target in Dist, name in Dist, version in Dist) map { (files, tgt, nm, ver) =>
      val zipFile = tgt / ".." / (nm + "-" + ver + ".zip")
      val paths = files x rebase(tgt, nm)
      IO.zip(paths, zipFile)
      zipFile
    }

  val distSettings = Seq(
     excludeFilter in Dist := HiddenFileFilter,
     target in Dist <<= (target in Compile)(_ / "dist"),
     assembleJarsAndClasses in Dist <<= assembleJarsAndClassesTask,
     stage in Dist <<= stageTask,
     dist in Dist <<= distTask,
     dist <<= dist in Dist,
     name in Dist <<= name
   )

  private def createScriptString(base: File, name: String, libFiles: Seq[File]): String = {
    """
      |#!/bin/env bash
      |
      |export CLASSPATH="lib:%s"
      |JAVA_CONFIG_OPTIONS="-Xms500m -Xmx1000m -XX:NewSize=200m -XX:MaxNewSize=200m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:PermSize=100m -XX:MaxPermSize=100m"
      |export JAVA_OPTS="-Duser.timezone=GMT ${JAVA_CONFIG_OPTIONS} ${JAVA_DEBUG_OPTIONS} "
      |
      |java $WORDNIK_OPTS $JAVA_CONFIG_OPTIONS $JAVA_OPTS -cp $CLASSPATH "$@"
      |
    """.stripMargin.format(classPathString(base, libFiles))
  }

  private def classPathString(base: File, libFiles: Seq[File]) = {
    (libFiles filter ClasspathUtilities.isArchive map (_.relativeTo(base))).flatten mkString java.io.File.pathSeparator
  }
}