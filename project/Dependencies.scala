import sbt._

object Dependencies {
  object V {
    val commonsLang  = "2.6"
    val metrics      = "2.2.0"
    val slf4j        = "1.7.2"
    val junitSupport = "0.10-M2"
    val scalaTest    = "1.9.1"
    val junit        = "4.11"
    val mongo        = "2.10.1"
    val jackson      = "2.1.2"
    val akka         = "2.0.5"
  }

  val commonsLang    = "commons-lang"                 % "commons-lang"               % V.commonsLang
  val metricsCore    = "com.yammer.metrics"           % "metrics-core"               % V.metrics
  val mongoJava      = "org.mongodb"                  % "mongo-java-driver"          % V.mongo
  val jackonsJaxRs   = "com.fasterxml.jackson.jaxrs"  % "jackson-jaxrs-xml-provider" % V.jackson
  val akka           = "com.typesafe.akka"            % "akka-actor"                 % V.akka

  val slf4jApi       = "org.slf4j"                    % "slf4j-api"                  % V.slf4j
  val slf4jLog4j     = "org.slf4j"                    % "slf4j-log4j12"              % V.slf4j        % "provided"

  val junitSupport   = "com.novocode"                 % "junit-interface"            % V.junitSupport % "test"
  val scalaTest      = "org.scalatest"               %% "scalatest"                  % V.scalaTest    % "test"
  val junit          = "junit"                        % "junit"                      % V.junit        % "test"

  val testDependencies = Seq(junitSupport, scalaTest, junit)
  val slf4j = Seq(slf4jApi, slf4jLog4j)
}