name := "vile"

version := "0.1"

scalaVersion := "2.11.11"
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.24"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.2"
// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.2" % Test
// https://mvnrepository.com/artifact/org.mongodb/casbah
libraryDependencies += "org.mongodb" %% "casbah" % "2.8.2" pomOnly()


        