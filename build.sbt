name := "aquery"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test"

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"

parallelExecution in Test := false

fork in run := true

scalacOptions ++= Seq("-feature", "-deprecation")
