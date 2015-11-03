name := """ASD_1516_P1"""

version := "1.0"

scalaVersion := "2.11.7"

// Change this to another test framework if you prefer
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

// Uncomment to use Akka
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.11"

// Apache Common Math
libraryDependencies += "org.apache.commons" % "commons-math3" % "3.5"

libraryDependencies += "com.typesafe.akka" %% "akka-remote" % "2.3.14"
