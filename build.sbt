name := "dynamodb4s"

organization := "com.codebreak"

version := "0.0.1"

scalaVersion := "2.11.1"

libraryDependencies ++= Seq(
  "org.scala-lang"               %  "scala-reflect"         % scalaVersion.value,
  "com.github.seratch"           %% "awscala"               % "0.2.5"
)
