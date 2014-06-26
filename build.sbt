name := "dynamodb4s"

organization := "jp.co.bizreach"

version := "0.0.1"

scalaVersion := "2.11.1"

libraryDependencies ++= Seq(
  "com.fasterxml.jackson.module" %% "jackson-module-scala"  % "2.4.0-rc2",
  "com.github.seratch"           %% "awscala"               % "0.2.5"
)
