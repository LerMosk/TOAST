ThisBuild / scalaVersion := "2.13.18"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.toast"

val doobieVersion = "1.0.0-RC13"
val circeVersion = "0.14.15"

val commonSettings = Seq(
  Compile / run / fork := true,
  Compile / run / javaOptions ++= Seq("-Xmx6g"),
  libraryDependencies ++= Seq(
    "org.typelevel" %% "doobie-core" % doobieVersion,
    "org.typelevel" %% "doobie-postgres" % doobieVersion,
    "org.typelevel" %% "doobie-postgres-circe" % doobieVersion,
    "org.typelevel" %% "doobie-hikari" % doobieVersion,
    "io.circe" %% "circe-core" % circeVersion,
    "io.circe" %% "circe-generic" % circeVersion,
    "io.circe" %% "circe-parser" % circeVersion,
    "co.fs2" %% "fs2-core" % "3.11.0"
  )
)

lazy val common = (project in file("common"))
  .settings(commonSettings)
  .settings(
    name := "toast-common"
  )

lazy val core = (project in file("core"))
  .dependsOn(common)
  .settings(commonSettings)
  .settings(
    name := "toast-core",
    libraryDependencies += "com.github.pureconfig" %% "pureconfig" % "0.17.10"
  )

lazy val root = (project in file("."))
  .aggregate(common, core)
  .settings(name := "TOAST")
