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

// First benchmark: scalar (class-table-inheritance) vs JSONB for an ADT whose variable
// data lives in a collection (events_job_created_params).
lazy val core = (project in file("core"))
  .settings(commonSettings)
  .settings(
    name := "toast-core",
    libraryDependencies += "com.github.pureconfig" %% "pureconfig" % "0.17.10"
  )

// Second benchmark: same idea, but for a "wide row" ADT (many flat fields, no Param) —
// fully independent of `core`, its own package (toast.wide) and its own wide_-prefixed tables.
lazy val wide = (project in file("wide"))
  .settings(commonSettings)
  .settings(name := "toast-wide")

lazy val root = (project in file("."))
  .aggregate(core, wide)
  .settings(name := "TOAST")
