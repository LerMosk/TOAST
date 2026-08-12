package toast.wide.db

import cats.effect.{IO, Resource}
import cats.syntax.all._
import org.typelevel.doobie._
import org.typelevel.doobie.implicits._
import org.typelevel.doobie.hikari.HikariTransactor

import scala.concurrent.ExecutionContext

object Db {

  val mainTable: String = "wide_events"

  val scalarChildTables: List[String] =
    List("wide_job_created", "wide_job_success", "wide_job_failed")

  val scalarTables: List[String] = mainTable :: scalarChildTables

  val jsonbTable: String = "wide_events_jsonb"

  def transactor: Resource[IO, HikariTransactor[IO]] =
    HikariTransactor.newHikariTransactor[IO](
      "org.postgresql.Driver",
      "jdbc:postgresql://localhost:5432/toast",
      "toast",
      "toast",
      ExecutionContext.global
    )

  private def attrColumnsDdl(n: Int): String =
    (1 to n).map(i => s"attr$i TEXT NOT NULL").mkString(",\n        ")

  private val ddlStatements: List[Fragment] = List(
    // Children first: they hold a FK into wide_events, so must be dropped before it.
    sql"DROP TABLE IF EXISTS wide_job_created",
    sql"DROP TABLE IF EXISTS wide_job_success",
    sql"DROP TABLE IF EXISTS wide_job_failed",
    sql"DROP TABLE IF EXISTS wide_events",
    sql"DROP TABLE IF EXISTS wide_events_jsonb",
    sql"""
      CREATE TABLE wide_events (
        event_id     UUID PRIMARY KEY,
        job_id       UUID NOT NULL,
        event_type   TEXT NOT NULL,
        occurred_at  TIMESTAMPTZ NOT NULL,
        source       TEXT NOT NULL,
        level        TEXT NOT NULL
      )
    """,
    Fragment.const(s"""
      CREATE TABLE wide_job_created (
        event_id      UUID PRIMARY KEY REFERENCES wide_events (event_id),
        priority      INT NOT NULL,
        submitted_by  TEXT NOT NULL,
        queue_name    TEXT NOT NULL,
        ${attrColumnsDdl(22)}
      )
    """),
    Fragment.const(s"""
      CREATE TABLE wide_job_created (
        event_id      UUID PRIMARY KEY REFERENCES wide_events (event_id),
        priority      INT NOT NULL,
        submitted_by  TEXT NOT NULL,
        queue_name    TEXT NOT NULL,
        ${attrColumnsDdl(22)}
      )
    """),
    Fragment.const(s"""
      CREATE TABLE wide_job_failed (
        event_id       UUID PRIMARY KEY REFERENCES wide_events (event_id),
        attempt        INT NOT NULL,
        error_message  TEXT NOT NULL,
        stack_trace    TEXT NOT NULL,
        ${attrColumnsDdl(21)}
      )
    """),
    sql"""
      CREATE TABLE wide_events_jsonb (
        event_id     UUID PRIMARY KEY,
        job_id       UUID NOT NULL,
        event_type   TEXT NOT NULL,
        occurred_at  TIMESTAMPTZ NOT NULL,
        source       TEXT NOT NULL,
        level        TEXT NOT NULL,
        body         JSONB NOT NULL
      )
    """
  )

  def recreateSchema(xa: Transactor[IO]): IO[Unit] =
    ddlStatements.traverse_(_.update.run.transact(xa))

  def analyze(xa: Transactor[IO]): IO[Unit] =
    (scalarTables :+ jsonbTable).traverse_ { table =>
      Fragment.const(s"ANALYZE $table").update.run.transact(xa)
    }
}
