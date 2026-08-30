package toast.db

import cats.effect.{IO, Resource}
import cats.syntax.all._
import org.typelevel.doobie._
import org.typelevel.doobie.implicits._
import org.typelevel.doobie.hikari.HikariTransactor
import toast.config.Config

import scala.concurrent.ExecutionContext

final class Db(config: Config) {

  val mainTable: String = "events_scalar"
  val jsonbTable: String = "events_jsonb"
  val jsonbExternalTable: String = "events_jsonb_external"

  val scalarChildTables: List[String] =
    (if (config.enableJobCreated) List("events_job_created", "events_job_created_params") else Nil) ++
      (if (config.enableJobInProgress) List("events_job_in_progress") else Nil) ++
      (if (config.enableJobSuccess) List("events_job_success") else Nil) ++
      (if (config.enableJobFailed) List("events_job_failed") else Nil)

  val scalarTables: List[String] = mainTable :: scalarChildTables
  val jsonbTables: List[String] = List(jsonbTable, jsonbExternalTable)

  def transactor: Resource[IO, HikariTransactor[IO]] =
    HikariTransactor.newHikariTransactor[IO](
      "org.postgresql.Driver",
      "jdbc:postgresql://localhost:5432/toast",
      "toast",
      "toast",
      ExecutionContext.global
    )

  private val dropJobCreatedParams = sql"DROP TABLE IF EXISTS events_job_created_params"
  private val dropJobCreated = sql"DROP TABLE IF EXISTS events_job_created"
  private val dropJobInProgress = sql"DROP TABLE IF EXISTS events_job_in_progress"
  private val dropJobSuccess = sql"DROP TABLE IF EXISTS events_job_success"
  private val dropJobFailed = sql"DROP TABLE IF EXISTS events_job_failed"
  private val dropMain = sql"DROP TABLE IF EXISTS events_scalar"
  private val dropJsonb = sql"DROP TABLE IF EXISTS events_jsonb"
  private val dropJsonbExternal = sql"DROP TABLE IF EXISTS events_jsonb_external"

  private val createMain = sql"""
    CREATE TABLE events_scalar (
      event_id     UUID PRIMARY KEY,
      job_id       UUID NOT NULL,
      event_type   TEXT NOT NULL,
      occurred_at  TIMESTAMPTZ NOT NULL,
      source       TEXT NOT NULL,
      level        TEXT NOT NULL
    )
  """

  private val createJobCreated = sql"""
    CREATE TABLE events_job_created (
      event_id      UUID PRIMARY KEY REFERENCES events_scalar (event_id),
      priority      INT NOT NULL,
      submitted_by  TEXT NOT NULL,
      queue_name    TEXT NOT NULL
    )
  """

  private val createJobCreatedParams = sql"""
    CREATE TABLE events_job_created_params (
      event_id  UUID NOT NULL REFERENCES events_job_created (event_id),
      seq       INT NOT NULL,
      name      TEXT NOT NULL,
      value     TEXT NOT NULL,
      PRIMARY KEY (event_id, seq)
    )
  """

  private val createJobInProgress = sql"""
    CREATE TABLE events_job_in_progress (
      event_id          UUID PRIMARY KEY REFERENCES events_scalar (event_id),
      worker_id         TEXT NOT NULL,
      attempt           INT NOT NULL,
      progress_percent  INT NOT NULL
    )
  """

  private val createJobSuccess = sql"""
    CREATE TABLE events_job_success (
      event_id           UUID PRIMARY KEY REFERENCES events_scalar (event_id),
      duration_ms        BIGINT NOT NULL,
      result_summary     TEXT NOT NULL,
      output_size_bytes  BIGINT NOT NULL
    )
  """

  private val createJobFailed = sql"""
    CREATE TABLE events_job_failed (
      event_id       UUID PRIMARY KEY REFERENCES events_scalar (event_id),
      attempt        INT NOT NULL,
      error_message  TEXT NOT NULL,
      stack_trace    TEXT NOT NULL
    )
  """

  private val createJsonb = sql"""
    CREATE TABLE events_jsonb (
      event_id     UUID PRIMARY KEY,
      job_id       UUID NOT NULL,
      event_type   TEXT NOT NULL,
      occurred_at  TIMESTAMPTZ NOT NULL,
      source       TEXT NOT NULL,
      level        TEXT NOT NULL,
      body         JSONB NOT NULL
    )
  """

  // Same shape as events_jsonb, but STORAGE EXTERNAL disables TOAST compression on `body`
  // (it can still move out-of-line, just uncompressed) — lets the size/read comparison
  // isolate the effect of compression from the effect of TOASTing itself.
  // STORAGE isn't a CREATE TABLE column-definition clause; it can only be set afterwards
  // via ALTER TABLE.
  private val createJsonbExternal = sql"""
    CREATE TABLE events_jsonb_external (
      event_id     UUID PRIMARY KEY,
      job_id       UUID NOT NULL,
      event_type   TEXT NOT NULL,
      occurred_at  TIMESTAMPTZ NOT NULL,
      source       TEXT NOT NULL,
      level        TEXT NOT NULL,
      body         JSONB NOT NULL
    )
  """

  private val alterJsonbExternalStorage =
    sql"ALTER TABLE events_jsonb_external ALTER COLUMN body SET STORAGE EXTERNAL"

  private val ddlStatements: List[Fragment] = {
    // Drops always cover every possible table (idempotent DROP IF EXISTS), regardless of
    // which variants this run's config enables: a *previous* run may have had a different
    // config and left tables behind with a FK into events_scalar, which would otherwise
    // block dropping it. Only creates are conditional on the current config.
    // Grandchild first: it holds a FK into events_job_created, which itself holds a FK into events_scalar.
    val dropChildren =
      List(dropJobCreatedParams, dropJobCreated, dropJobInProgress, dropJobSuccess, dropJobFailed)

    val createChildren =
      (if (config.enableJobCreated) List(createJobCreated, createJobCreatedParams) else Nil) ++
        (if (config.enableJobInProgress) List(createJobInProgress) else Nil) ++
        (if (config.enableJobSuccess) List(createJobSuccess) else Nil) ++
        (if (config.enableJobFailed) List(createJobFailed) else Nil)

    dropChildren ++ List(dropMain, dropJsonb, dropJsonbExternal, createMain) ++ createChildren ++
      List(createJsonb, createJsonbExternal, alterJsonbExternalStorage)
  }

  def recreateSchema(xa: Transactor[IO]): IO[Unit] =
    ddlStatements.traverse_(_.update.run.transact(xa))

  def analyze(xa: Transactor[IO]): IO[Unit] =
    (scalarTables :+ jsonbTable :+ jsonbExternalTable).traverse_ { table =>
      Fragment.const(s"ANALYZE $table").update.run.transact(xa)
    }
}
