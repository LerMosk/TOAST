package toast.db

import cats.effect.IO
import cats.syntax.all._
import fs2.Stream
import org.typelevel.doobie._
import org.typelevel.doobie.implicits._
import org.typelevel.doobie.postgres.implicits._
import toast.config.Config
import toast.model.{Event, EventBody, Param}

final class ScalarEventRepo(xa: Transactor[IO], config: Config) extends EventRepo {

  private val batchSize = 10000

  private type MainRow = (java.util.UUID, java.util.UUID, String, java.time.Instant, String, String)
  private type CreatedRow = (java.util.UUID, Int, String, String)
  private type CreatedParamsRow = (java.util.UUID, Int, String, String)
  private type InProgressRow = (java.util.UUID, String, Int, Int)
  private type SuccessRow = (java.util.UUID, Long, String, Long)
  private type FailedRow = (java.util.UUID, Int, String, String)

  private def toMainRow(e: Event): MainRow =
    (e.eventId, e.jobId, e.eventType, e.occurredAt, e.source, e.level)

  private def toCreatedRow(e: Event, b: EventBody.JobCreated): CreatedRow =
    (e.eventId, b.priority, b.submittedBy, b.queueName)

  private def toCreatedParamsRows(e: Event, b: EventBody.JobCreated): List[CreatedParamsRow] =
    b.params.zipWithIndex.map { case (param, seq) => (e.eventId, seq, param.name, param.value) }

  private def toInProgressRow(e: Event, b: EventBody.JobInProgress): InProgressRow =
    (e.eventId, b.workerId, b.attempt, b.progressPercent)

  private def toSuccessRow(e: Event, b: EventBody.JobSuccess): SuccessRow =
    (e.eventId, b.durationMs, b.resultSummary, b.outputSizeBytes)

  private def toFailedRow(e: Event, b: EventBody.JobFailed): FailedRow =
    (e.eventId, b.attempt, b.errorMessage, b.stackTrace)

  private val insertMain =
    Update[MainRow](
      "INSERT INTO events_scalar (event_id, job_id, event_type, occurred_at, source, level) VALUES (?, ?, ?, ?, ?, ?)"
    )

  private val insertCreated =
    Update[CreatedRow](
      "INSERT INTO events_job_created (event_id, priority, submitted_by, queue_name) VALUES (?, ?, ?, ?)"
    )

  private val insertCreatedParams =
    Update[CreatedParamsRow](
      "INSERT INTO events_job_created_params (event_id, seq, name, value) VALUES (?, ?, ?, ?)"
    )

  private val insertInProgress =
    Update[InProgressRow](
      "INSERT INTO events_job_in_progress (event_id, worker_id, attempt, progress_percent) VALUES (?, ?, ?, ?)"
    )

  private val insertSuccess =
    Update[SuccessRow](
      "INSERT INTO events_job_success (event_id, duration_ms, result_summary, output_size_bytes) VALUES (?, ?, ?, ?)"
    )

  private val insertFailed =
    Update[FailedRow](
      "INSERT INTO events_job_failed (event_id, attempt, error_message, stack_trace) VALUES (?, ?, ?, ?)"
    )

  // Disabled variants have no table at all, so their Update/Query must never even be
  // attempted (a PreparedStatement against a nonexistent table fails at prepare time,
  // even with an empty batch) — every variant is gated by its config flag below.
  private def insertBatch(events: List[Event]): IO[Unit] = {
    val createdEvents = events.collect { case e @ Event(_, _, _, _, _, _, b: EventBody.JobCreated) => (e, b) }
    val created = createdEvents.map { case (e, b) => toCreatedRow(e, b) }
    val createdParams = createdEvents.flatMap { case (e, b) => toCreatedParamsRows(e, b) }
    val inProgress = events.collect { case e @ Event(_, _, _, _, _, _, b: EventBody.JobInProgress) => toInProgressRow(e, b) }
    val success = events.collect { case e @ Event(_, _, _, _, _, _, b: EventBody.JobSuccess) => toSuccessRow(e, b) }
    val failed = events.collect { case e @ Event(_, _, _, _, _, _, b: EventBody.JobFailed) => toFailedRow(e, b) }

    val childInserts: List[IO[Int]] =
      (if (config.enableJobCreated) List(insertCreated.updateMany(created).transact(xa)) else Nil) ++
        (if (config.enableJobInProgress) List(insertInProgress.updateMany(inProgress).transact(xa)) else Nil) ++
        (if (config.enableJobSuccess) List(insertSuccess.updateMany(success).transact(xa)) else Nil) ++
        (if (config.enableJobFailed) List(insertFailed.updateMany(failed).transact(xa)) else Nil)

    // events_scalar must be populated before the child tables (FK), and events_job_created
    // before events_job_created_params (FK on top of a FK).
    for {
      _ <- insertMain.updateMany(events.map(toMainRow)).transact(xa)
      _ <- childInserts.traverse_(identity)
      // A single event-batch of JobCreated rows can still fan out past batchSize params;
      // sub-batch the JDBC insert itself to keep each addBatch bounded.
      _ <-
        if (config.enableJobCreated)
          createdParams.grouped(batchSize).toList.traverse_(chunk => insertCreatedParams.updateMany(chunk).transact(xa))
        else IO.unit
    } yield ()
  }

  override def insertAll(events: Stream[IO, Event]): IO[Unit] =
    events.chunkN(batchSize).evalMap(chunk => insertBatch(chunk.toList)).compile.drain

  override def selectAll(): Stream[IO, Event] = {
    val created: Stream[IO, Event] =
      sql"""
        SELECT m.event_id, m.job_id, m.occurred_at, m.source, m.level, c.priority, c.submitted_by, c.queue_name,
               COALESCE(array_agg(p.name ORDER BY p.seq) FILTER (WHERE p.event_id IS NOT NULL), ARRAY[]::text[]),
               COALESCE(array_agg(p.value ORDER BY p.seq) FILTER (WHERE p.event_id IS NOT NULL), ARRAY[]::text[])
        FROM events_scalar m
          JOIN events_job_created c ON m.event_id = c.event_id
          LEFT JOIN events_job_created_params p ON c.event_id = p.event_id
        GROUP BY m.event_id, m.job_id, m.occurred_at, m.source, m.level, c.priority, c.submitted_by, c.queue_name
      """
        .query[(java.util.UUID, java.util.UUID, java.time.Instant, String, String, Int, String, String, List[String], List[String])]
        .stream
        .transact(xa)
        .map { case (eventId, jobId, occurredAt, source, level, priority, submittedBy, queueName, names, values) =>
          val params = names.zip(values).map { case (name, value) => Param(name, value) }
          Event(eventId, jobId, Event.JobCreatedType, occurredAt, source, level, EventBody.JobCreated(priority, submittedBy, queueName, params))
        }

    val inProgress: Stream[IO, Event] =
      sql"""
        SELECT m.event_id, m.job_id, m.occurred_at, m.source, m.level, p.worker_id, p.attempt, p.progress_percent
        FROM events_scalar m JOIN events_job_in_progress p ON m.event_id = p.event_id
      """
        .query[(java.util.UUID, java.util.UUID, java.time.Instant, String, String, String, Int, Int)]
        .stream
        .transact(xa)
        .map { case (eventId, jobId, occurredAt, source, level, workerId, attempt, progressPercent) =>
          Event(eventId, jobId, Event.JobInProgressType, occurredAt, source, level, EventBody.JobInProgress(workerId, attempt, progressPercent))
        }

    val success: Stream[IO, Event] =
      sql"""
        SELECT m.event_id, m.job_id, m.occurred_at, m.source, m.level, s.duration_ms, s.result_summary, s.output_size_bytes
        FROM events_scalar m JOIN events_job_success s ON m.event_id = s.event_id
      """
        .query[(java.util.UUID, java.util.UUID, java.time.Instant, String, String, Long, String, Long)]
        .stream
        .transact(xa)
        .map { case (eventId, jobId, occurredAt, source, level, durationMs, resultSummary, outputSizeBytes) =>
          Event(eventId, jobId, Event.JobSuccessType, occurredAt, source, level, EventBody.JobSuccess(durationMs, resultSummary, outputSizeBytes))
        }

    val failed: Stream[IO, Event] =
      sql"""
        SELECT m.event_id, m.job_id, m.occurred_at, m.source, m.level, f.attempt, f.error_message, f.stack_trace
        FROM events_scalar m JOIN events_job_failed f ON m.event_id = f.event_id
      """
        .query[(java.util.UUID, java.util.UUID, java.time.Instant, String, String, Int, String, String)]
        .stream
        .transact(xa)
        .map { case (eventId, jobId, occurredAt, source, level, attempt, errorMessage, stackTrace) =>
          Event(eventId, jobId, Event.JobFailedType, occurredAt, source, level, EventBody.JobFailed(attempt, errorMessage, stackTrace))
        }

    val enabledStreams: List[Stream[IO, Event]] =
      (if (config.enableJobCreated) List(created) else Nil) ++
        (if (config.enableJobInProgress) List(inProgress) else Nil) ++
        (if (config.enableJobSuccess) List(success) else Nil) ++
        (if (config.enableJobFailed) List(failed) else Nil)

    val empty: Stream[IO, Event] = Stream.empty
    enabledStreams.foldLeft(empty)(_ ++ _)
  }
}
