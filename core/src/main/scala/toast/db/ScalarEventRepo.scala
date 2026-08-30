package toast.db

import cats.effect.IO
import cats.syntax.all._
import fs2.Stream
import org.typelevel.doobie._
import org.typelevel.doobie.implicits._
import org.typelevel.doobie.postgres.implicits._
import toast.config.Config
import toast.model.{Event, EventBody, Param}

final class ScalarEventRepo(xa: Transactor[IO], config: Config) extends EventRepo[Event] {

  private val batchSize = 5000

  private type MainRow = (java.util.UUID, java.util.UUID, String, java.time.Instant, String, String)
  private type CreatedRow = (java.util.UUID, Int, String, String)
  private type CreatedParamsRow = (java.util.UUID, Int, String, String)
  private type InProgressRow = (java.util.UUID, String, Int, Int)
  private type SuccessRow = (java.util.UUID, Long, String, Long)
  private type FailedRow = (java.util.UUID, Int, String, String)

  private final case class AllRow(
      eventId: java.util.UUID, jobId: java.util.UUID, eventType: String, occurredAt: java.time.Instant,
      source: String, level: String,
      cPriority: Option[Int], cSubmittedBy: Option[String], cQueueName: Option[String],
      pWorkerId: Option[String], pAttempt: Option[Int], pProgressPercent: Option[Int],
      sDurationMs: Option[Long], sResultSummary: Option[String], sOutputSizeBytes: Option[Long],
      fAttempt: Option[Int], fErrorMessage: Option[String], fStackTrace: Option[String]
  )

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

  private def insertBatch(events: List[Event]): IO[Unit] = {
    val createdEvents = events.collect { case e @ Event(_, _, _, _, _, _, b: EventBody.JobCreated) => (e, b) }
    val created = createdEvents.map { case (e, b) => toCreatedRow(e, b) }
    val createdParams = createdEvents.flatMap { case (e, b) => toCreatedParamsRows(e, b) }
    val inProgress = events.collect { case e @ Event(_, _, _, _, _, _, b: EventBody.JobInProgress) => toInProgressRow(e, b) }
    val success = events.collect { case e @ Event(_, _, _, _, _, _, b: EventBody.JobSuccess) => toSuccessRow(e, b) }
    val failed = events.collect { case e @ Event(_, _, _, _, _, _, b: EventBody.JobFailed) => toFailedRow(e, b) }

    val childInserts: List[ConnectionIO[Int]] =
      (if (config.enableJobCreated) List(insertCreated.updateMany(created) *> insertCreatedParams.updateMany(createdParams)) else Nil) ++
        (if (config.enableJobInProgress) List(insertInProgress.updateMany(inProgress)) else Nil) ++
        (if (config.enableJobSuccess) List(insertSuccess.updateMany(success)) else Nil) ++
        (if (config.enableJobFailed) List(insertFailed.updateMany(failed)) else Nil)

    (insertMain.updateMany(events.map(toMainRow)) *> childInserts.sequence.void).transact(xa)
  }

  override def insertAll(events: Stream[IO, Event]): IO[Unit] =
    events.chunkN(batchSize).evalMap(chunk => insertBatch(chunk.toList)).compile.drain


  private def loadCreatedParams(): IO[Map[java.util.UUID, List[Param]]] =
    if (!config.enableJobCreated) IO.pure(Map.empty)
    else
      sql"""
        SELECT event_id, seq, name, value
        FROM events_job_created_params
        ORDER BY event_id, seq
      """
        .query[(java.util.UUID, Int, String, String)]
        .to[List]
        .transact(xa)
        .map(_.groupBy(_._1).view.mapValues(_.map { case (_, _, name, value) => Param(name, value) }.toList).toMap)

  override def selectAll(): Stream[IO, Event] =
    Stream.eval(loadCreatedParams()).flatMap { paramsByEvent =>
      val createdJoin = if (config.enableJobCreated) "LEFT JOIN events_job_created c ON m.event_id = c.event_id" else ""
      val createdCols = if (config.enableJobCreated) "c.priority, c.submitted_by, c.queue_name" else "NULL::int4, NULL::text, NULL::text"
      val inProgressJoin = if (config.enableJobInProgress) "LEFT JOIN events_job_in_progress p ON m.event_id = p.event_id" else ""
      val inProgressCols = if (config.enableJobInProgress) "p.worker_id, p.attempt, p.progress_percent" else "NULL::text, NULL::int4, NULL::int4"
      val successJoin = if (config.enableJobSuccess) "LEFT JOIN events_job_success s ON m.event_id = s.event_id" else ""
      val successCols = if (config.enableJobSuccess) "s.duration_ms, s.result_summary, s.output_size_bytes" else "NULL::int8, NULL::text, NULL::int8"
      val failedJoin = if (config.enableJobFailed) "LEFT JOIN events_job_failed f ON m.event_id = f.event_id" else ""
      val failedCols = if (config.enableJobFailed) "f.attempt, f.error_message, f.stack_trace" else "NULL::int4, NULL::text, NULL::text"

      Fragment
        .const(s"""
          SELECT m.event_id, m.job_id, m.event_type, m.occurred_at, m.source, m.level,
                 $createdCols, $inProgressCols, $successCols, $failedCols
          FROM events_scalar m
            $createdJoin
            $inProgressJoin
            $successJoin
            $failedJoin
        """)
        .query[AllRow]
        .stream
        .transact(xa)
        .map { r =>
          val body: EventBody = r.eventType match {
            case Event.JobCreatedType =>
              EventBody.JobCreated(r.cPriority.get, r.cSubmittedBy.get, r.cQueueName.get, paramsByEvent.getOrElse(r.eventId, Nil))
            case Event.JobInProgressType =>
              EventBody.JobInProgress(r.pWorkerId.get, r.pAttempt.get, r.pProgressPercent.get)
            case Event.JobSuccessType =>
              EventBody.JobSuccess(r.sDurationMs.get, r.sResultSummary.get, r.sOutputSizeBytes.get)
            case Event.JobFailedType =>
              EventBody.JobFailed(r.fAttempt.get, r.fErrorMessage.get, r.fStackTrace.get)
          }
          Event(r.eventId, r.jobId, r.eventType, r.occurredAt, r.source, r.level, body)
        }
    }
}
