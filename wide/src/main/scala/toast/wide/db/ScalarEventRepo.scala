package toast.wide.db

import cats.effect.IO
import cats.syntax.all._
import fs2.Stream
import org.typelevel.doobie._
import org.typelevel.doobie.implicits._
import org.typelevel.doobie.postgres.implicits._
import toast.db.EventRepo
import toast.wide.model.{Event, EventBody}

final class ScalarEventRepo(xa: Transactor[IO]) extends EventRepo[Event] {

  private val batchSize = 10000

  private def attrCols(n: Int, prefix: String = ""): String = (1 to n).map(i => s"$prefix" + s"attr$i").mkString(", ")
  private def placeholders(n: Int): String = List.fill(n)("?").mkString(", ")

  // Flat (not nested) row case classes: doobie's Read/Write derivation for this version
  // doesn't auto-flatten an embedded case class field, and a plain tuple caps out at 22
  // elements (below what these wide rows need), so every column gets its own field here.
  private final case class MainRow(
      eventId: java.util.UUID,
      jobId: java.util.UUID,
      eventType: String,
      occurredAt: java.time.Instant,
      source: String,
      level: String
  )

  private final case class CreatedRow(
      eventId: java.util.UUID,
      priority: Int,
      submittedBy: String,
      queueName: String,
      attr1: String, attr2: String, attr3: String, attr4: String, attr5: String,
      attr6: String, attr7: String, attr8: String, attr9: String, attr10: String,
      attr11: String, attr12: String, attr13: String, attr14: String, attr15: String,
      attr16: String, attr17: String, attr18: String, attr19: String, attr20: String,
      attr21: String, attr22: String
  )

  private final case class SuccessRow(
      eventId: java.util.UUID,
      durationMs: Long,
      resultSummary: String,
      outputSizeBytes: Long,
      attr1: String, attr2: String, attr3: String, attr4: String, attr5: String,
      attr6: String, attr7: String, attr8: String, attr9: String, attr10: String,
      attr11: String, attr12: String, attr13: String, attr14: String, attr15: String,
      attr16: String, attr17: String, attr18: String, attr19: String, attr20: String,
      attr21: String, attr22: String
  )

  private final case class FailedRow(
      eventId: java.util.UUID,
      attempt: Int,
      errorMessage: String,
      stackTrace: String,
      attr1: String, attr2: String, attr3: String, attr4: String, attr5: String,
      attr6: String, attr7: String, attr8: String, attr9: String, attr10: String,
      attr11: String, attr12: String, attr13: String, attr14: String, attr15: String,
      attr16: String, attr17: String, attr18: String, attr19: String, attr20: String,
      attr21: String
  )

  private final case class CreatedJoinRow(
      eventId: java.util.UUID, jobId: java.util.UUID, occurredAt: java.time.Instant, source: String, level: String,
      priority: Int, submittedBy: String, queueName: String,
      attr1: String, attr2: String, attr3: String, attr4: String, attr5: String,
      attr6: String, attr7: String, attr8: String, attr9: String, attr10: String,
      attr11: String, attr12: String, attr13: String, attr14: String, attr15: String,
      attr16: String, attr17: String, attr18: String, attr19: String, attr20: String,
      attr21: String, attr22: String
  )

  private final case class SuccessJoinRow(
      eventId: java.util.UUID, jobId: java.util.UUID, occurredAt: java.time.Instant, source: String, level: String,
      durationMs: Long, resultSummary: String, outputSizeBytes: Long,
      attr1: String, attr2: String, attr3: String, attr4: String, attr5: String,
      attr6: String, attr7: String, attr8: String, attr9: String, attr10: String,
      attr11: String, attr12: String, attr13: String, attr14: String, attr15: String,
      attr16: String, attr17: String, attr18: String, attr19: String, attr20: String,
      attr21: String, attr22: String
  )

  private final case class FailedJoinRow(
      eventId: java.util.UUID, jobId: java.util.UUID, occurredAt: java.time.Instant, source: String, level: String,
      attempt: Int, errorMessage: String, stackTrace: String,
      attr1: String, attr2: String, attr3: String, attr4: String, attr5: String,
      attr6: String, attr7: String, attr8: String, attr9: String, attr10: String,
      attr11: String, attr12: String, attr13: String, attr14: String, attr15: String,
      attr16: String, attr17: String, attr18: String, attr19: String, attr20: String,
      attr21: String
  )

  private def toMainRow(e: Event): MainRow =
    MainRow(e.eventId, e.jobId, e.eventType, e.occurredAt, e.source, e.level)

  private def toCreatedRow(eventId: java.util.UUID, b: EventBody.JobCreated): CreatedRow =
    CreatedRow(
      eventId, b.priority, b.submittedBy, b.queueName,
      b.attr1, b.attr2, b.attr3, b.attr4, b.attr5, b.attr6, b.attr7, b.attr8, b.attr9, b.attr10,
      b.attr11, b.attr12, b.attr13, b.attr14, b.attr15, b.attr16, b.attr17, b.attr18, b.attr19, b.attr20,
      b.attr21, b.attr22
    )

  private def toSuccessRow(eventId: java.util.UUID, b: EventBody.JobSuccess): SuccessRow =
    SuccessRow(
      eventId, b.durationMs, b.resultSummary, b.outputSizeBytes,
      b.attr1, b.attr2, b.attr3, b.attr4, b.attr5, b.attr6, b.attr7, b.attr8, b.attr9, b.attr10,
      b.attr11, b.attr12, b.attr13, b.attr14, b.attr15, b.attr16, b.attr17, b.attr18, b.attr19, b.attr20,
      b.attr21, b.attr22
    )

  private def toFailedRow(eventId: java.util.UUID, b: EventBody.JobFailed): FailedRow =
    FailedRow(
      eventId, b.attempt, b.errorMessage, b.stackTrace,
      b.attr1, b.attr2, b.attr3, b.attr4, b.attr5, b.attr6, b.attr7, b.attr8, b.attr9, b.attr10,
      b.attr11, b.attr12, b.attr13, b.attr14, b.attr15, b.attr16, b.attr17, b.attr18, b.attr19, b.attr20,
      b.attr21
    )

  private val insertMain =
    Update[MainRow](
      "INSERT INTO wide_events (event_id, job_id, event_type, occurred_at, source, level) VALUES (?, ?, ?, ?, ?, ?)"
    )

  private val insertCreated =
    Update[CreatedRow](
      s"INSERT INTO wide_job_created (event_id, priority, submitted_by, queue_name, ${attrCols(22)}) VALUES (${placeholders(4 + 22)})"
    )

  private val insertSuccess =
    Update[SuccessRow](
      s"INSERT INTO wide_job_success (event_id, duration_ms, result_summary, output_size_bytes, ${attrCols(22)}) VALUES (${placeholders(4 + 22)})"
    )

  private val insertFailed =
    Update[FailedRow](
      s"INSERT INTO wide_job_failed (event_id, attempt, error_message, stack_trace, ${attrCols(21)}) VALUES (${placeholders(4 + 21)})"
    )

  private def insertBatch(events: List[Event]): IO[Unit] = {
    val created = events.collect { case e @ Event(_, _, _, _, _, _, b: EventBody.JobCreated) => toCreatedRow(e.eventId, b) }
    val success = events.collect { case e @ Event(_, _, _, _, _, _, b: EventBody.JobSuccess) => toSuccessRow(e.eventId, b) }
    val failed = events.collect { case e @ Event(_, _, _, _, _, _, b: EventBody.JobFailed) => toFailedRow(e.eventId, b) }

    // wide_events must be populated before the child tables (FK). No row explosion here
    // (flat fields, not a collection) so a single batch per table per chunk is enough.
    for {
      _ <- insertMain.updateMany(events.map(toMainRow)).transact(xa)
      _ <- List(
        insertCreated.updateMany(created),
        insertSuccess.updateMany(success),
        insertFailed.updateMany(failed)
      ).traverse_(_.transact(xa))
    } yield ()
  }

  override def insertAll(events: Stream[IO, Event]): IO[Unit] =
    events.chunkN(batchSize).evalMap(chunk => insertBatch(chunk.toList)).compile.drain

  override def selectAll(): Stream[IO, Event] = {
    val created: Stream[IO, Event] =
      Fragment
        .const(s"""
          SELECT m.event_id, m.job_id, m.occurred_at, m.source, m.level,
                 c.priority, c.submitted_by, c.queue_name, ${attrCols(22, "c.")}
          FROM wide_events m JOIN wide_job_created c ON m.event_id = c.event_id
        """)
        .query[CreatedJoinRow]
        .stream
        .transact(xa)
        .map { r =>
          val body = EventBody.JobCreated(
            r.priority, r.submittedBy, r.queueName,
            r.attr1, r.attr2, r.attr3, r.attr4, r.attr5, r.attr6, r.attr7, r.attr8, r.attr9, r.attr10,
            r.attr11, r.attr12, r.attr13, r.attr14, r.attr15, r.attr16, r.attr17, r.attr18, r.attr19, r.attr20,
            r.attr21, r.attr22
          )
          Event(r.eventId, r.jobId, Event.JobCreatedType, r.occurredAt, r.source, r.level, body)
        }

    val success: Stream[IO, Event] =
      Fragment
        .const(s"""
          SELECT m.event_id, m.job_id, m.occurred_at, m.source, m.level,
                 s.duration_ms, s.result_summary, s.output_size_bytes, ${attrCols(22, "s.")}
          FROM wide_events m JOIN wide_job_success s ON m.event_id = s.event_id
        """)
        .query[SuccessJoinRow]
        .stream
        .transact(xa)
        .map { r =>
          val body = EventBody.JobSuccess(
            r.durationMs, r.resultSummary, r.outputSizeBytes,
            r.attr1, r.attr2, r.attr3, r.attr4, r.attr5, r.attr6, r.attr7, r.attr8, r.attr9, r.attr10,
            r.attr11, r.attr12, r.attr13, r.attr14, r.attr15, r.attr16, r.attr17, r.attr18, r.attr19, r.attr20,
            r.attr21, r.attr22
          )
          Event(r.eventId, r.jobId, Event.JobSuccessType, r.occurredAt, r.source, r.level, body)
        }

    val failed: Stream[IO, Event] =
      Fragment
        .const(s"""
          SELECT m.event_id, m.job_id, m.occurred_at, m.source, m.level,
                 f.attempt, f.error_message, f.stack_trace, ${attrCols(21, "f.")}
          FROM wide_events m JOIN wide_job_failed f ON m.event_id = f.event_id
        """)
        .query[FailedJoinRow]
        .stream
        .transact(xa)
        .map { r =>
          val body = EventBody.JobFailed(
            r.attempt, r.errorMessage, r.stackTrace,
            r.attr1, r.attr2, r.attr3, r.attr4, r.attr5, r.attr6, r.attr7, r.attr8, r.attr9, r.attr10,
            r.attr11, r.attr12, r.attr13, r.attr14, r.attr15, r.attr16, r.attr17, r.attr18, r.attr19, r.attr20,
            r.attr21
          )
          Event(r.eventId, r.jobId, Event.JobFailedType, r.occurredAt, r.source, r.level, body)
        }

    created ++ success ++ failed
  }
}
