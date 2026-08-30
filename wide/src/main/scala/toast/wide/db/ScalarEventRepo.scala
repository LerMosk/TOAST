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

  // One row per event covering all three variants at once (main LEFT JOINed with all three
  // child tables): exactly one of the three column groups is non-NULL per row, determined
  // by eventType, the other two come back NULL from their unmatched LEFT JOIN.
  private final case class AllRow(
      eventId: java.util.UUID, jobId: java.util.UUID, eventType: String, occurredAt: java.time.Instant,
      source: String, level: String,
      cPriority: Option[Int], cSubmittedBy: Option[String], cQueueName: Option[String],
      cAttr1: Option[String], cAttr2: Option[String], cAttr3: Option[String], cAttr4: Option[String], cAttr5: Option[String],
      cAttr6: Option[String], cAttr7: Option[String], cAttr8: Option[String], cAttr9: Option[String], cAttr10: Option[String],
      cAttr11: Option[String], cAttr12: Option[String], cAttr13: Option[String], cAttr14: Option[String], cAttr15: Option[String],
      cAttr16: Option[String], cAttr17: Option[String], cAttr18: Option[String], cAttr19: Option[String], cAttr20: Option[String],
      cAttr21: Option[String], cAttr22: Option[String],
      sDurationMs: Option[Long], sResultSummary: Option[String], sOutputSizeBytes: Option[Long],
      sAttr1: Option[String], sAttr2: Option[String], sAttr3: Option[String], sAttr4: Option[String], sAttr5: Option[String],
      sAttr6: Option[String], sAttr7: Option[String], sAttr8: Option[String], sAttr9: Option[String], sAttr10: Option[String],
      sAttr11: Option[String], sAttr12: Option[String], sAttr13: Option[String], sAttr14: Option[String], sAttr15: Option[String],
      sAttr16: Option[String], sAttr17: Option[String], sAttr18: Option[String], sAttr19: Option[String], sAttr20: Option[String],
      sAttr21: Option[String], sAttr22: Option[String],
      fAttempt: Option[Int], fErrorMessage: Option[String], fStackTrace: Option[String],
      fAttr1: Option[String], fAttr2: Option[String], fAttr3: Option[String], fAttr4: Option[String], fAttr5: Option[String],
      fAttr6: Option[String], fAttr7: Option[String], fAttr8: Option[String], fAttr9: Option[String], fAttr10: Option[String],
      fAttr11: Option[String], fAttr12: Option[String], fAttr13: Option[String], fAttr14: Option[String], fAttr15: Option[String],
      fAttr16: Option[String], fAttr17: Option[String], fAttr18: Option[String], fAttr19: Option[String], fAttr20: Option[String],
      fAttr21: Option[String]
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

  override def selectAll(): Stream[IO, Event] =
    Fragment
      .const(s"""
        SELECT m.event_id, m.job_id, m.event_type, m.occurred_at, m.source, m.level,
               c.priority, c.submitted_by, c.queue_name, ${attrCols(22, "c.")},
               s.duration_ms, s.result_summary, s.output_size_bytes, ${attrCols(22, "s.")},
               f.attempt, f.error_message, f.stack_trace, ${attrCols(21, "f.")}
        FROM wide_events m
          LEFT JOIN wide_job_created c ON m.event_id = c.event_id
          LEFT JOIN wide_job_success s ON m.event_id = s.event_id
          LEFT JOIN wide_job_failed f ON m.event_id = f.event_id
      """)
      .query[AllRow]
      .stream
      .transact(xa)
      .map { r =>
        val body: EventBody = r.eventType match {
          case Event.JobCreatedType =>
            EventBody.JobCreated(
              r.cPriority.get, r.cSubmittedBy.get, r.cQueueName.get,
              r.cAttr1.get, r.cAttr2.get, r.cAttr3.get, r.cAttr4.get, r.cAttr5.get,
              r.cAttr6.get, r.cAttr7.get, r.cAttr8.get, r.cAttr9.get, r.cAttr10.get,
              r.cAttr11.get, r.cAttr12.get, r.cAttr13.get, r.cAttr14.get, r.cAttr15.get,
              r.cAttr16.get, r.cAttr17.get, r.cAttr18.get, r.cAttr19.get, r.cAttr20.get,
              r.cAttr21.get, r.cAttr22.get
            )
          case Event.JobSuccessType =>
            EventBody.JobSuccess(
              r.sDurationMs.get, r.sResultSummary.get, r.sOutputSizeBytes.get,
              r.sAttr1.get, r.sAttr2.get, r.sAttr3.get, r.sAttr4.get, r.sAttr5.get,
              r.sAttr6.get, r.sAttr7.get, r.sAttr8.get, r.sAttr9.get, r.sAttr10.get,
              r.sAttr11.get, r.sAttr12.get, r.sAttr13.get, r.sAttr14.get, r.sAttr15.get,
              r.sAttr16.get, r.sAttr17.get, r.sAttr18.get, r.sAttr19.get, r.sAttr20.get,
              r.sAttr21.get, r.sAttr22.get
            )
          case Event.JobFailedType =>
            EventBody.JobFailed(
              r.fAttempt.get, r.fErrorMessage.get, r.fStackTrace.get,
              r.fAttr1.get, r.fAttr2.get, r.fAttr3.get, r.fAttr4.get, r.fAttr5.get,
              r.fAttr6.get, r.fAttr7.get, r.fAttr8.get, r.fAttr9.get, r.fAttr10.get,
              r.fAttr11.get, r.fAttr12.get, r.fAttr13.get, r.fAttr14.get, r.fAttr15.get,
              r.fAttr16.get, r.fAttr17.get, r.fAttr18.get, r.fAttr19.get, r.fAttr20.get,
              r.fAttr21.get
            )
        }
        Event(r.eventId, r.jobId, r.eventType, r.occurredAt, r.source, r.level, body)
      }
}
