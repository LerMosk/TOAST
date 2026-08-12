package toast.wide.model

import java.time.Instant
import java.util.UUID
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

sealed trait EventBody

object EventBody {
  // Each variant carries a handful of meaningful fields plus a large number of flat
  // attrN: String fields (no Param/collection) so the total body size crosses the
  // ~2KB TOAST threshold as one unit, while every individual scalar column stays small.
  final case class JobCreated(
      priority: Int,
      submittedBy: String,
      queueName: String,
      attr1: String,
      attr2: String,
      attr3: String,
      attr4: String,
      attr5: String,
      attr6: String,
      attr7: String,
      attr8: String,
      attr9: String,
      attr10: String,
      attr11: String,
      attr12: String,
      attr13: String,
      attr14: String,
      attr15: String,
      attr16: String,
      attr17: String,
      attr18: String,
      attr19: String,
      attr20: String,
      attr21: String,
      attr22: String
  ) extends EventBody

  final case class JobSuccess(
      durationMs: Long,
      resultSummary: String,
      outputSizeBytes: Long,
      attr1: String,
      attr2: String,
      attr3: String,
      attr4: String,
      attr5: String,
      attr6: String,
      attr7: String,
      attr8: String,
      attr9: String,
      attr10: String,
      attr11: String,
      attr12: String,
      attr13: String,
      attr14: String,
      attr15: String,
      attr16: String,
      attr17: String,
      attr18: String,
      attr19: String,
      attr20: String,
      attr21: String,
      attr22: String
  ) extends EventBody

  final case class JobFailed(
      attempt: Int,
      errorMessage: String,
      stackTrace: String,
      attr1: String,
      attr2: String,
      attr3: String,
      attr4: String,
      attr5: String,
      attr6: String,
      attr7: String,
      attr8: String,
      attr9: String,
      attr10: String,
      attr11: String,
      attr12: String,
      attr13: String,
      attr14: String,
      attr15: String,
      attr16: String,
      attr17: String,
      attr18: String,
      attr19: String,
      attr20: String,
      attr21: String
  ) extends EventBody

  implicit val jobCreatedEncoder: Encoder[JobCreated] = deriveEncoder
  implicit val jobCreatedDecoder: Decoder[JobCreated] = deriveDecoder
  implicit val jobSuccessEncoder: Encoder[JobSuccess] = deriveEncoder
  implicit val jobSuccessDecoder: Decoder[JobSuccess] = deriveDecoder
  implicit val jobFailedEncoder: Encoder[JobFailed] = deriveEncoder
  implicit val jobFailedDecoder: Decoder[JobFailed] = deriveDecoder

  implicit val encoder: Encoder[EventBody] = deriveEncoder
  implicit val decoder: Decoder[EventBody] = deriveDecoder
}

final case class Event(
    eventId: UUID,
    jobId: UUID,
    eventType: String,
    occurredAt: Instant,
    source: String,
    level: String,
    body: EventBody
)

object Event {
  val JobCreatedType = "JobCreated"
  val JobSuccessType = "JobSuccess"
  val JobFailedType = "JobFailed"

  def eventTypeOf(body: EventBody): String = body match {
    case _: EventBody.JobCreated => JobCreatedType
    case _: EventBody.JobSuccess => JobSuccessType
    case _: EventBody.JobFailed  => JobFailedType
  }
}
