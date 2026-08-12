package toast.model

import java.time.Instant
import java.util.UUID
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

final case class Param(name: String, value: String)

object Param {
  implicit val encoder: Encoder[Param] = deriveEncoder
  implicit val decoder: Decoder[Param] = deriveDecoder
}

sealed trait EventBody

object EventBody {
  final case class JobCreated(
      priority: Int,
      submittedBy: String,
      queueName: String,
      params: List[Param]
  ) extends EventBody

  final case class JobInProgress(
      workerId: String,
      attempt: Int,
      progressPercent: Int
  ) extends EventBody

  final case class JobSuccess(
      durationMs: Long,
      resultSummary: String,
      outputSizeBytes: Long
  ) extends EventBody

  final case class JobFailed(
      attempt: Int,
      errorMessage: String,
      stackTrace: String
  ) extends EventBody

  implicit val jobCreatedEncoder: Encoder[JobCreated] = deriveEncoder
  implicit val jobCreatedDecoder: Decoder[JobCreated] = deriveDecoder
  implicit val jobInProgressEncoder: Encoder[JobInProgress] = deriveEncoder
  implicit val jobInProgressDecoder: Decoder[JobInProgress] = deriveDecoder
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
  val JobInProgressType = "JobInProgress"
  val JobSuccessType = "JobSuccess"
  val JobFailedType = "JobFailed"

  def eventTypeOf(body: EventBody): String = body match {
    case _: EventBody.JobCreated    => JobCreatedType
    case _: EventBody.JobInProgress => JobInProgressType
    case _: EventBody.JobSuccess    => JobSuccessType
    case _: EventBody.JobFailed     => JobFailedType
  }
}
