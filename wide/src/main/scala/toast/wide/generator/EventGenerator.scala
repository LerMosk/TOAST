package toast.wide.generator

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import scala.util.Random
import toast.wide.model.{Event, EventBody}

object EventGenerator {

  private val sources = Vector("scheduler", "worker-pool", "api-gateway", "queue-consumer")
  private val queues = Vector("default", "emails", "reports", "billing", "video-encode")

  private def randomLevel(rnd: Random): String = {
    val r = rnd.nextDouble()
    if (r < 0.85) "INFO" else if (r < 0.97) "WARN" else "ERROR"
  }

  private def randomAttr(rnd: Random): String = {
    val length = 40 + rnd.nextInt(100)
    rnd.alphanumeric.take(length).mkString
  }

  private def randomJobCreated(rnd: Random): EventBody.JobCreated =
    EventBody.JobCreated(
      priority = rnd.nextInt(10),
      submittedBy = s"user-${rnd.nextInt(500)}",
      queueName = queues(rnd.nextInt(queues.length)),
      attr1 = randomAttr(rnd),
      attr2 = randomAttr(rnd),
      attr3 = randomAttr(rnd),
      attr4 = randomAttr(rnd),
      attr5 = randomAttr(rnd),
      attr6 = randomAttr(rnd),
      attr7 = randomAttr(rnd),
      attr8 = randomAttr(rnd),
      attr9 = randomAttr(rnd),
      attr10 = randomAttr(rnd),
      attr11 = randomAttr(rnd),
      attr12 = randomAttr(rnd),
      attr13 = randomAttr(rnd),
      attr14 = randomAttr(rnd),
      attr15 = randomAttr(rnd),
      attr16 = randomAttr(rnd),
      attr17 = randomAttr(rnd),
      attr18 = randomAttr(rnd),
      attr19 = randomAttr(rnd),
      attr20 = randomAttr(rnd),
      attr21 = randomAttr(rnd),
      attr22 = randomAttr(rnd)
    )

  private def randomJobSuccess(rnd: Random): EventBody.JobSuccess =
    EventBody.JobSuccess(
      durationMs = 100L + rnd.nextInt(60000),
      resultSummary = s"processed ${1 + rnd.nextInt(500)} records",
      outputSizeBytes = 1024L + rnd.nextInt(1024 * 1024),
      attr1 = randomAttr(rnd),
      attr2 = randomAttr(rnd),
      attr3 = randomAttr(rnd),
      attr4 = randomAttr(rnd),
      attr5 = randomAttr(rnd),
      attr6 = randomAttr(rnd),
      attr7 = randomAttr(rnd),
      attr8 = randomAttr(rnd),
      attr9 = randomAttr(rnd),
      attr10 = randomAttr(rnd),
      attr11 = randomAttr(rnd),
      attr12 = randomAttr(rnd),
      attr13 = randomAttr(rnd),
      attr14 = randomAttr(rnd),
      attr15 = randomAttr(rnd),
      attr16 = randomAttr(rnd),
      attr17 = randomAttr(rnd),
      attr18 = randomAttr(rnd),
      attr19 = randomAttr(rnd),
      attr20 = randomAttr(rnd),
      attr21 = randomAttr(rnd),
      attr22 = randomAttr(rnd)
    )

  private def randomBody(rnd: Random): EventBody = {
    val r = rnd.nextDouble()
    if (r < 0.50) randomJobCreated(rnd)
    else randomJobSuccess(rnd)
  }

  def generate(n: Int, seed: Long = 42L): List[Event] = {
    val rnd = new Random(seed)
    val now = Instant.now().truncatedTo(ChronoUnit.MICROS)
    (1 to n).map { _ =>
      val body = randomBody(rnd)
      Event(
        eventId = UUID.randomUUID(),
        jobId = UUID.randomUUID(),
        eventType = Event.eventTypeOf(body),
        occurredAt = now.minusSeconds(rnd.nextInt(60 * 60 * 24 * 30)),
        source = sources(rnd.nextInt(sources.length)),
        level = randomLevel(rnd),
        body = body
      )
    }.toList
  }
}
