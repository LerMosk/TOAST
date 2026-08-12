package toast.generator

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import scala.util.Random
import toast.config.Config
import toast.model.{Event, EventBody, Param}

object EventGenerator {

  private val sources = Vector("scheduler", "worker-pool", "api-gateway", "queue-consumer")
  private val queues = Vector("default", "emails", "reports", "billing", "video-encode")
  private val workers = Vector("worker-1", "worker-2", "worker-3", "worker-4", "worker-5")
  private val paramKeys = Vector("region", "tenant", "retries", "format", "priority-hint", "locale")
  private val errorMessages = Vector(
    "connection timed out",
    "out of memory",
    "invalid input payload",
    "downstream service unavailable",
    "unexpected null reference"
  )

  private def randomLevel(rnd: Random): String = {
    val r = rnd.nextDouble()
    if (r < 0.85) "INFO" else if (r < 0.97) "WARN" else "ERROR"
  }

  private def randomString(rnd: Random, length: Int): String =
    rnd.alphanumeric.take(length).mkString

  private def randomParams(rnd: Random, maxParams: Int): List[Param] = {
    val n = rnd.nextInt(maxParams + 1)
    (1 to n).map { _ =>
      val key = paramKeys(rnd.nextInt(paramKeys.length))
      Param(key, randomString(rnd, 100))
    }.toList
  }

  private def randomStackTrace(rnd: Random): String = {
    val frameCount = 40 + rnd.nextInt(150)
    val header = s"java.lang.RuntimeException: ${errorMessages(rnd.nextInt(errorMessages.length))}\n"
    val frames = (1 to frameCount).map { i =>
      s"\tat com.toast.jobs.Worker$$anon$$${rnd.nextInt(999)}.step$i(Worker.scala:${100 + rnd.nextInt(900)})"
    }
    header + frames.mkString("\n")
  }

  private def randomJobCreated(rnd: Random, maxParams: Int): EventBody.JobCreated =
    EventBody.JobCreated(
      priority = rnd.nextInt(10),
      submittedBy = s"user-${rnd.nextInt(500)}",
      queueName = queues(rnd.nextInt(queues.length)),
      params = randomParams(rnd, maxParams)
    )

  private def randomJobInProgress(rnd: Random): EventBody.JobInProgress =
    EventBody.JobInProgress(
      workerId = workers(rnd.nextInt(workers.length)),
      attempt = 1 + rnd.nextInt(3),
      progressPercent = rnd.nextInt(101)
    )

  private def randomJobSuccess(rnd: Random): EventBody.JobSuccess =
    EventBody.JobSuccess(
      durationMs = 100L + rnd.nextInt(60000),
      resultSummary = s"processed ${1 + rnd.nextInt(500)} records",
      outputSizeBytes = 1024L + rnd.nextInt(1024 * 1024)
    )

  private def randomJobFailed(rnd: Random): EventBody.JobFailed =
    EventBody.JobFailed(
      attempt = 1 + rnd.nextInt(5),
      errorMessage = errorMessages(rnd.nextInt(errorMessages.length)),
      stackTrace = randomStackTrace(rnd)
    )

  // Relative weights match the original fixed 30/30/25/15 split; disabled variants are
  // simply left out, so whichever remain are picked proportionally among themselves.
  private def randomBody(rnd: Random, config: Config): EventBody = {
    val options: List[(Double, Random => EventBody)] =
      (if (config.enableJobCreated) List(0.30 -> ((r: Random) => randomJobCreated(r, config.maxParams))) else Nil) ++
        (if (config.enableJobInProgress) List(0.30 -> randomJobInProgress _) else Nil) ++
        (if (config.enableJobSuccess) List(0.25 -> randomJobSuccess _) else Nil) ++
        (if (config.enableJobFailed) List(0.15 -> randomJobFailed _) else Nil)

    val total = options.map(_._1).sum
    val target = rnd.nextDouble() * total

    def pick(remaining: List[(Double, Random => EventBody)], acc: Double): EventBody =
      remaining match {
        case (weight, gen) :: rest =>
          if (target < acc + weight) gen(rnd) else pick(rest, acc + weight)
        case Nil =>
          throw new IllegalStateException("no event types enabled (Config validates this, so this is unreachable)")
      }

    pick(options, 0.0)
  }

  def generate(n: Int, config: Config, seed: Long = 42L): List[Event] = {
    val rnd = new Random(seed)
    val now = Instant.now().truncatedTo(ChronoUnit.MICROS)
    (1 to n).map { _ =>
      val body = randomBody(rnd, config)
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
