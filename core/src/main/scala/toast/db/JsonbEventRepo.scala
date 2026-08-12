package toast.db

import cats.effect.IO
import fs2.Stream
import org.typelevel.doobie._
import org.typelevel.doobie.implicits._
import org.typelevel.doobie.postgres.implicits._
import org.typelevel.doobie.postgres.circe.jsonb.implicits._
import toast.model.{Event, EventBody}

final class JsonbEventRepo(xa: Transactor[IO]) extends EventRepo {

  private val batchSize = 10000

  private implicit val eventBodyGet: Get[EventBody] = pgDecoderGetT[EventBody]
  private implicit val eventBodyPut: Put[EventBody] = pgEncoderPutT[EventBody]

  private type Row = (java.util.UUID, java.util.UUID, String, java.time.Instant, String, String, EventBody)

  private def toRow(e: Event): Row =
    (e.eventId, e.jobId, e.eventType, e.occurredAt, e.source, e.level, e.body)

  private val insert =
    Update[Row](
      "INSERT INTO events_jsonb (event_id, job_id, event_type, occurred_at, source, level, body) VALUES (?, ?, ?, ?, ?, ?, ?)"
    )

  override def insertAll(events: Stream[IO, Event]): IO[Unit] =
    events.chunkN(batchSize).evalMap(chunk => insert.updateMany(chunk.toList.map(toRow)).transact(xa).void).compile.drain

  override def selectAll(): Stream[IO, Event] =
    sql"SELECT event_id, job_id, event_type, occurred_at, source, level, body FROM events_jsonb"
      .query[Row]
      .stream
      .transact(xa)
      .map { case (eventId, jobId, eventType, occurredAt, source, level, body) =>
        Event(eventId, jobId, eventType, occurredAt, source, level, body)
      }
}
