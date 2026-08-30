package toast.db

import cats.effect.IO
import fs2.Stream

trait EventRepo[Event] {
  def insertAll(events: Stream[IO, Event]): IO[Unit]
  def selectAll(): Stream[IO, Event]
}
