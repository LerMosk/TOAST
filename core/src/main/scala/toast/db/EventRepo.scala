package toast.db

import cats.effect.IO
import fs2.Stream
import toast.model.Event

trait EventRepo {
  def insertAll(events: Stream[IO, Event]): IO[Unit]
  def selectAll(): Stream[IO, Event]
}
