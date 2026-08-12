package toast.wide.db

import cats.effect.IO
import fs2.Stream
import toast.wide.model.Event

trait EventRepo {
  def insertAll(events: Stream[IO, Event]): IO[Unit]
  def selectAll(): Stream[IO, Event]
}
