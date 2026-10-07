package toast.db

import cats.effect.IO
import fs2.Stream

trait EventRepo[Event] {
  def insertAll(events: Stream[IO, Event]): IO[Unit]
  def selectAll(): Stream[IO, Event]
  def truncate(): IO[Unit]
  def analyse(): IO[Unit]
}

object EventRepo {
  val BatchSize: Int = 5000
}