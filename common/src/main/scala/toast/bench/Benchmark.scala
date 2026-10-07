package toast.bench

import cats.Monoid
import cats.effect.IO
import cats.syntax.all._
import fs2.Stream
import toast.db.EventRepo

import scala.concurrent.duration.FiniteDuration

object Benchmark {
  private case class Results(writes: Map[String, Vector[FiniteDuration]], reads: Map[String, Vector[FiniteDuration]]) {
    def addWrite(label: String, result: FiniteDuration): Results =
      Results(add(label, result, writes), reads)

    def addRead(label: String, result: FiniteDuration): Results =
      Results(writes, add(label, result, reads))

    def print(labels: List[String]): IO[Unit] = {
      IO.println(s"=== Insert (create) benchmark ===") *>
        print(labels, writes) *>
        IO.println(s"=== Full table read benchmark ===") *>
        print(labels, reads)
    }


    private def print(labels: List[String], results: Map[String, Vector[FiniteDuration]]): IO[Unit] =
      labels.traverse_ { label =>
        val timings = results(label)
        val ms = timings.map(_.toMillis).sum/timings.size
        IO.println(f"$label%-24s $ms%6d ms")
      }


    private def add(label: String, result: FiniteDuration, results: Map[String, Vector[FiniteDuration]]): Map[String, Vector[FiniteDuration]] =
      results.updatedWith(label)(_.map(_ :+ result).getOrElse(Vector(result)).some)
  }
  private object Results {
    implicit val monoid: Monoid[Results] = new Monoid[Results]{
      def empty: Results = Empty

      def combine(x: Results, y: Results): Results= Results(x.writes |+| y.writes, x.reads |+| y.reads)
    }
    val Empty: Results = Results(Map.empty, Map.empty)

    def fromWrite(label:String, result: FiniteDuration): Results =
      Results(Map(label -> Vector(result)), Map.empty)
  }

  def benchmark[Event](
                        repos: List[(String, EventRepo[Event])],
                        events: List[Event],
                        runs: Int = 5
                      ): IO[Unit] = {
    IO.println(s"=== Full table write/read benchmark (n=${events.size}, avg of $runs runs) ===") *>
    List.range(0, runs).foldMapM{_ =>
      repos.foldMapM { case (label, repo) =>
        repo.truncate() *>
        IO.println(s"Inserting $label") *>
        repo.insertAll(Stream.emits(events).covary[IO]).timed.map { case (d, _) =>
          Results.fromWrite(label, d)
        } <* repo.analyse()
      }.flatMap { results =>
        repos.foldM(results) { case (acc, (label, repo)) =>
          IO.println(s"Selecting $label") *>
          repo.selectAll().compile.count.timed
            .flatMap { case (d, count) =>
              IO.raiseUnless(count == events.size)(
                new RuntimeException(s"$label read count mismatch: expected ${events.size}, got $count")
              ).as(acc.addRead(label, d))
            }
        }
      }
    }.flatMap(_.print(repos.map(_._1)))
  }
}
