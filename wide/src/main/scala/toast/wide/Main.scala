package toast.wide

import cats.effect.{ExitCode, IO, IOApp}
import fs2.Stream
import toast.wide.bench.Benchmark
import toast.wide.db.{Db, JsonbEventRepo, ScalarEventRepo}
import toast.wide.generator.EventGenerator

object Main extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    val n = args.headOption.flatMap(_.toIntOption).getOrElse(100000)

    Db.transactor.use { xa =>
      val scalarRepo = new ScalarEventRepo(xa)
      val jsonbRepo = new JsonbEventRepo(xa, Db.jsonbTable)
      val jsonbExternalRepo = new JsonbEventRepo(xa, Db.jsonbExternalTable)
      val repos = List(
        s"scalar (${Db.scalarTables.size} tables)" -> scalarRepo,
        "wide_events_jsonb (extended)" -> jsonbRepo,
        "wide_events_jsonb (external)" -> jsonbExternalRepo
      )

      for {
        _ <- IO.println(s"Generating $n events...")
        events = EventGenerator.generate(n)
        _ <- IO.println("Recreating schema...")
        _ <- Db.recreateSchema(xa)
        _ <- Benchmark.insertBenchmarkReport(repos, Stream.emits(events).covary[IO], n.toLong)
        _ <- IO.println("Analyzing tables...")
        _ <- Db.analyze(xa)
        _ <- Benchmark.sanityCheck(repos, events)
        _ <- Benchmark.sizeReport(xa)
        _ <- Benchmark.readBenchmarkReport(repos, n.toLong)
      } yield ExitCode.Success
    }
  }
}
