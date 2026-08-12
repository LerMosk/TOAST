package toast

import cats.effect.{ExitCode, IO, IOApp}
import fs2.Stream
import toast.bench.Benchmark
import toast.config.Config
import toast.db.{Db, JsonbEventRepo, ScalarEventRepo}
import toast.generator.EventGenerator

object Main extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    val config = Config.load()
    val db = new Db(config)

    db.transactor.use { xa =>
      val scalarRepo = new ScalarEventRepo(xa, config)
      val jsonbRepo = new JsonbEventRepo(xa, db.jsonbTable)
      val jsonbExternalRepo = new JsonbEventRepo(xa, db.jsonbExternalTable)
      val repos = List(
        s"scalar (${db.scalarTables.size} tables)" -> scalarRepo,
        "events_jsonb (extended)" -> jsonbRepo,
        "events_jsonb (external)" -> jsonbExternalRepo
      )

      for {
        _ <- IO.println(s"Config: $config")
        _ <- IO.println(s"Generating ${config.generateEvents} events...")
        events = EventGenerator.generate(config.generateEvents, config)
        _ <- IO.println("Recreating schema...")
        _ <- db.recreateSchema(xa)
        _ <- Benchmark.insertBenchmarkReport(repos, Stream.emits(events).covary[IO], config.generateEvents.toLong)
        _ <- IO.println("Analyzing tables...")
        _ <- db.analyze(xa)
        _ <- Benchmark.sanityCheck(repos, events)
        _ <- Benchmark.sizeReport(xa, db)
        _ <- Benchmark.readBenchmarkReport(repos, config.generateEvents.toLong)
      } yield ExitCode.Success
    }
  }
}
