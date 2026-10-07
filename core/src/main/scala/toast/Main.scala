package toast

import cats.effect.{ExitCode, IO, IOApp}
import fs2.Stream
import toast.bench.Benchmark
import toast.config.Config
import toast.db.{Db, EventRepo, JsonbEventRepo, ScalarEventRepo}
import toast.generator.EventGenerator
import toast.size.SizeReporter

object Main extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    val config = Config.load()
    val db = new Db(config)

    db.transactor.use { xa =>
      val scalarRepo = new ScalarEventRepo(xa, config, EventRepo.BatchSize)
      val jsonbRepo = new JsonbEventRepo(xa, db.jsonbTable, EventRepo.BatchSize)
      val jsonbExternalRepo = new JsonbEventRepo(xa, db.jsonbExternalTable, EventRepo.BatchSize)
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
        _ <- Benchmark.benchmark(repos, events)
        _ <- SizeReporter.sizeReport(xa, db.scalarTables, db.jsonbTables)
      } yield ExitCode.Success
    }
  }
}
