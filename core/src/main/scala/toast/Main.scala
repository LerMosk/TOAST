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
      val jsonbRepo = new JsonbEventRepo(xa)

      for {
        _ <- IO.println(s"Config: $config")
        _ <- IO.println(s"Generating ${config.generateEvents} events...")
        events = EventGenerator.generate(config.generateEvents, config)
        _ <- IO.println("Recreating schema...")
        _ <- db.recreateSchema(xa)
        _ <- Benchmark.insertBenchmarkReport(scalarRepo, jsonbRepo, db, Stream.emits(events).covary[IO], config.generateEvents.toLong)
        _ <- IO.println("Analyzing tables...")
        _ <- db.analyze(xa)
        _ <- Benchmark.sanityCheck(scalarRepo, jsonbRepo, events)
        _ <- Benchmark.sizeReport(xa, db)
        _ <- Benchmark.readBenchmarkReport(scalarRepo, jsonbRepo, db, config.generateEvents.toLong)
      } yield ExitCode.Success
    }
  }
}
