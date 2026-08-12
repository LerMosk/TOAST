package toast.bench

import cats.effect.IO
import cats.syntax.all._
import fs2.Stream
import org.typelevel.doobie._
import org.typelevel.doobie.implicits._
import toast.db.{Db, EventRepo}
import toast.model.Event

object Benchmark {

  final case class TableSize(name: String, heapBytes: Long, toastBytes: Long, totalBytes: Long)
  object TableSize {
    def sum(name: String, ts: List[TableSize]): TableSize = {
      val (heapBytes, toastBytes, totalBytes) = ts.foldMap(ts => (ts.heapBytes, ts.toastBytes, ts.totalBytes))
      TableSize(name, heapBytes, toastBytes, totalBytes)
    }
  }

    def sizeReport(xa: Transactor[IO], db: Db): IO[Unit] =
    for {
      scalarSizes <- db.scalarTables.traverse(tableSize(xa, _))
      jsonbSize <- tableSize(xa, db.jsonbTable)
      _ <- IO.println("=== Storage size ===")
      _ <- scalarSizes.traverse_(printSize)
      _ <- printSize(TableSize.sum("scalar total", scalarSizes))
      _ <- printSize(jsonbSize)
    } yield ()

  private def bytesToMb(bytes: Long): Double = bytes / (1024.0 * 1024.0)

  private def tableSize(xa: Transactor[IO], table: String): IO[TableSize] =
    sql"""
      SELECT pg_relation_size(c.oid),
             COALESCE(pg_total_relation_size(c.reltoastrelid), 0),
             pg_total_relation_size(c.oid)
      FROM pg_class c
      WHERE c.relname = $table
    """.query[(Long, Long, Long)].unique.transact(xa).map { case (heap, toast, total) =>
      TableSize(table, heap, toast, total)
    }

  private def printSize(ts: TableSize): IO[Unit] = 
    IO.println(
        f"${ts.name}%-25s heap: ${bytesToMb(ts.heapBytes)}%8.2f MB   toast: ${bytesToMb(ts.toastBytes)}%8.2f MB   total: ${bytesToMb(ts.totalBytes)}%8.2f MB"
      )

  private def median(xs: List[Long]): Long = {
    val sorted = xs.sorted
    sorted(sorted.size / 2)
  }

  private def timeMillis[A](io: IO[A]): IO[(A, Long)] =
    for {
      start <- IO.realTime
      a <- io
      end <- IO.realTime
    } yield (a, (end - start).toMillis)

  // Counting via the stream (rather than materializing selectAll() into a List) keeps
  // this benchmark's own memory footprint independent of table size.
  def timeFullRead(repo: EventRepo, repeats: Int = 5): IO[(Long, Long)] =
    List.fill(repeats)(()).traverse { _ =>
      timeMillis(repo.selectAll().compile.count)
    }.map { results =>
      (results.head._1, median(results.map(_._2)))
    }

  def insertBenchmarkReport(
      scalarRepo: EventRepo,
      jsonbRepo: EventRepo,
      db: Db,
      events: Stream[IO, Event],
      expectedCount: Long
  ): IO[Unit] =
    for {
      _ <- IO.println("Inserting into scalar tables...")
      scalarResult <- timeMillis(scalarRepo.insertAll(events))
      _ <- IO.println("Inserting into events_jsonb...")
      jsonbResult <- timeMillis(jsonbRepo.insertAll(events))
      _ <- IO.println(s"=== Insert (create) benchmark (n=$expectedCount) ===")
      _ <- IO.println(f"scalar (${db.scalarTables.size} tables)      ${scalarResult._2}%6d ms")
      _ <- IO.println(f"events_jsonb           ${jsonbResult._2}%6d ms")
    } yield ()

  def readBenchmarkReport(scalarRepo: EventRepo, jsonbRepo: EventRepo, db: Db, expectedCount: Long): IO[Unit] =
    for {
      scalarResult <- timeFullRead(scalarRepo)
      jsonbResult <- timeFullRead(jsonbRepo)
      scalarCount = scalarResult._1
      scalarMs = scalarResult._2
      jsonbCount = jsonbResult._1
      jsonbMs = jsonbResult._2
      _ <- IO.raiseUnless(scalarCount == expectedCount)(
        new RuntimeException(s"scalar read count mismatch: expected $expectedCount, got $scalarCount")
      )
      _ <- IO.raiseUnless(jsonbCount == expectedCount)(
        new RuntimeException(s"jsonb read count mismatch: expected $expectedCount, got $jsonbCount")
      )
      _ <- IO.println(s"=== Full table read benchmark (n=$expectedCount, median of 5 runs) ===")
      _ <- IO.println(f"scalar (${db.scalarTables.size} tables)      $scalarMs%6d ms")
      _ <- IO.println(f"events_jsonb           $jsonbMs%6d ms")
    } yield ()

  // Two bounded passes per repo (count, then a small filtered sample) instead of
  // materializing the whole table into a Map, so this stays memory-safe at any n.
  def sanityCheck(scalarRepo: EventRepo, jsonbRepo: EventRepo, inserted: List[Event]): IO[Unit] = {
    val expectedCount = inserted.size.toLong
    val sample = inserted.take(20)
    val sampleIds = sample.map(_.eventId).toSet
    val sampleById = sample.map(e => e.eventId -> e).toMap

    def checkRepo(repo: EventRepo, label: String): IO[Unit] =
      for {
        count <- repo.selectAll().compile.count
        _ <- IO.raiseUnless(count == expectedCount)(
          new RuntimeException(s"$label count mismatch: expected $expectedCount, got $count")
        )
        matched <- repo.selectAll().filter(e => sampleIds.contains(e.eventId)).compile.toList
        _ <- IO.raiseUnless(matched.size == sampleIds.size)(
          new RuntimeException(s"$label: not all sample events found (found ${matched.size}/${sampleIds.size})")
        )
        _ <- matched.traverse_ { e =>
          IO.raiseUnless(sampleById.get(e.eventId).contains(e))(
            new RuntimeException(s"$label round-trip mismatch for ${e.eventId}")
          )
        }
      } yield ()

    for {
      _ <- checkRepo(scalarRepo, "scalar")
      _ <- checkRepo(jsonbRepo, "jsonb")
      _ <- IO.println("Sanity check passed: row counts and sample round-trips match.")
    } yield ()
  }
}
