package toast.size

import cats.syntax.all._
import fs2.Stream
import org.typelevel.doobie._
import org.typelevel.doobie.implicits._
import cats.effect.IO
import org.typelevel.doobie.Transactor

object SizeReporter {
  private final case class TableSize(name: String, heapBytes: Long, toastBytes: Long, totalBytes: Long)
  private object TableSize {
    def sum(name: String, ts: List[TableSize]): TableSize = {
      val (heapBytes, toastBytes, totalBytes) = ts.foldMap(ts => (ts.heapBytes, ts.toastBytes, ts.totalBytes))
      TableSize(name, heapBytes, toastBytes, totalBytes)
    }
  }

  def sizeReport(xa: Transactor[IO], scalarTables: List[String], jsonbTables: List[String]): IO[Unit] =
    for {
      scalarSizes <- scalarTables.traverse(tableSize(xa, _))
      jsonbSizes <- jsonbTables.traverse(tableSize(xa, _))
      _ <- IO.println("=== Storage size ===")
      _ <- scalarSizes.traverse_(printSize)
      _ <- printSize(TableSize.sum("scalar total", scalarSizes))
      _ <- jsonbSizes.traverse_(printSize)
    } yield ()

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

  private def bytesToMb(bytes: Long): Double = bytes / (1024.0 * 1024.0)

  private def printSize(ts: TableSize): IO[Unit] =
    IO.println(
      f"${ts.name}%-28s heap: ${bytesToMb(ts.heapBytes)}%8.2f MB   " +
        f"toast: ${bytesToMb(ts.toastBytes)}%8.2f MB   " +
        f"total: ${bytesToMb(ts.totalBytes)}%8.2f MB"
    )

}
