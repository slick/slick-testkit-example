package slick.examples.testkit

import com.typesafe.slick.testkit.tests.InsertTest

class DuckDBInsertTest extends InsertTest {

  import tdb.profile.api.*

  // DuckDB doesn't truncate data because it doesn't enforce the maximum length at all.
  // From the docs on VARCHAR(n): "The maximum length n has no effect and is only provided for compatibility"
  // TODO: implement creating a CHECK constraint when using a Slick def like:
  //       `def name = column[String]("name", O.Length(2))`
  override def testInsertAndUpdateShouldNotTruncateData = DBIO.seq()

  override def testInsertOrUpdatePlainWithFuncDefinedPK: DBIOAction[Unit, NoStream, Effect.All] = {
    class T(tag: Tag) extends Table[(Int, String)](tag, "t_merge3") {
      def id = column[Int]("id")
      def name = column[String]("name")
      def * = (id, name)
      def ins = (id, name)
      def pk = primaryKey("t_merge_pk_a", id)
    }
    val ts = TableQuery[T]

    for {
      _ <- ts.schema.create
      _ <- ts ++= Seq((1, "a"), (2, "b"))
      _ <- ts.insertOrUpdate((3, "c")).map(_ shouldBe 1)
      _ <- ts.insertOrUpdate((1, "d")).map(_ shouldBe 1)
      _ <- ts.sortBy(_.id).result.map(_ shouldBe Seq((1, "d"), (2, "b"), (3, "c")))
    } yield ()
  }
}
