package slick.examples.testkit

import com.typesafe.slick.testkit.tests.InsertTest
import slick.jdbc.JdbcCapabilities

import scala.util.Failure

class DuckDBInsertTest extends InsertTest {

  import tdb.profile.api.*

  // DuckDB doesn't truncate data because it doesn't enforce the maximum length at all.
  // From the docs on VARCHAR(n): "The maximum length n has no effect and is only provided for compatibility"
  // TODO: implement creating a CHECK constraint when using a Slick def like:
  //       `def name = column[String]("name", O.Length(2))`
  override def testInsertAndUpdateShouldNotTruncateData = DBIO.seq()

  override def testInsertOrUpdateAll = {
    class T(tag: Tag) extends Table[(Int, String)](tag, "insert_or_update") {
      def id = column[Int]("id", O.PrimaryKey)
      def name = column[String]("name")
      def * = (id, name)
      def ins = (id, name)
    }
    val ts = TableQuery[T]
    def prepare = DBIO.seq(ts.schema.create, ts ++= Seq((1, "a"), (2, "b")))
    if (tdb.capabilities.contains(JdbcCapabilities.insertOrUpdate)) {
      for {
        _ <- prepare
        // We override this test purely to set the affected rows to 2 instead of 3.
        // Some JDBC drivers count an update to a row as 2 affected rows instead of 1.
        // Maybe this is because they count an update as 1 delete operation and 1 insert operation.
        // The default Slick tests assume this behavior.
        // DuckDB counts an update as only one affected row, hence the override.
        _ <- ts.insertOrUpdateAll(Seq((3, "c"), (1, "d"))).map(_.foreach(_ shouldBe 2))
        _ <- ts.sortBy(_.id).result.map(_ shouldBe Seq((1, "d"), (2, "b"), (3, "c")))
      } yield ()
    } else {
      for {
        _ <- prepare
        _ <- ts.insertOrUpdateAll(Seq((3, "c"), (1, "d")))
      } yield ()
    }.asTry.map {
      case Failure(exception) => exception.isInstanceOf[SlickException] shouldBe true
      case _ => throw new RuntimeException("Should insertOrUpdateAll is not supported for this profile.")
    }
  }

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
