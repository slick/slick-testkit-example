package slick.driver.test

import org.junit.runner.RunWith
import com.typesafe.slick.testkit.util.{ExternalJdbcTestDB, TestDB, DriverTest, Testkit}
import scala.concurrent.ExecutionContext
import slick.jdbc.{ResultSetAction, ResultSetInvoker}
import slick.dbio.DBIO
import slick.jdbc.GetResult._
import slick.driver.MyPostgresDriver

@RunWith(classOf[Testkit])
class MyPostgresTest extends DriverTest(MyPostgresTest.tdb)

object MyPostgresTest {
  def tdb = new ExternalJdbcTestDB("mypostgres") {
    val driver = MyPostgresDriver
    override def localTables(implicit ec: ExecutionContext): DBIO[Vector[String]] =
      ResultSetAction[(String,String,String, String)](_.conn.getMetaData().getTables("", "public", null, null)).map { ts =>
        ts.filter(_._4.toUpperCase == "TABLE").map(_._3).sorted
      }
    override def getLocalSequences(implicit session: profile.Backend#Session) = {
      val tables = ResultSetInvoker[(String,String,String, String)](_.conn.getMetaData().getTables("", "public", null, null))
      tables.buildColl[List].filter(_._4.toUpperCase == "SEQUENCE").map(_._3).sorted
    }
    override def capabilities = super.capabilities - TestDB.capabilities.jdbcMetaGetFunctions
  }
}
