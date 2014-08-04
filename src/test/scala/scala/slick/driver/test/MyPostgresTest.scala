package scala.slick.driver.test

import org.junit.runner.RunWith
import com.typesafe.slick.testkit.util.{ExternalJdbcTestDB, TestDB, DriverTest, Testkit}
import scala.slick.jdbc.ResultSetInvoker
import scala.slick.jdbc.GetResult._
import scala.slick.driver.MyPostgresDriver

@RunWith(classOf[Testkit])
class MyPostgresTest extends DriverTest(MyPostgresTest.tdb)

object MyPostgresTest {
  def tdb = new ExternalJdbcTestDB("mypostgres") {
    val driver = MyPostgresDriver
    override def getLocalTables(implicit session: profile.Backend#Session) = {
      val tables = ResultSetInvoker[(String,String,String, String)](
        _.conn.getMetaData().getTables("", "public", null, null))
      tables.list.filter(_._4.toUpperCase == "TABLE").map(_._3).sorted
    }
    override def getLocalSequences(implicit session: profile.Backend#Session) = {
      val tables = ResultSetInvoker[(String,String,String, String)](
        _.conn.getMetaData().getTables("", "public", null, null))
      tables.list.filter(_._4.toUpperCase == "SEQUENCE").map(_._3).sorted
    }
    override def capabilities =
      super.capabilities - TestDB.capabilities.jdbcMetaGetFunctions
  }
}
