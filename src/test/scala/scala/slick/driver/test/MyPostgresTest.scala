package scala.slick.driver.test

import org.junit.runner.RunWith
import com.typesafe.slick.testkit.util.{TestDB, ExternalTestDB, DriverTest, Testkit}
import scala.slick.session.Session
import scala.slick.jdbc.ResultSetInvoker
import scala.slick.jdbc.GetResult._
import scala.slick.driver.MyPostgresDriver

@RunWith(classOf[Testkit])
class MyPostgresTest extends DriverTest(MyPostgresTest.tdb)

object MyPostgresTest {
  def tdb(cname: String) = new ExternalTestDB("mypostgres", MyPostgresDriver) {
    override def getLocalTables(implicit session: Session) = {
      val tables = ResultSetInvoker[(String,String,String, String)](_.conn.getMetaData().getTables("", "public", null, null))
      tables.list.filter(_._4.toUpperCase == "TABLE").map(_._3).sorted
    }
    override def getLocalSequences(implicit session: Session) = {
      val tables = ResultSetInvoker[(String,String,String, String)](_.conn.getMetaData().getTables("", "public", null, null))
      tables.list.filter(_._4.toUpperCase == "SEQUENCE").map(_._3).sorted
    }
    override lazy val capabilities = driver.capabilities + TestDB.plainSql
  }
}
