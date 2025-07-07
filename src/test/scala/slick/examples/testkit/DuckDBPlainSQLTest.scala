package slick.examples.testkit

import com.typesafe.slick.testkit.tests.PlainSQLTest
import slick.jdbc.{GetResult, SetParameter}

class DuckDBPlainSQLTest extends PlainSQLTest {

  import tdb.profile.api.*

  private val valueWhenReturnsNothing = -1

  override def testInterpolation = ifCap(tcap.plainSql) {
    def userForID(id: Int) = sql"select id, name from USERS where id = $id".as[User]
    def userForIdAndName(id: Int, name: String) = sql"select id, name from USERS where id = $id and name = $name".as[User]

    val foo = "foo"
    val s1 = sql"select id from USERS where name = ${"szeiger"}".as[Int]
    val s2 = sql"select id from USERS where name = '#${"guest"}'".as[Int]
    val s3 = sql"select id from USERS where name = $foo".as[Int]
    val s4 = sql"select id from USERS where name = '#$foo'".as[Int]
    s1.statements.head shouldBe "select id from USERS where name = ?"
    s2.statements.head shouldBe "select id from USERS where name = 'guest'"
    s3.statements.head shouldBe "select id from USERS where name = ?"
    s4.statements.head shouldBe "select id from USERS where name = 'foo'"

    val create: DBIO[Int] = sqlu"create table USERS(ID int not null primary key, NAME varchar(255))"

    seq(
      create.map(_ shouldBe valueWhenReturnsNothing),
      DBIO.fold((for {
        (id, name) <- List((1, "szeiger"), (0, "admin"), (2, "guest"), (3, "foo"))
      } yield sqlu"insert into USERS values ($id, $name)"), 0)(_ + _) shouldYield(4),
      sql"select id from USERS".as[Int] shouldYield Set(0,1,2,3), //TODO Support `to` in Plain SQL Actions
      userForID(2).head shouldYield User(2,"guest"), //TODO Support `head` and `headOption` in Plain SQL Actions
      s1 shouldYield List(1),
      s2 shouldYield List(2),
      userForIdAndName(2, "guest").head shouldYield User(2,"guest"), //TODO Support `head` and `headOption` in Plain SQL Actions
      userForIdAndName(2, "foo").headOption shouldYield None //TODO Support `head` and `headOption` in Plain SQL Actions
    )
  }

  override def testEdgeTypes = ifCap(tcap.plainSql) {
    object Const
    case class Weird(u: Unit, x: (Int, String), c: Const.type)

    implicit val getC = GetResult.const[Const.type](Const)
    implicit val get: GetResult[Weird] = GetResult[Weird](r => new Weird(r.<<[Unit], r.<<, r.<<[Const.type]))

    implicit val setC = SetParameter.const[Const.type]
    implicit val setU = SetParameter[User]{(u, p) => println(u); p.setInt(u.id); p.setString(u.name)}
    implicit val setT = setU.contramap((User.apply _).tupled)
    implicit val set  = SetParameter[Weird]{(w, p) =>
      SetParameter.SetUnit(w.u, p)
      setT(w.x, p)
      setC(w.c, p)
    }

    val w = Weird((), (3, "Ah"), Const)

    val create =
      sqlu"create table USERS2(ID int not null primary key, NAME varchar(255))".map(_ shouldBe valueWhenReturnsNothing)

    def testSet =
      sqlu"insert into USERS2 values (${w.x._1}, ${w.x._2})".map(_ shouldBe 1)
    //sqlu"insert into USERS2 values ($w)".map(_ shouldBe 1)

    def testGet =
      sql"select id, name from USERS2 where id = ${w.x._1}".as[Weird].map(_.head shouldBe w)

    seq(create, testSet, testGet)
  }
}
