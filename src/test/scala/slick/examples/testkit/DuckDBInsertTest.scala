package slick.examples.testkit

import com.typesafe.slick.testkit.tests.InsertTest

class DuckDBInsertTest extends InsertTest {

  import tdb.profile.api.*

  // DuckDB doesn't truncate data because it doesn't enforce the maximum length at all.
  // From the docs on VARCHAR(n): "The maximum length n has no effect and is only provided for compatibility"
  // TODO: implement creating a CHECK constraint when using a Slick def like:
  //       `def name = column[String]("name", O.Length(2))`
  override def testInsertAndUpdateShouldNotTruncateData = DBIO.seq()
}
