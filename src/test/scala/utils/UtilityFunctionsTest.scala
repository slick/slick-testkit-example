package utils

import org.scalatest.funsuite.AnyFunSuiteLike
import utils.UtilityFunctions.extractTableNamesReferencedByForeignKeys

class UtilityFunctionsTest extends AnyFunSuiteLike {

  private val quotedKeyVariations = Map[String, String](
    "single key column" -> """("col1")""",
    "single key column with special character" -> """("col!§$%&/()[]@*'?")""",
    "single key column with escaped quotation mark" -> """("col\"1")""",
    "multiple key columns" -> """("col1", "col!§$%&/()[]@*'?2", "col\"3")""",
  )
  private val expectedTableName = "table_name"

  private val singleForeignKeyReference = quotedKeyVariations.view.mapValues(quotedKeys =>
    s"""foreign key$quotedKeys references "$expectedTableName"$quotedKeys""")

  private val multipleForeignKeyReferences = singleForeignKeyReference.view.mapValues(singleReference =>
    s"$singleReference, $singleReference")

  private val ddlFragmentsWithSingleForeignKeyReference = singleForeignKeyReference.view.mapValues(foreignKeyReference =>
    s"beginning of ddl, $foreignKeyReference, rest of ddl;"
  )

  private val ddlFragmentsWithMultipleForeignKeyReferences = multipleForeignKeyReferences.view.mapValues(foreignKeyReference =>
    s"beginning of ddl, $foreignKeyReference, rest of ddl;"
  )

  test("extractTableNameReferencedByForeignKeys should extract a table name from a single foreign key reference") {
    for (fragment <- ddlFragmentsWithSingleForeignKeyReference.values) {
      val extractedTableNames = extractTableNamesReferencedByForeignKeys(fragment)
      assert(extractedTableNames.size.equals(1))
      assert(extractedTableNames.head == expectedTableName)
    }
  }

  test("extractTableNameReferencedByForeignKeys should extract table names from multiple foreign key references") {
    for (fragment <- ddlFragmentsWithMultipleForeignKeyReferences.values) {
      val extractedTableNames = extractTableNamesReferencedByForeignKeys(fragment)
      assert(extractedTableNames.size.equals(2))
      assert(extractedTableNames === Seq(expectedTableName, expectedTableName))
    }
  }

}
