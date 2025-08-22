package builders

import slick.SlickException
import slick.ast.Insert
import slick.jdbc.{InsertBuilderResult, JdbcProfile}

trait DuckDBUpsertBuilderComponent {
  self: JdbcProfile =>

  /** Builder for UPSERT statements.
    *
    * We need to override base UpsertBuilder, because it's implemented using
    * `MERGE` which DuckDB doesn't support. This implementation uses DuckDB's
    * `INSERT ... ON CONFLICT` syntax instead.
    */
  class DuckDBUpsertBuilder(insert: Insert) extends UpsertBuilder(insert) {
    override def buildInsert: InsertBuilderResult = {
      if (pkNames.isEmpty) {
        throw new SlickException("Primary key required for insertOrUpdate")
      }
      val pkCols            = pkNames.mkString(", ")
      val updateAssignments = softNames
        .map(fs => s"$fs = EXCLUDED.$fs")
        .mkString(", ")
      val conflictAction    =
        if (updateAssignments.isEmpty) "do nothing"
        else "do update set " + updateAssignments
      val insertSql         =
        s"""insert into $tableName ${allNames.mkString("(", ", ", ")")}
           |values $allVars
           |on conflict ($pkCols)
           |$conflictAction
           |""".stripMargin.replaceAll("\n", " ")
      new InsertBuilderResult(table, insertSql, allFields) {
        override def buildMultiRowInsert(size: Int): String = {
          // Generate placeholders per row (e.g., `(?, ?, ?)` with allVars)
          val rowPlaceholder      = allVars
          // Create placeholders for all rows
          val multiRowPlaceholder =
            List.fill(size)(rowPlaceholder).mkString(", ")

          // Generate the multi-row insert SQL
          s"""insert into $tableName ${allNames.mkString("(", ", ", ")")}
             |values $multiRowPlaceholder
             |on conflict ($pkCols)
             |$conflictAction
             |""".stripMargin.replaceAll("\n", " ")
        }
      }
    }
  }
}
