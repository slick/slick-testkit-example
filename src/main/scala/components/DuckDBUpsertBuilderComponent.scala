package components

import slick.SlickException
import slick.ast.ColumnOption.{AutoInc, PrimaryKey}
import slick.ast.{FieldSymbol, Insert, Node, Select}
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

    private lazy val (nonPkAutoIncSyms, insertingSyms) = syms.toSeq.partition(sym => isAutoInc(sym) && !isPrimaryKey(sym))
    override lazy val (pkSyms, softSyms) = insertingSyms.partition(sym => isPrimaryKey(sym) || isPartOfFuncDefinedPk(sym))

    override def buildInsert: InsertBuilderResult = {
      val insertSql = buildUpsertSql(tableName, insertingSyms, softSyms)
      new InsertBuilderResult(table, insertSql, syms)
    }

    override def transformMapping(n: Node) = reorderColumns(n,
      insertingSyms ++ nonPkAutoIncSyms ++ nonPkAutoIncSyms ++ nonPkAutoIncSyms)

    private def buildUpsertSql(tableName: String, insertingSyms: Seq[FieldSymbol], softSyms: Seq[FieldSymbol]): String = {
      val insertingColNames = insertingSyms.map(fs => quoteIdentifier(fs.name)).mkString("(", ", ", ")")
      val insertingValuePlaceholders = insertingSyms.map(_ => "?").mkString("(", ", ", ")")
      val primaryKeyNames = pkSyms.map(fs => quoteIdentifier(fs.name)).mkString("(", ",", ")")
      val softNames = softSyms.map(fs => quoteIdentifier(fs.name))
      val padding = if (nonPkAutoIncSyms.isEmpty) "" else "where ? is null or ?=?" // padding for JDBC parameters
      val doUpdateOrNothing = if (softNames.isEmpty) "DO NOTHING" else
        "DO UPDATE SET " + softNames.map(n => s"$n=EXCLUDED.$n").mkString(",")
      s"""
         |INSERT INTO $tableName $insertingColNames VALUES $insertingValuePlaceholders
         |ON CONFLICT $primaryKeyNames $padding $doUpdateOrNothing
         |""".stripMargin
    }

    private def isAutoInc(sym: FieldSymbol) = sym.options.contains(AutoInc)

    private def isPrimaryKey(sym: FieldSymbol) = sym.options.contains(PrimaryKey)

    private def isPartOfFuncDefinedPk(sym: FieldSymbol) = {
      val funcDefinedPKs = table.profileTable.asInstanceOf[Table[?]].primaryKeys
      funcDefinedPKs.exists(pk => pk.columns.collect {
        case Select(_, f: FieldSymbol) => f
      }.exists(_.name == sym.name))
    }


  }


  class DuckDBUpsertBuilderV1(insert: Insert) extends UpsertBuilder(insert) {
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
