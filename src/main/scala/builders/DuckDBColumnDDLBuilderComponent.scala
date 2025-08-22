package builders

import builders.DuckDBColumnDDLBuilderComponent.getBackingSequenceName
import slick.ast.FieldSymbol
import slick.jdbc.JdbcProfile

trait DuckDBColumnDDLBuilderComponent {
  self: JdbcProfile =>

  /** Builder for the column definition parts of DDL statements.
    *
    * Customizes how column options are appended to the SQL statement,
    * particularly for auto-increment columns which require a backing sequence.
    */
  class DuckDBColumnDDLBuilder(column: FieldSymbol, table: Table[?])
      extends ColumnDDLBuilder(column) {

    // TODO: extract the sequence creation
    private lazy val backingSequenceName: String =
      getBackingSequenceName(table.tableName, column.name)

    override protected def appendOptions(sb: StringBuilder): Unit = {
      if (autoIncrement)
        sb append " DEFAULT " append "nextval('" append backingSequenceName append "')"
      if (defaultLiteral ne null) sb append " DEFAULT " append defaultLiteral
      if (notNull) sb append " NOT NULL"
      if (primaryKey) sb append " PRIMARY KEY"
      if (unique) sb append " UNIQUE"
    }
  }
}

// TODO: TableDDLBuilder has a similar function --> move to utils or change sequnce creation
object DuckDBColumnDDLBuilderComponent {
  def getBackingSequenceName(
      tableName: String,
      columnName: String
  ): String = s"${tableName}_${columnName}_seq"
}
