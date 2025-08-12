package duckdbslick

import slick.SlickException
import slick.ast.*
import slick.ast.ColumnOption.AutoInc
import slick.basic.Capability
import slick.compiler.CompilerState
import slick.dbio.DBIO
import slick.examples.testkit.DuckDBProfile.getBackingSequenceName
import slick.jdbc.JdbcActionComponent.MultipleRowsPerStatementSupport
import slick.jdbc.meta.MTable
import slick.jdbc.{InsertBuilderResult, JdbcCapabilities, JdbcProfile}
import slick.lifted.{ForeignKey, PrimaryKey}
import slick.util.QueryInterpolator.queryInterpolator

import java.sql.*
import java.util.UUID
import javax.sql.rowset.serial.SerialBlob
import scala.concurrent.ExecutionContext

/** Slick profile for DuckDB.
  *
  * This profile extends the standard JDBC profile with DuckDB-specific
  * capabilities. It provides a foundation for using Slick with DuckDB
  * databases.
  *
  * DuckDB is an in-process SQL OLAP database management system designed
  * to be fast and efficient for analytical queries.
  */
trait DuckDBProfile extends JdbcProfile with MultipleRowsPerStatementSupport {

  override def defaultTables(implicit
      ec: ExecutionContext
  ): DBIO[Seq[MTable]] = {
    MTable.getTables(None, None, Some("%"), Some(Seq("BASE TABLE")))
  }

  override protected def computeCapabilities: Set[Capability] = {
    val unsupportedCapabilities = Set(
      // Definitely not supported by DuckDB
      JdbcCapabilities.mutable,   // The result type of DuckDB's JDBC driver is immutable
      JdbcCapabilities.forUpdate, // No SELECT ... FOR UPDATE locking in DuckDB
      JdbcCapabilities.nullableNoDefault,

      // Requires missing `DuckDBDatabaseMetaData.getImportedKeys` driver implementation.
      JdbcCapabilities.createModel,

      // The `O.AutoInc` column option doesn't work because DuckDB's JDBC doesn't fully
      // implement the key generation or metadata generation Slick expects
      JdbcCapabilities.forceInsert,

      // Slick queries can be configured to return values such as the insert key using
      // the `returning` method. However, the DuckDB JDBC driver doesn't implement the
      // necessary `prepareStatement` methods on the `DuckDBConnection` class.
      // As a result, `returning` cannot be used with DuckDB at all.
      JdbcCapabilities.returnInsertKey,
      JdbcCapabilities.returnMultipleInsertKey,
      JdbcCapabilities.returnInsertOther
    )
    super.computeCapabilities -- unsupportedCapabilities
  }

  /** DuckDB-specific API.
    *
    * This API extends the standard JDBC API with DuckDB-specific functionality.
    * Future enhancements could include:
    *   - Support for DuckDB-specific data types
    *   - Support for DuckDB-specific SQL functions
    *   - Support for DuckDB-specific query features
    */
  override val api: DuckDBApi = new DuckDBApi

  class DuckDBApi extends JdbcAPI {
    // DuckDB-specific API methods can be added here
  }

  /** DuckDB-specific type mappings.
    *
    * This class extends the standard JDBC type mappings with DuckDB-specific
    * type mappings. Future enhancements could include:
    *   - Support for DuckDB-specific data types like HUGEINT
    *   - Support for DuckDB-specific array types
    *   - Support for DuckDB-specific JSON operations
    *   - Support for DuckDB-specific time/date functions
    */
  override val columnTypes: DuckDBJdbcTypes = new DuckDBJdbcTypes

  class DuckDBJdbcTypes extends JdbcTypes {

    override val blobJdbcType: BlobJdbcType = new DuckDBBlobJdbcType
    private class DuckDBBlobJdbcType extends super.BlobJdbcType {
      // The DuckDB JDBC driver doesn't support Blobs, because the
      // `DuckDBPreparedStatement.setBlob` is not implemented.
      // The workaround is to treat Blobs as serialized ByteArrays instead.
      override def sqlType: Int                                            = java.sql.Types.BINARY
      override def sqlTypeName(sym: Option[FieldSymbol]): String           = "BLOB"
      override def setValue(v: Blob, p: PreparedStatement, idx: Int): Unit =
        p.setBytes(idx, v.getBytes(1, v.length.toInt))
      override def getValue(rs: ResultSet, idx: Int): Blob                 = {
        val bytes = rs.getBytes(idx)
        new SerialBlob(bytes)
      }
      override def updateValue(v: Blob, rs: ResultSet, idx: Int): Unit     =
        rs.updateBytes(idx, v.getBytes(1, v.length.toInt))
      override val hasLiteralForm                                          = false
    }

    override val timeJdbcType: TimeJdbcType = new DuckDBTimeJdbcType
    private class DuckDBTimeJdbcType extends super.TimeJdbcType {
      override def valueToSQLLiteral(value: Time): String =
        "'" + value.toString + "'"
      override def hasLiteralForm: Boolean                = true
    }

    override val timestampJdbcType: TimestampJdbcType =
      new DuckDBTimestampJdbcType
    private class DuckDBTimestampJdbcType extends super.TimestampJdbcType {
      override def valueToSQLLiteral(value: java.sql.Timestamp): String =
        "'" + value.toString + "'"
      override def hasLiteralForm: Boolean                              = true
    }

    override val dateJdbcType: DateJdbcType = new DuckDBDateJdbcType
    private class DuckDBDateJdbcType extends super.DateJdbcType {
      override def valueToSQLLiteral(value: java.sql.Date): String =
        "'" + value.toString + "'"
      override def hasLiteralForm: Boolean                         = true
    }

    override val uuidJdbcType: UUIDJdbcType = new DuckDBUUIDJdbcType
    private class DuckDBUUIDJdbcType extends super.UUIDJdbcType {
      override def sqlTypeName(sym: Option[FieldSymbol]): String           = "UUID"
      override def setValue(v: UUID, p: PreparedStatement, idx: Int): Unit =
        p.setString(idx, if (v == null) null else v.toString)
      override def getValue(r: ResultSet, idx: Int): UUID                  = {
        val s = r.getString(idx)
        if (s eq null) null else UUID.fromString(s)
      }
      override def updateValue(v: UUID, r: ResultSet, idx: Int): Unit      =
        r.updateString(idx, if (v == null) null else v.toString)
      override def valueToSQLLiteral(value: UUID): String                  =
        if (value == null) "NULL" else "'" + value.toString + "'"
      override def hasLiteralForm: Boolean                                 = true
    }

  }

  override def createQueryBuilder(
      n: Node,
      state: CompilerState
  ): DuckDBQueryBuilder =
    new DuckDBQueryBuilder(n, state)

  class DuckDBQueryBuilder(tree: Node, state: CompilerState)
      extends QueryBuilder(tree, state) {
    override protected val concatOperator: Option[String]                   = Some("||")
    override protected val quotedJdbcFns: Option[Seq[Library.JdbcFunction]] =
      Some(Seq.empty)

    override def expr(n: Node): Unit = n match {
      case Library.IfNull(ch, d)            => b"ifnull($ch, $d)"
      case Library.Length(ch)               => b"length($ch)"
      case Library.Database()               => b"current_database()"
      case Library.User()                   => b"current_user()"
      case Library.Substring(n, start, end) =>
        val startNode  =
          QueryParameter.constOp[Int]("+")(_ + _)(start, LiteralNode(1).infer())
        val lengthNode = QueryParameter.constOp[Int]("-")(_ - _)(end, start)
        b"substring($n, $startNode, $lengthNode)"
      case Library.Substring(n, start)      =>
        val startNode =
          QueryParameter.constOp[Int]("+")(_ + _)(start, LiteralNode(1).infer())
        b"substring($n, $startNode)"
      case Library.IndexOf(n, str)          => b"strpos($n, $str) - 1"

      // Since the DuckDB dialect is widely compatible with Postgres SQL by design,
      // the following function mappings are taken directly from the Postgres Profile.
      case Library.UCase(ch)                        => b"upper($ch)"
      case Library.LCase(ch)                        => b"lower($ch)"
      case Library.NextValue(SequenceNode(name))    => b"nextval('$name')"
      case Library.CurrentValue(SequenceNode(name)) => b"currval('$name')"
      case Library.CurrentDate()                    => b"current_date"
      case Library.CurrentTime()                    => b"current_time"

      case _ => super.expr(n)
    }

    // DuckDB supports limit and offset clauses that are more modern than the SQL 2008 standard
    // implemented in the super method.
    override protected def buildFetchOffsetClause(
        fetch: Option[Node],
        offset: Option[Node]
    ): Unit =
      (fetch, offset) match {
        case (Some(t), Some(d)) => b"\nlimit $t offset $d"
        case (Some(t), None)    => b"\nlimit $t"
        case (None, Some(d))    => b"\noffset $d"
        case _                  => ()
      }
  }

  override def createTableDDLBuilder(table: Table[?]): DuckDBTableDDLBuilder =
    new DuckDBTableDDLBuilder(table)

  class DuckDBTableDDLBuilder(table: Table[?]) extends TableDDLBuilder(table) {
    // DuckDB doesn't support `ALTER TABLE` statements for primary and foreign key constraints.
    // The key constraints must be provided on table creation. Here we handle it in the `addTableOptions`
    // method instead.
    override val foreignKeys: Nil.type = Nil
    override val primaryKeys: Nil.type = Nil

    private val autoIncCols = table.create_*.filter(_.options.contains(AutoInc))

    // TODO: create Sequence with RelationalComponent.Sequence and createSequenceDDLBUilder
    private def createSequencesForAutoInc: Iterable[String] = autoIncCols.map {
      col =>
        val seqName = getBackingSequenceName(table.tableName, col.name)
        s"create sequence if not exists $seqName"
    }

    override def createPhase1: Iterable[String] = {
      createSequencesForAutoInc ++ super.createPhase1
    }

    override def createIfNotExistsPhase: Iterable[String] = {
      createSequencesForAutoInc ++ super.createIfNotExistsPhase
    }

    override def addTableOptions(sb: StringBuilder): Unit = {
      for (pk <- table.primaryKeys) {
        sb append ","
        addPrimaryKey(pk, sb)
      }
      for (fk <- table.foreignKeys) {
        sb append ","
        addForeignKey(fk, sb)
      }
    }

    override def addPrimaryKey(pk: PrimaryKey, sb: StringBuilder): Unit = {
      sb append "primary key("
      addPrimaryKeyColumnList(pk.columns, sb, tableNode.tableName)
      sb append ")"
    }

    override def addForeignKey(fk: ForeignKey, sb: StringBuilder): Unit = {
      sb append "foreign key("
      addForeignKeyColumnList(
        fk.linearizedSourceColumns,
        sb,
        tableNode.tableName
      )
      sb append ") references " append quoteTableName(fk.targetTable) append "("
      addForeignKeyColumnList(
        fk.linearizedTargetColumnsForOriginalTargetTable,
        sb,
        fk.targetTable.tableName
      )
      sb append ") on update " append fk.onUpdate.action
      sb append " on delete " append fk.onDelete.action
    }
  }

  override def createColumnDDLBuilder(
      column: FieldSymbol,
      table: Table[?]
  ): DuckDBColumnDDLBuilder =
    new DuckDBColumnDDLBuilder(column, table)

  class DuckDBColumnDDLBuilder(column: FieldSymbol, table: Table[?])
      extends ColumnDDLBuilder(column) {

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

  // We need to override base UpsertBuilder, because it's implemented using `MERGE` which DuckDB doesn't support.
  override def createUpsertBuilder(node: Insert): InsertBuilder = new DuckDBUpsertBuilder(node)

  class DuckDBUpsertBuilder(insert: Insert) extends UpsertBuilder(insert) {
    override def buildInsert: InsertBuilderResult = {
      if (pkNames.isEmpty) {
        throw new SlickException("Primary key required for insertOrUpdate")
      }
      val pkCols = pkNames.mkString(", ")
      val updateAssignments = softNames
        .map(fs => s"$fs = EXCLUDED.$fs")
        .mkString(", ")
      val conflictAction = if (updateAssignments.isEmpty) "do nothing" else "do update set " + updateAssignments
      val insertSql =
           s"""insert into $tableName ${allNames.mkString("(", ", ", ")")}
           |values $allVars
           |on conflict ($pkCols)
           |$conflictAction
           |""".stripMargin.replaceAll("\n", " ")
      new InsertBuilderResult(table, insertSql, allFields) {
        override def buildMultiRowInsert(size: Int): String = {
          // Generate placeholders per row (e.g., `(?, ?, ?)` with allVars)
          val rowPlaceholder = allVars
          // Create placeholders for all rows
          val multiRowPlaceholder = List.fill(size)(rowPlaceholder).mkString(", ")

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

object DuckDBProfile extends DuckDBProfile {
  private def getBackingSequenceName(
      tableName: String,
      columnName: String
  ): String = s"${tableName}_${columnName}_seq"
}
