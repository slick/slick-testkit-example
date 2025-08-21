package duckdbslick

import builders.{DuckDBColumnDDLBuilderComponent, DuckDBQueryBuilderComponent, DuckDBTableDDLBuilderComponent}
import slick.SlickException
import slick.ast.*
import slick.basic.Capability
import slick.compiler.CompilerState
import slick.dbio.DBIO
import slick.jdbc.JdbcActionComponent.MultipleRowsPerStatementSupport
import slick.jdbc.meta.MTable
import slick.jdbc.{InsertBuilderResult, JdbcCapabilities, JdbcProfile}

import java.sql.*
import java.util.UUID
import javax.sql.rowset.serial.SerialBlob
import scala.concurrent.ExecutionContext
import scala.language.implicitConversions

/** Slick profile for DuckDB.
  *
  * This profile extends the standard JDBC profile with DuckDB-specific
  * capabilities. It provides a foundation for using Slick with DuckDB
  * databases.
  *
  * DuckDB is an in-process SQL OLAP database management system designed to be
  * fast and efficient for analytical queries.
  */
trait DuckDBProfile
    extends JdbcProfile
    with MultipleRowsPerStatementSupport
    with DuckDBQueryBuilderComponent
    with DuckDBTableDDLBuilderComponent
    with DuckDBColumnDDLBuilderComponent {

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
      // JdbcCapabilities.forceInsert,

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

  override def createTableDDLBuilder(table: Table[?]): DuckDBTableDDLBuilder =
    new DuckDBTableDDLBuilder(table)

  override def createColumnDDLBuilder(
      column: FieldSymbol,
      table: Table[?]
  ): DuckDBColumnDDLBuilder =
    new DuckDBColumnDDLBuilder(column, table)

  // We need to override base UpsertBuilder, because it's implemented using `MERGE` which DuckDB doesn't support.
  override def createUpsertBuilder(node: Insert): InsertBuilder =
    new DuckDBUpsertBuilder(node)

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

object DuckDBProfile extends DuckDBProfile
