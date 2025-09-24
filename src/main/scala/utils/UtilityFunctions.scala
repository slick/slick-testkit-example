package utils

object UtilityFunctions {

  /** Perform a topological sort so dependencies come first */
  def orderCreateStatements(
      stmts: Iterable[String]
  ): Seq[String] = {

    val tables = stmts.map { stmt =>
      val tableName =
        stmt.split(" ")(2) // crude but works for "create table X"
      val deps      = extractTableNamesReferencedByForeignKeys(stmt).toSet
      tableName -> (stmt -> deps)
    }.toMap

    def visit(
        node: String,
        visited: Set[String],
        acc: List[String]
    ): (Set[String], List[String]) = {
      if (visited.contains(node)) (visited, acc)
      else {
        val (stmt, deps) = tables(node)
        val (vis2, acc2) = deps.foldLeft((visited, acc)) { case ((v, a), dep) =>
          visit(dep, v, a)
        }
        (vis2 + node, stmt :: acc2)
      }
    }

    val (_, result) =
      tables.keys.foldLeft((Set.empty[String], List.empty[String])) {
        case ((v, a), node) => visit(node, v, a)
      }

    result.reverse
  }

  private[utils] def extractTableNamesReferencedByForeignKeys(
      sql: String
  ): Seq[String] = {
    val quotedColumnIdentifiers = """\("[\s\S]*?"\)"""
    val quotedTableIdentifier   = """("(?:\\.|[^"\\])*")"""
    val foreignKeyRegex         =
      s"""foreign key$quotedColumnIdentifiers references $quotedTableIdentifier$quotedColumnIdentifiers""".r
    foreignKeyRegex.findAllMatchIn(sql).map(_.group(1)).toSeq
  }

}
