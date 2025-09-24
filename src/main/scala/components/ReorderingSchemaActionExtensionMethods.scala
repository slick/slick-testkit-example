package components

import slick.dbio.{Effect, NoStream}
import slick.jdbc.{JdbcBackend, JdbcProfile}
import utils.UtilityFunctions.orderCreateStatements

trait ReorderingSchemaActionExtensionMethods {
  self: JdbcProfile =>
  class ReorderingSchemaActionExtensionMethodsImpl(schema: DDL)
      extends JdbcSchemaActionExtensionMethodsImpl(schema: DDL) {

    override def create: ProfileAction[Unit, NoStream, Effect.Schema] =
      new SimpleJdbcProfileAction[Unit](
        "schema.create",
        schema.createStatements.toVector
      ) {
        def run(
            ctx: JdbcBackend#JdbcActionContext,
            sql: Vector[String]
        ): Unit = {
          val reorderedSql = orderCreateStatements(sql)
          for (s <- reorderedSql)
            ctx.session.withPreparedStatement(s)(_.execute)
        }
      }
  }
}
