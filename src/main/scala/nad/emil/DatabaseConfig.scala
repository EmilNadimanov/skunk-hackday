package nad.emil

import cats.implicits._
import org.postgresql.util.PSQLException

abstract class DatabaseConfig(dbms: String) {
  // database host
  def host: String

  // database port for postgresql usually 5432
  def port: Int

  // database user
  def user: String

  // database password
  def password: String

  // default database
  def database: String

  // name of the schema
  def schemaName: String = database

  val jdbcUrl: String = s"jdbc:$dbms://$host:$port/$database"
}


case class DatabaseException(message: String, cause: PSQLException, input: Option[String] = None)
  extends Exception(message + input.foldMap(input => s"\nUser input: $input"), cause)

case class PostgresConfig(
                           override val host: String,
                           override val port: Int,
                           override val user: String,
                           override val password: String,
                           override val database: String,
                           override val schemaName: String
                         ) extends DatabaseConfig(dbms = "postgresql")
