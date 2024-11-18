package nad.emil

import com.zaxxer.hikari.HikariDataSource
import org.postgresql.util.PSQLException

import java.sql.Connection
import scala.concurrent._


case class Database[C <: DatabaseConfig](config: C) {
  this.getClass
  val dataSource = new HikariDataSource()
  dataSource.setDriverClassName((new org.postgresql.Driver).getClass.getName)
  dataSource.setJdbcUrl(config.jdbcUrl)
  dataSource.setUsername(config.user)
  dataSource.setPassword(config.password)
  dataSource.setSchema(config.schemaName)

  /** Get a JDBC connection from the underlying data source.
   * Autocommit is enabled by default.
   *
   * Don't forget to release the connection at some point by calling close().
   *
   * @return a JDBC connection
   */
  def getConnection(): Connection = {
    getConnection(autocommit = true)
  }

  /** Get a JDBC connection from the underlying data source.
   *
   * Don't forget to release the connection at some point by calling close().
   *
   * @param autocommit determines whether to autocommit the connection
   * @return a JDBC connection
   */
  def getConnection(autocommit: Boolean): Connection = {
    val connection = dataSource.getConnection
    try {
      connection.setAutoCommit(autocommit)
    } catch {
      case e =>
        connection.close()
        throw e
    }
    connection
  }

  /** Execute a block of code, providing a JDBC connection.
   * The connection and all created statements are automatically released.
   *
   * @param autocommit determines whether to autocommit the connection
   * @param block      code to execute
   * @return the result of the code block
   */
  def withConnection[A](autocommit: Boolean)(block: Connection => A)(implicit ec: ExecutionContext): Future[A] =
    Future {
      val connection = getConnection(autocommit)
      try {
        block(connection)
      } finally {
        connection.close()
      }
    }.recoverWith { case e: PSQLException =>
      Future.failed(DatabaseException(e.getMessage, e))
    }

  /** Execute a block of code, providing a JDBC connection.
   * The connection and all created statements are automatically released.
   *
   * @param block code to execute
   * @return the result of the code block
   */
  def withConnection[A](block: Connection => A)(implicit ec: ExecutionContext): Future[A] = {
    withConnection(autocommit = true)(block)
  }

  /** Close the underlying data source.
   */
  def close(): Unit = dataSource.close()
}
