package nad.emil

import org.postgresql.util.PSQLException

import java.sql.Connection
import scala.concurrent._


class ReadWriteDatabase(val readDatabase: Database[PostgresConfig], val writeDatabase: Database[PostgresConfig]) {

  def write[A](block: Connection => A)(implicit ec: ExecutionContext): Future[A] =
    writeWithLoggedInputs(Map.empty)(block)

  /** Execute a code of block with a connection to the write database
   */
  def writeWithLoggedInputs[A](
                                loggedInputs: Map[String, String]
                              )(block: Connection => A)(implicit ec: ExecutionContext): Future[A] =
    Future {
      val connection = writeDatabase.getConnection()
      try {
        block(connection)
      } finally {
        connection.close()
      }
    }.recoverWith { case e: PSQLException => toDatabaseException(e, loggedInputs) }

  def read[A](block: Connection => A)(implicit ec: ExecutionContext): Future[A] =
    readWithLoggedInputs(Map.empty)(block)

  /** Execute a code of block with a connection to the read database
   *
   * @param loggedInputs map from the name of the input to the value/content of the input
   */
  def readWithLoggedInputs[A](
                               loggedInputs: Map[String, String]
                             )(block: Connection => A)(implicit ec: ExecutionContext): Future[A] =
    Future {
      val connection = readDatabase.getConnection()
      try {
        block(connection)
      } finally {
        connection.close()
      }
    }.recoverWith { case e: PSQLException => toDatabaseException(e, loggedInputs) }

  private def toDatabaseException[A](e: PSQLException, loggedInputs: Map[String, String]): Future[A] = {
    val input: Option[String] = Option.when(loggedInputs.nonEmpty) {
      loggedInputs.map { case (key, value) => s"$key: $value" }.mkString("[", ",", "]")
    }
    Future.failed(DatabaseException(e.getMessage, e, input))
  }

}


