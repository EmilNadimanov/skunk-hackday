package nad.emil

import anorm._
import cats.effect._
import cats.implicits._
import org.typelevel.otel4s.trace.Tracer.Implicits._
import skunk._
import skunk.implicits._

import scala.concurrent.ExecutionContext.global

object Comparison extends IOApp {
  implicit val ec: scala.concurrent.ExecutionContext = global

  val sessionPool: Resource[IO, Resource[IO, Session[IO]]] =
    Session.pooled(
      host = "",
      port = 5432,
      user = "",
      database = "",
      max = 10,
      password = Some("")
    )
  private val postgresConfigW = PostgresConfig(
    host = "",
    port = 5432,
    password = "",
    user = "",
    database = "",
    schemaName = "",
  )
  private val postgresConfigR = PostgresConfig(
    host = "",
    port = 5432,
    password = "",
    user = "",
    database = "",
    schemaName = "",
  )

  val database: ReadWriteDatabase = new ReadWriteDatabase(Database[PostgresConfig](postgresConfigR), Database[PostgresConfig](postgresConfigW))

  def getRevenueAggregationRowsAnorm(): IO[List[ConfirmationRevenueAggregationRow]] = IO.fromFuture {
    IO {
      database.read { implicit c =>
        SQL"""
      WITH regular_data as (
        SELECT
            DATE_TRUNC('DAY', current_data.date) AS result_date,
            DATE_TRUNC('DAY', MIN(comparison_data.date)) AS compared_date,
            SUM(current_data.unconfirmed_revenue) AS unconfirmed_revenue,
            SUM(current_data.confirmed_revenue) AS confirmed_revenue,
            0 AS additional_revenue,
            NULLIF(COALESCE(SUM(comparison_data.unconfirmed_revenue), 0) + COALESCE(SUM(comparison_data.confirmed_revenue), 0), 0) AS comparison_revenue
          FROM aggregation_dashboard_daily_revenue current_data
          LEFT JOIN aggregation_dashboard_daily_revenue comparison_data
            ON current_data.date - interval '7 day' = comparison_data.date
            AND current_data.publisher_code = comparison_data.publisher_code
            AND current_data.channel = comparison_data.channel
            AND current_data.position = comparison_data.position
            AND current_data.device = comparison_data.device
            AND current_data.domain = comparison_data.domain
          WHERE current_data.publisher_code = 'gutefrage' and current_data.date = '2024-11-12'
          GROUP BY result_date
      ),
      additional_data as (
        SELECT
          DATE_TRUNC('DAY', additional_data.date)    AS result_date,
          DATE_TRUNC('DAY', MIN(comparison_data.date))    AS compared_date,
          0 AS unconfirmed_revenue,
          0 AS confirmed_revenue,
          SUM(additional_data.unconfirmed_revenue + additional_data.confirmed_revenue) as additional_revenue,
          NULLIF(COALESCE(SUM(comparison_data.unconfirmed_revenue), 0) + COALESCE(SUM(comparison_data.confirmed_revenue), 0), 0) AS comparison_revenue
          FROM aggregation_additional_daily_revenue additional_data
          LEFT JOIN aggregation_additional_daily_revenue comparison_data
            ON additional_data.date - interval '7 day' = comparison_data.date
            AND additional_data.publisher_code = comparison_data.publisher_code
            AND additional_data.channel = comparison_data.channel
            AND additional_data.partner = comparison_data.partner
            AND additional_data.device = comparison_data.device
          WHERE additional_data.publisher_code = 'gutefrage' and additional_data.date = '2024-11-12'
          GROUP BY result_date
      )
      SELECT regular_data.result_date,
             regular_data.compared_date,
             regular_data.unconfirmed_revenue + COALESCE(additional_data.unconfirmed_revenue, 0) AS unconfirmed_revenue,
             regular_data.confirmed_revenue + COALESCE(additional_data.confirmed_revenue, 0)     AS confirmed_revenue,
             regular_data.additional_revenue + COALESCE(additional_data.additional_revenue, 0)   AS additional_revenue,
             regular_data.comparison_revenue + COALESCE(additional_data.comparison_revenue, 0)   AS comparison_revenue
      FROM regular_data
      LEFT JOIN additional_data
        ON regular_data.result_date = additional_data.result_date
      ORDER BY regular_data.result_date
    """.as(ConfirmationRevenueAggregationRow.confirmationRevenueAggregationRowParser.*)
      }
    }
  }

  def getRevenueAggregationRowsSkunk(): Query[Void, ConfirmationRevenueAggregationRow] = {
    sql"""
      WITH regular_data as (
        SELECT
            DATE_TRUNC('DAY', current_data.date) AS result_date,
            DATE_TRUNC('DAY', MIN(comparison_data.date)) AS compared_date,
            SUM(current_data.unconfirmed_revenue) AS unconfirmed_revenue,
            SUM(current_data.confirmed_revenue) AS confirmed_revenue,
            0 AS additional_revenue,
            NULLIF(COALESCE(SUM(comparison_data.unconfirmed_revenue), 0) + COALESCE(SUM(comparison_data.confirmed_revenue), 0), 0) AS comparison_revenue
          FROM h5v_reporting.aggregation_dashboard_daily_revenue current_data
          LEFT JOIN h5v_reporting.aggregation_dashboard_daily_revenue comparison_data
            ON current_data.date - interval '7 day' = comparison_data.date
            AND current_data.publisher_code = comparison_data.publisher_code
            AND current_data.channel = comparison_data.channel
            AND current_data.position = comparison_data.position
            AND current_data.device = comparison_data.device
            AND current_data.domain = comparison_data.domain
          WHERE current_data.publisher_code = 'gutefrage' and current_data.date = '2024-11-12'
          GROUP BY result_date
      ),
      additional_data as (
        SELECT
          DATE_TRUNC('DAY', additional_data.date)    AS result_date,
          DATE_TRUNC('DAY', MIN(comparison_data.date))    AS compared_date,
          0 AS unconfirmed_revenue,
          0 AS confirmed_revenue,
          SUM(additional_data.unconfirmed_revenue + additional_data.confirmed_revenue) as additional_revenue,
          NULLIF(COALESCE(SUM(comparison_data.unconfirmed_revenue), 0) + COALESCE(SUM(comparison_data.confirmed_revenue), 0), 0) AS comparison_revenue
          FROM h5v_reporting.aggregation_additional_daily_revenue additional_data
          LEFT JOIN h5v_reporting.aggregation_additional_daily_revenue comparison_data
            ON additional_data.date - interval '7 day' = comparison_data.date
            AND additional_data.publisher_code = comparison_data.publisher_code
            AND additional_data.channel = comparison_data.channel
            AND additional_data.partner = comparison_data.partner
            AND additional_data.device = comparison_data.device
          WHERE additional_data.publisher_code = 'gutefrage' and additional_data.date = '2024-11-12'
          GROUP BY result_date
      )
      SELECT regular_data.result_date,
             regular_data.compared_date,
             regular_data.unconfirmed_revenue + COALESCE(additional_data.unconfirmed_revenue, 0) AS unconfirmed_revenue,
             regular_data.confirmed_revenue + COALESCE(additional_data.confirmed_revenue, 0)     AS confirmed_revenue,
             regular_data.additional_revenue + COALESCE(additional_data.additional_revenue, 0)   AS additional_revenue,
             regular_data.comparison_revenue + COALESCE(additional_data.comparison_revenue, 0)   AS comparison_revenue
      FROM regular_data
      LEFT JOIN additional_data
        ON regular_data.result_date = additional_data.result_date
      ORDER BY regular_data.result_date
    """.query(ConfirmationRevenueAggregationRow.confirmationRevenueAggregationRowDecoder)
  }

  def runNtimes[A](computation: IO[A], name: String, N: Int): IO[List[A]] = for {
    realTime <- IO.realTime
    result <- List.fill(N)(computation).parTraverse(identity)
    duration <- IO.realTime.map(_ - realTime)
    _ <- IO.println(s"[${name}] Duration: $duration or ${duration.toMicros.toDouble / 1_000_000} seconds")
  } yield result


  def testSkunk: IO[Unit] = sessionPool
    // access the pool
    .use { session =>
      // the action consists of :
      val action = session
        // (1) taking a connection from the pool and
        .use { s: Session[IO] =>
          // (2) executing the query on it
          s.execute(getRevenueAggregationRowsSkunk())
        }
      runNtimes(action, "SKUNK", 400).void
    }

  def testAnorm: IO[Unit] = 
    runNtimes(getRevenueAggregationRowsAnorm(), "ANORM", 400).void

  def run(args: List[String]): IO[ExitCode] = for {
    _ <- if (args.head == "anorm") testAnorm
    else if (args.head == "skunk") testSkunk
    else IO.println("Please provide either 'anorm' or 'skunk' as an argument")
  } yield ExitCode.Success

}
