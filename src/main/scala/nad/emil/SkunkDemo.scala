package nad.emil

import cats.effect._
import cats.implicits._
import skunk._
import skunk.implicits._
import skunk.codec.all._
import org.typelevel.otel4s.trace.Tracer.Implicits._
import scala.collection.View.Empty

object SkunkDemo extends IOApp {

  val session: Resource[IO, Session[IO]] =
    Session.single(
      host     = "",
      port     = 5432,
      user     = "",
      database = "",
      password = Some("")
    )

  val sessionPool: Resource[IO, Resource[IO, Session[IO]]] =
    Session.pooled(
      host     = "",
      port     = 5432,
      user     = "",
      database = "",
      max = 10,
      password = Some("")
    )

  case class Country(name: String, population: Int)

  object Country {
    val d: Decoder[Country] = (varchar *: int4).to[Country]
  }

  case class Partner(displayName: String, partnerId: Long)

  object Partner {
    val d: Codec[Partner] = (text *: int8).to[Partner]
  }

  def partnerQuery: Query[String *: Long *: EmptyTuple ,Partner] =
    sql"""
      SELECT
        display_name,
        partner_id 
      FROM h5v_reporting.partner_mapping
      WHERE display_name LIKE $text and partner_id < $int8
      """
      .query(Partner.d)

  def runPartnerQuery: IO[List[Partner]] = session.use {
    s => s.execute(partnerQuery)(args = "A%" *: 100L *: EmptyTuple)
  }

  def getOnePartnerx: IO[Partner] = session.use {
    s => s.unique(partnerQuery)(args = "A%" *: 100L *: EmptyTuple)
  }

  def getOnePartner: IO[Partner] = session.use {
    s => s.unique(partnerQueryLego)(args = "A%" *: 100L *: EmptyTuple)
  }

  def getPartnerCursor: Resource[IO, Cursor[IO, Partner]]= 
    session.flatMap(s => s.cursor(partnerQuery)(args = "A%" *: 100L *: EmptyTuple))

  def streamPartners = 
    session.use { s =>
      s
        .stream(partnerQuery)(("A%", 65L), 4)
        .take(10)
        .compile
        .toList
    }

  def insertPartnersCommand(ps: List[Partner]): Command[List[Partner]] = {
    val enc: Encoder[List[Partner]] = Partner.d.list(ps.length)
    sql"INSERT INTO h5v_reporting.pets VALUES $enc".command
  }

  def setSeed(): Command[Void] = {
    sql"SET SEED TO 0.123".command
  }

  def prepareInsertCommand(ps: List[Partner]) = session.use { s =>
    s.prepare(insertPartnersCommand(ps)).flatMap(_.execute(ps))
  }

  def usingPipe = 
    session.use { s =>
      fs2.Stream
        .emits[IO, String *: Long *: EmptyTuple](
          Seq(
            "A%" *: 100L *: EmptyTuple,
            "B%" *: 100L *: EmptyTuple
          )
        )
        .through(s.pipe(partnerQuery, 2))
        .evalTap(IO.println)
        .compile
        .drain
    } 

  def partnerQueryLego = {
    val rows = sql"""
        display_name,
        partner_id
    """

    val whereClause = sql"""
      WHERE display_name LIKE $text and partner_id < $int8
    """

    sql"""
      SELECT $rows
      FROM h5v_reporting.partner_mapping
      $whereClause
      """
      .query((text ~ int8).map(a => Partner(a._1, a._2)))
  }



  def getMetricsAggregationRow() = {
    val filter = "current_data.publisher_code = 'gutefrage' and current_data.date = '2024-11-12'"
    val comparisonPeriodDays = 7
    val aggregationLevelValue = "DAY"

     val decoder =  
        timestamptz *:
        timestamptz.opt *:
        numeric *:
        numeric *:
        numeric *:
        int8 *:
        int8 *:
        int4 *:
        float8 *:
        float8 *:
        int8 *:
        float8 *:
        int8 *:
        float8 *:
        float8 *:
        numeric *:
        int8 *:
        float8 *:
        float8 *:
        int8 *:
        float8 *:
        int8 *:
        float8 *:
        float8

    sql"""SELECT
        DATE_TRUNC('#${aggregationLevelValue}', current_data.date) AS result_date,
        DATE_TRUNC('#${aggregationLevelValue}', MIN(comparison_data.date)) AS compared_date,
        SUM(current_data.unconfirmed_revenue) AS unconfirmed_revenue,
        SUM(current_data.confirmed_revenue) AS confirmed_revenue,
        SUM(current_data.additional_revenue) AS additional_revenue,
        SUM(current_data.unconfirmed_impressions) AS unconfirmed_impressions,
        SUM(current_data.confirmed_impressions) AS confirmed_impressions,
        0 AS additional_impressions, -- always 0, because additional revenue is not filterable
        COALESCE((COALESCE(SUM(current_data.confirmed_revenue) + SUM(current_data.unconfirmed_revenue), 0)::float)::float / NULLIF((SUM(current_data.confirmed_impressions) + SUM(current_data.unconfirmed_impressions)), 0), 0) * 1000.0 AS current_eCPM,
        (SUM(current_data.confirmed_impressions) + SUM(current_data.unconfirmed_impressions))::float / NULLIF(COALESCE(SUM(current_data.ad_requests), 0), 0)::float AS current_fill_rate,
        COALESCE(SUM(current_data.page_views), 0) AS current_page_views,
        COALESCE((SUM(current_data.confirmed_revenue) + SUM(current_data.unconfirmed_revenue))::float * 1000.0 / NULLIF(COALESCE(SUM(current_data.page_views), 0), 0)::float, 0) AS current_page_rpm,
        SUM(current_data.pubstack_page_views) AS current_pubstack_page_views,
        COALESCE((SUM(current_data.confirmed_revenue) + SUM(current_data.unconfirmed_revenue))::float * 1000.0 / SUM(current_data.pubstack_page_views)::float, 0) AS current_effective_page_rpm,
        COALESCE((SUM(current_data.ad_manager_active_view_impressions))::float / NULLIF(COALESCE(SUM(current_data.ad_manager_impressions), 0), 0), 0) AS current_viewability,
        -- comparison
        SUM(comparison_data.unconfirmed_revenue) + SUM(comparison_data.confirmed_revenue) + COALESCE(SUM(comparison_data.additional_revenue), 0) AS comparison_revenue,
        NULLIF(COALESCE(SUM(comparison_data.unconfirmed_impressions), 0) + COALESCE(SUM(comparison_data.confirmed_impressions), 0), 0) AS comparison_impressions,
        (COALESCE(SUM(comparison_data.confirmed_revenue) + SUM(comparison_data.unconfirmed_revenue), 0)::float)::float / NULLIF((SUM(comparison_data.confirmed_impressions) + SUM(comparison_data.unconfirmed_impressions)), 0) * 1000.0 AS comparison_eCPM,
        (SUM(comparison_data.confirmed_impressions) + SUM(comparison_data.unconfirmed_impressions))::float / NULLIF(COALESCE(SUM(comparison_data.ad_requests), 0), 0)::float AS comparison_fill_rate,
        SUM(comparison_data.page_views) AS comparison_page_views,
        (SUM(comparison_data.confirmed_revenue) + SUM(comparison_data.unconfirmed_revenue))::float * 1000.0 / NULLIF(COALESCE(SUM(comparison_data.page_views), 0), 0)::float AS comparison_page_rpm,
        SUM(comparison_data.pubstack_page_views) AS comparison_pubstack_page_views,
        (SUM(comparison_data.confirmed_revenue) + SUM(comparison_data.unconfirmed_revenue))::float * 1000.0 / SUM(comparison_data.pubstack_page_views)::float AS comparison_effective_page_rpm,
        (SUM(comparison_data.ad_manager_active_view_impressions))::float / NULLIF(COALESCE(SUM(current_data.ad_manager_impressions), 0), 0)::float AS comparison_viewability
    FROM aggregation_dashboard_daily_publisher_metrics current_data
    LEFT JOIN aggregation_dashboard_daily_publisher_metrics comparison_data
             ON current_data.date - interval '#${comparisonPeriodDays.toString} day' = comparison_data.date
             AND current_data.publisher_code = comparison_data.publisher_code
    WHERE #$filter
    GROUP BY result_date
    ORDER BY result_date
    """.query(decoder)
  }


  


  def run(args: List[String]): IO[ExitCode] = 
    session.use {
      s => s.execute(getMetricsAggregationRow()).flatMap(IO.println)
    }.as(ExitCode.Success)

}
