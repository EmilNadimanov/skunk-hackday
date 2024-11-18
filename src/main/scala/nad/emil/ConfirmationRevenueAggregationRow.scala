package nad.emil

import anorm.RowParser
import anorm.SqlParser._

import java.time.LocalDate
import skunk.Decoder
import skunk.implicits._
import skunk.codec.all._

case class ConfirmationRevenueAggregationRow(
  date: LocalDate,
  comparedDate: Option[LocalDate],
  unconfirmedRevenue: Double,
  confirmedRevenue: Double,
  additionalRevenue: Double,
  comparisonRevenue: Option[Double]
)

object ConfirmationRevenueAggregationRow {

  val confirmationRevenueAggregationRowParser: RowParser[ConfirmationRevenueAggregationRow] =
    (get[LocalDate]("result_date")
      ~ get[Option[LocalDate]]("compared_date")
      ~ double("unconfirmed_revenue")
      ~ double("confirmed_revenue")
      ~ double("additional_revenue")
      ~ get[Option[Double]]("comparison_revenue"))
      .map(flatten)
      .map((ConfirmationRevenueAggregationRow.apply _).tupled)

  val confirmationRevenueAggregationRowDecoder: Decoder[ConfirmationRevenueAggregationRow] =
    (timestamptz.map(_.toLocalDate) *:
    timestamptz.map(_.toLocalDate).opt *:
      numeric.map(_.toDouble) *:
        numeric.map(_.toDouble) *:
          numeric.map(_.toDouble) *:
            numeric.map(_.toDouble).opt).to[ConfirmationRevenueAggregationRow]

}
