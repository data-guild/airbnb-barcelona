package dataguild

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import dataguild.caseclass.DataColumn
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{DataType, DateType, DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.Try

object Schema {
  val schema = List(
    DataColumn("rowId", "String"),
    DataColumn("id", "String"),
    DataColumn("experiences_offered", "String"),
    DataColumn("host_since", "Date", "yyyy-MM-dd"),
    DataColumn("host_location", "String"),
    DataColumn("host_response_time", "String"),
    DataColumn("host_response_rate", "Double"),
    DataColumn("host_acceptance_rate", "Double"),
    DataColumn("host_is_superhost", "Boolean"),
    DataColumn("host_neighbourhood", "String"),
    DataColumn("host_listings_count", "Double"),
    DataColumn("host_total_listings_count", "Double"),
    DataColumn("host_verifications", "String"),
    DataColumn("host_has_profile_pic", "Boolean"),
    DataColumn("host_identity_verified", "Boolean"),
    DataColumn("street", "String"),
    DataColumn("neighbourhood", "String"),
    DataColumn("neighbourhood_cleansed", "String"),
    DataColumn("neighbourhood_group_cleansed", "String"),
    DataColumn("city", "String"),
    DataColumn("state", "String"),
    DataColumn("zipcode", "String"),
    DataColumn("market", "String"),
    DataColumn("smart_location", "String"),
    DataColumn("latitude", "Double"),
    DataColumn("longitude", "Double"),
    DataColumn("is_location_exact", "Boolean"),
    DataColumn("property_type", "String"),
    DataColumn("room_type", "String"),
    DataColumn("accommodates", "Integer"),
    DataColumn("bathrooms", "Double"),
    DataColumn("bedrooms", "Double"),
    DataColumn("beds", "Double"),
    DataColumn("bed_type", "String"),
    DataColumn("amenities", "String"),
    DataColumn("square_feet", "Double"),
    DataColumn("price", "Double"),
    DataColumn("weekly_price", "Double"),
    DataColumn("monthly_price", "Double"),
    DataColumn("security_deposit", "Double"),
    DataColumn("cleaning_fee", "Double"),
    DataColumn("guests_included", "Integer"),
    DataColumn("extra_people", "Double"),
    DataColumn("minimum_nights", "Integer"),
    DataColumn("maximum_nights", "Integer"),
    DataColumn("minimum_minimum_nights", "Integer"),
    DataColumn("maximum_minimum_nights", "Integer"),
    DataColumn("minimum_maximum_nights", "Integer"),
    DataColumn("maximum_maximum_nights", "Integer"),
    DataColumn("minimum_nights_avg_ntm", "Double"),
    DataColumn("maximum_nights_avg_ntm", "Double"),
    DataColumn("calendar_updated", "String"),
    DataColumn("has_availability", "Boolean"),
    DataColumn("availability_30", "Integer"),
    DataColumn("availability_60", "Integer"),
    DataColumn("availability_90", "Integer"),
    DataColumn("availability_365", "Integer"),
    DataColumn("calendar_last_scraped", "Date", "yyyy-MM-dd"),
    DataColumn("number_of_reviews", "Integer"),
    DataColumn("number_of_reviews_ltm", "Integer"),
    DataColumn("first_review", "Date", "yyyy-MM-dd"),
    DataColumn("last_review", "Date", "yyyy-MM-dd"),
    DataColumn("review_scores_rating", "Double"),
    DataColumn("review_scores_accuracy", "Double"),
    DataColumn("review_scores_cleanliness", "Double"),
    DataColumn("review_scores_checkin", "Double"),
    DataColumn("review_scores_communication", "Double"),
    DataColumn("review_scores_location", "Double"),
    DataColumn("review_scores_value", "Double"),
    DataColumn("requires_license", "Boolean"),
    DataColumn("license", "String"),
    DataColumn("instant_bookable", "Boolean"),
    DataColumn("is_business_travel_ready", "Boolean"),
    DataColumn("cancellation_policy", "String"),
    DataColumn("require_guest_profile_picture", "Boolean"),
    DataColumn("require_guest_phone_verification", "Boolean"),
    DataColumn("calculated_host_listings_count", "Integer"),
    DataColumn("calculated_host_listings_count_entire_homes", "Integer"),
    DataColumn("calculated_host_listings_count_private_rooms", "Integer"),
    DataColumn("calculated_host_listings_count_shared_rooms", "Integer"),
    DataColumn("reviews_per_month", "Double"),
    DataColumn("currentDate", "Date", "yyyy-MM-dd", false)
  )

  def enforceDataType(dataType: String): UserDefinedFunction = udf((columnValue: String) => {
    dataType match {
      case "Double" => columnValue.toDouble
      //      case DateType => LocalDate.parse(value, DateTimeFormatter.ofPattern(dataColumn.format))
      case "String" => columnValue
    }
  })

  def generateDfWithSchema(df: DataFrame, inputSchema: List[DataColumn]): DataFrame = {

    val origColOrder = inputSchema.map(_.name)

    val unorderedDf = inputSchema.foldLeft(df) {
      (df,dataCol) =>  {
        dataCol.dType match {
          case "Double" =>  df.withColumn(dataCol.name, df(dataCol.name).cast(DoubleType))
          case "String" => df
        }
      }
    }
    val returnDf = unorderedDf.select(origColOrder.map(col): _*)

    returnDf
  }
}
