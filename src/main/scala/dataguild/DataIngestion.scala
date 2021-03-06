package dataguild


import dataguild.caseclass.Replacement
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataIngestion {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Airbnb Barcelona Ingest")
      .master("local[*]")
      .getOrCreate()

    spark.conf.set("pathToSave", args(0))

    val sc = spark.sparkContext
    if(args.length > 1) sc.hadoopConfiguration.set("fs.s3a.access.key", args(1))
    if(args.length > 2) sc.hadoopConfiguration.set("fs.s3a.secret.key", args(2))

    val sourceFile = "dataguild/raw/listings.csv"

    val airbnbDF = spark
      .read
      .format("csv")
      .option("header", true)
      .option("quote", "\"")
      .option("escape", "\"")
      .option("multiLine", true)
      .load(sourceFile)

    val transformedDF = transform(airbnbDF)
    FileWriter.writeToRaw(transformedDF, "parquet", spark)

    val (validDf, errorDf) = DataTypeValidation.validate(transformedDF, Schema.schema)
    FileWriter.writeToError(errorDf, "parquet", spark)

    val finalValidDf = Schema.generateDfWithSchema(validDf, Schema.schema)
    FileWriter.writeToValid(finalValidDf, "parquet", spark)

    spark.stop()
  }

  def transform(airbnbDF: DataFrame) = {
    val columnsToRemove = Seq("listing_url", "scrape_id", "last_scraped", "name", "summary", "space",
      "description", "neighborhood_overview", "notes", "transit", "access",
      "interaction", "house_rules", "thumbnail_url", "medium_url",
      "picture_url", "xl_picture_url", "host_url", "host_name",
      "host_about", "host_thumbnail_url", "host_picture_url", "country_code",
      "country", "jurisdiction_names", "host_acceptance_rate", "square_feet",
      "weekly_price", "monthly_price", "experiences_offered", "requires_license", "is_business_travel_ready",
      "calendar_last_scraped", "smart_location", "minimum_minimum_nights","city", "state",
      "maximum_minimum_nights", "minimum_maximum_nights", "maximum_maximum_nights")

    val columnDroppedDF = DropColumnsTransformation.dropColumn(airbnbDF, columnsToRemove)
    val withRowKeyDF = AddRowKeyTransformation.transform(columnDroppedDF)
    val withCurrentDateDF = AddCurrentDateTransformation.transform(withRowKeyDF)
    val replacements = List(
      Replacement("price", "$", ""),
      Replacement("security_deposit", "$", ""),
      Replacement("cleaning_fee", "$", ""),
      Replacement("extra_people", "$", ""),
      Replacement("price", ",", ""),
      Replacement("host_response_rate", "%", ""))

    val replaceStringDF = ReplaceStringTransformation.replaceStringMultiColumn(withCurrentDateDF, replacements)
    val convertToTrueFalseDf = ConvertBinaryValueToTrueFalseTransformation.transform(replaceStringDF,
      "host_is_superhost", "host_has_profile_pic", "host_identity_verified", "is_location_exact",
      "has_availability", "instant_bookable", "require_guest_profile_picture",
      "require_guest_phone_verification")

    val convertToDecimalsDf = ConvertToDecimalTransformation.transform(convertToTrueFalseDf, colNames = "host_response_rate")

    convertToDecimalsDf
  }
}
