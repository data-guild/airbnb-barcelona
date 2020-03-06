package dataguild

<<<<<<< HEAD
import dataguild.caseclass.Replacement
=======
import dataguild.caseclass.DataColumn
import org.apache.spark.sql.types.{DoubleType, StringType}
>>>>>>> [#29] WIP cast data type
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataIngestion {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Airbnb Barcelona Ingest")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.hadoopConfiguration.set("fs.s3a.access.key", args(0))
    sc.hadoopConfiguration.set("fs.s3a.secret.key", args(1))
    spark.conf.set("pathToSave", args(2))

    val airbnbDF = spark
      .read
      .format("csv")
      .option("header", true)
      .option("quote", "\"")
      .option("escape", "\"")
      .option("multiLine", true)
      .load("src/main/resource/raw/listings.csv")

    val transformedDF = transform(airbnbDF)

    //    FileWriter.writeToRaw(transformedDF, "parquet", spark)

    val (validDf, errorDf)=DataTypeValidation.validate(transformedDF, Schema.schema)

    validDf.createOrReplaceGlobalTempView("validDfTable")
    val castedDf = spark.sql("select cast(year as date) as year from validDfTable")

    Schema.generateDfWithSchema(validDf, Schema.schema)
    spark.stop()
  }

  def transform(airbnbDF: DataFrame) = {
    val columnsToRemove = Seq("listing_url", "scrape_id", "last_scraped", "name", "summary", "space",
      "description", "neighborhood_overview", "notes", "transit", "access",
      "interaction", "house_rules", "thumbnail_url", "medium_url",
      "picture_url", "xl_picture_url", "host_id", "host_url", "host_name",
      "host_about", "host_thumbnail_url", "host_picture_url", "country_code",
      "country", "jurisdiction_names")

    val columnDroppedDF = DropColumnsTransformation.dropColumn(airbnbDF, columnsToRemove)
    val withRowKeyDF = AddRowKeyTransformation.transform(columnDroppedDF)
    val withCurrentDateDF = AddCurrentDateTransformation.transform(withRowKeyDF)
    val replacements = List(
      Replacement("price", "$", ""),
      Replacement("monthly_price", "$", ""),
      Replacement("weekly_price", "$", ""),
      Replacement("security_deposit", "$", ""),
      Replacement("cleaning_fee", "$", ""),
      Replacement("extra_people", "$", ""),
      Replacement("price", ",", ""))

    val replaceStringDF = ReplaceStringTransformation.replaceStringMultiColumn(withCurrentDateDF, replacements)
    replaceStringDF
  }
}
