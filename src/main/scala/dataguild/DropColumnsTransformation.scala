package dataguild

import org.apache.spark.sql.{DataFrame, SparkSession}

object DropColumnsTransformation {
  def dropColumn(df: DataFrame, columns: Seq[String]): DataFrame = {
    val filteredDf = df.drop(columns: _*)
    filteredDf
  }


  def main(args: Array[String]): Unit = {

    val columnsToRemove = Seq("listing_url", "scrape_id", "last_scraped", "name", "summary", "space",
      "description", "neighborhood_overview", "notes", "transit", "access",
      "interaction", "house_rules", "thumbnail_url", "medium_url",
      "picture_url", "xl_picture_url", "host_id", "host_url", "host_name",
      "host_about", "host_thumbnail_url", "host_picture_url", "country_code",
      "country", "jurisdiction_names")

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("spark drop columns transformation")
      .getOrCreate()

    val airbnbDF = spark
      .read
      .format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .option("quote", "\"")
      .option("escape", "\"")
      .option("multiLine", true)
      .load("src/main/resource/raw/listings.csv")

    val outputDF = dropColumn(airbnbDF, columnsToRemove)
    outputDF.show()
  }

}
