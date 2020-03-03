package dataguild

import org.apache.spark.sql.SparkSession

object DataInjection {


  def main(args:Array[String]): Unit = {

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
      .option("inferSchema", true)
      .option("quote", "\"")
      .option("escape", "\"")
      .option("multiLine", true)
      .load("src/main/resource/raw/listings.csv")

    val columnsToRemove = Seq("listing_url", "scrape_id", "last_scraped", "name", "summary", "space",
      "description", "neighborhood_overview", "notes", "transit", "access",
      "interaction", "house_rules", "thumbnail_url", "medium_url",
      "picture_url", "xl_picture_url", "host_id", "host_url", "host_name",
      "host_about", "host_thumbnail_url", "host_picture_url", "country_code",
      "country", "jurisdiction_names")

    val airbnbColumnDroppedDF = DropColumnsTransformation.dropColumn(airbnbDF, columnsToRemove)

    WholeDataInjection.dataInjection(spark)

    spark.stop()
  }
}
