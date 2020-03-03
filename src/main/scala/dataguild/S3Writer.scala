package dataguild
import org.apache.spark.sql.SparkSession

object S3Writer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Airbnb Barcelona Ingest")
      .master("local[*]")
      .getOrCreate()

    val airbnbDf = spark
      .read
      .format("csv")
      .option("inferSchema", "false")
      .option("header", "true")
      .load("src/data/listings.csv")

    airbnbDf.show()
    airbnbDf
      .write
      .format("parquet")
      .save("s3a://errors-airbnb/")

  }
}
