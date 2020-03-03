package dataguild
import org.apache.spark.sql.SparkSession

object S3Writer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Airbnb Barcelona Ingest")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.hadoopConfiguration.set("fs.s3a.access.key", args(0))
    sc.hadoopConfiguration.set("fs.s3a.secret.key", args(1))

    val airbnbDf = spark
      .read
      .format("csv")
      .option("inferSchema", "false")
      .option("header", "true")
      .load("src/main/resource/raw/listings.csv")

    airbnbDf
      .write
      .mode("append")
      .format("parquet")
      .save("s3a://airbnb-barcelona/raw")

    spark.stop()

  }

}
