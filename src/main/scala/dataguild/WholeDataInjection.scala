package dataguild

import org.apache.spark.sql.SparkSession

object WholeDataInjection {
  def dataInjection (spark: SparkSession): Unit = {
    val airbnbDf = spark
      .read
      .format("csv")
      .option("inferSchema", "false")
      .option("header", "true")
      .load("src/main/resource/raw/listings.csv")

    FileWriter.writeToRaw(airbnbDf, "parquet", spark)
  }
}