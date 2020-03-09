package dataguild

import org.apache.spark.sql.{DataFrame, SparkSession}

object FileWriter {

  val PARTITION_COLS = Seq("currentDate")

  def writeToError(df: DataFrame, format: String, spark: SparkSession): Unit = {
    val bucketPath = spark.conf.get("pathToSave")
    write(df, format, bucketPath + "error", PARTITION_COLS)
  }

  def writeToRaw(df: DataFrame, format: String, spark: SparkSession): Unit = {
    val bucketPath = spark.conf.get("pathToSave")
    write(df, format, bucketPath + "raw", PARTITION_COLS)
  }

  def writeToValid(df: DataFrame, format: String, spark: SparkSession): Unit = {
    val bucketPath = spark.conf.get("pathToSave")
    write(df, format, bucketPath + "valid", PARTITION_COLS)
  }

  def write(df: DataFrame, format: String, folderPath: String, partitionCols: Seq[String] = Seq()): Unit = {
    df
      .write
      .mode("overwrite")
      .format(format)
      .partitionBy(partitionCols: _*)
      .save(folderPath)
  }
}
