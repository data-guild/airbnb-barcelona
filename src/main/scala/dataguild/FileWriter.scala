package dataguild
import org.apache.spark.sql.{DataFrame, SparkSession}

object FileWriter {

  def writeToError(df: DataFrame, format: String, spark: SparkSession): Unit ={
    val bucketPath = spark.conf.get("pathToSave")
    write(df, format, bucketPath+"error")
  }

  def writeToRaw(df: DataFrame, format: String, spark: SparkSession): Unit ={
    val bucketPath = spark.conf.get("pathToSave")
    write(df, format, bucketPath+"raw")
  }

  def writeToValid(df: DataFrame, format: String, spark: SparkSession): Unit ={
    val bucketPath = spark.conf.get("pathToSave")
    write(df, format, bucketPath+"valid")
  }

  def write(df: DataFrame, format: String, folderPath: String, partitionCols: Seq[String] = Seq()): Unit = {
    df
      .write
      .mode("append")
      .format(format)
      .partitionBy("currentDate")
      .save(folderPath)
  }
}
