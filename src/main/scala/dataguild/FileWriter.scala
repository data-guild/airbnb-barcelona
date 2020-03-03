package dataguild
import org.apache.spark.sql.{DataFrame, SparkSession}

object FileWriter {
  def write(df: DataFrame, format: String, folderPath: String): Unit = {
    df
      .write
      .mode("append")
      .format(format)
      .save(folderPath)
  }
}
