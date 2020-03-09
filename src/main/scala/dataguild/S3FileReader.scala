package dataguild

import org.apache.spark.sql.SparkSession

object S3FileReader {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("S3 File Reader")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.hadoopConfiguration.set("fs.s3a.access.key", args(0))
    sc.hadoopConfiguration.set("fs.s3a.secret.key", args(1))

    val currentDate = "2020-03-09"
    val path = s"s3a://airbnb-barcelona/raw/currentDate=$currentDate"
    val df = spark.read.format("parquet").load(path)
    df.show()

    println("row nums: " , df.collect().length)
  }

}
