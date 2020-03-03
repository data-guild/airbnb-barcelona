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

    WholeDataInjection.dataInjection(spark)
  }
}
