package dataguild
import org.apache.spark.sql.SparkSession

trait TestSparkSessionWrapper {
    lazy val spark: SparkSession = {
      SparkSession
        .builder()
        .master("local")
        .appName("spark test example")
        .getOrCreate()
    }
}
