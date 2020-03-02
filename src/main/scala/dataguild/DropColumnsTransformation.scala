package dataguild

import org.apache.spark.sql.{DataFrame, SparkSession}

object DropColumnsTransformation {
  def dropColumn(df: DataFrame, columns: List[String]) : DataFrame = {
    val filteredDf = df.drop(columns:_*)
    filteredDf
  }


  def main(args:Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("spark drop columns transformation")
      .getOrCreate()

    val airbnbDF = spark.read.format("csv").option("header", "true").load("")
    val outputDF = dropColumn(airbnbDF, List())

    outputDF.show()

  }
}
