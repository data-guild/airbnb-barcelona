package dataguild

import java.util.UUID

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions.{countDistinct, udf, unix_timestamp}
import org.apache.spark.sql.types.{BooleanType, DateType, DoubleType, FloatType, IntegerType, LongType, StructField, StructType}

import scala.util.{Failure, Success, Try}

object DataTypeValidation {

  def validate(df: DataFrame): Unit = {

    val priceCol = DataColumn("price", "Double")
    val schema = List(priceCol)
//    print(df.columns.length)
//    df.printSchema()
//    df.show()
//    df.agg(countDistinct("price")).show()
//    df.groupBy("price").count().show(1000)


    def typeValidation (colName: String, row: Row) = {
      val tryResult = colName match {
        case "id" => Try(row.getString(row.fieldIndex(colName)).toInt)
        case "price" => Failure(new Exception(s"validateDataTypes is not implemented for this datatype"))
      }

      if (tryResult.isSuccess) {
        println("SUCCESS")
      } else {
        println("FAIL!!!")
      }
    }

    val dataframe = df.select("id", "price").toDF()
    val columns = dataframe.columns

    columns.foreach(col => dataframe.foreach(row => typeValidation(col, row)))
  }

}
