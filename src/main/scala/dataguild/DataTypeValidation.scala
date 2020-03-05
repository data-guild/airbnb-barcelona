package dataguild

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, IntegerType}

import scala.util.{Failure, Success, Try}

object DataTypeValidation {

  def validate(df: DataFrame): Unit = {

    val priceCol = DataColumn("price", "Double")
    val schema = List(priceCol)


    def typeValidation (colName: String, row: Row) = {
      val tryResult = colName match {
        case "id" => Try(row.getAs[Int](colName))
        case "price" => Try(row.getAs[Double](colName))
        case t@_ => Failure(new Exception(s"validateDataTypes is not implemented for this datatype $t"))
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
