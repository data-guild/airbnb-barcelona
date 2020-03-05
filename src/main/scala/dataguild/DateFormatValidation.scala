package dataguild

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}

object DateFormatValidation {
  def validate(sourceDF: DataFrame, columnName: String, format: String) = {
    (sourceDF, sourceDF)
  }

}

object UDFs {
  def validate(format: String) = udf((columnValue: String) => {
    val fmt = DateTimeFormatter.ofPattern(format)
    val result  = Try(LocalDate.parse(columnValue, fmt))

    result match {
      case Success(x) => None
      case Failure(_) => Some("wrong date format")
    }
  })
}