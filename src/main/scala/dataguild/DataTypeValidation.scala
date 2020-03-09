package dataguild

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import dataguild.caseclass.{DataColumn, ErrorMessage}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

import scala.util.{Failure, Success, Try}

object DataTypeValidation {

  def validate(df: DataFrame, schema: List[DataColumn]): (DataFrame, DataFrame) = {
    val dfColumns = df.columns.map(each => col(each))
    val dfWithErrors = df.withColumn("errors", ValidateUDF.validateRow(schema)(struct(dfColumns: _*)))
    val errorsDf = dfWithErrors.select("errors")

    val finalErrors = errorsDf.withColumn("errors", explode(col("errors")))
    val errorMegDf = finalErrors.select("errors.*")
    val validDf = df.join(errorMegDf, df("rowId") === errorMegDf("rowId"), "leftanti")
    (validDf, errorMegDf)
  }

}

object ValidateUDF {
  def validateRow(schema: List[DataColumn]) = udf((row: Row) => {
    schema.flatMap(dataColumn => {
      val value = row.getString(row.fieldIndex(dataColumn.name))

      val result = dataColumn.dType match {
        case "Double" => Try(value.toDouble)
        case "Integer" => Try(value.toInt)
        case "Boolean" => Try(value.toBoolean)
        case "String" => Try(value)
        case "Date" => Try(LocalDate.parse(value, DateTimeFormatter.ofPattern(dataColumn.format)))
        case _ => Failure(new Exception("Some random exception"))
      }

      result match {
        case Success(_) => List.empty
        case Failure(_) => List(ErrorMessage(row.getAs("rowId"), dataColumn.name, value, "cannot do whatever", row.getAs("currentDate")))
      }
    })
  })
}