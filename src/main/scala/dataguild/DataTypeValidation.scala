package dataguild

import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}

import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.functions._

object DataTypeValidation {

  def validate(df: DataFrame): (DataFrame, DataFrame) = {

    val schema = List(
      DataColumn("rowId", "String"),
      DataColumn("price", "Double"))

    val dfColumns = df.columns.map(each => col(each))
    val dfWithErrors = df.withColumn("errors", ValidateUDF.validateRow(schema)(struct(dfColumns: _*)))


    val errorsDf = dfWithErrors.select("errors")

    errorsDf.show(false)
    errorsDf.printSchema()

    val finalErrors = errorsDf.withColumn("errors", explode(col("errors")))

    finalErrors.show(false)

    val errorMegDf = finalErrors.select("errors.*")

    errorMegDf.show(false)

    val validDf = df.join(errorMegDf, df("rowId") === errorMegDf("rowId"), "leftanti")

    (validDf, errorMegDf)
  }

}

case class ErrorMessage(rowId: String, columnName: String, columnValue: String, errorMessage: String)

object ValidateUDF {
  def validateRow(schema: List[DataColumn]) = udf((row: Row) => {
    schema.flatMap(dataColumn => {
      val value = row.getString(row.fieldIndex(dataColumn.name))

      val result = dataColumn.dType match {
        case "Double" => Try(value.toDouble)
        case "String" => Try(value)
        case _ => Failure(new Exception("Some random exception"))
      }

      result match {
        case Success(_) => List.empty
        case Failure(_) => List(ErrorMessage(row.getAs("rowId"), dataColumn.name, value, "cannot do whatever"))
      }
    })
  })
}