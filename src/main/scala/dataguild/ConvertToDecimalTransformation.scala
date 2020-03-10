package dataguild

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, udf, when}

import scala.util.{Failure, Success, Try}

object ConvertToDecimalTransformation {

  def divideByValue(df: DataFrame, divider: Int, colName: String): DataFrame = {

    val columnsInOrder = df.columns
    val columnArray = columnsInOrder.map(str => col(str))

    val temporaryTransformedColumnName = s"${colName}_TRANSFORMED"

    df.withColumn(temporaryTransformedColumnName,
      when(
        col(colName).isNotNull,
        conversionUDF.divideByIntUdf(divider)(col(colName))
      ).otherwise(lit(null)))
      .drop(colName)
      .withColumnRenamed(temporaryTransformedColumnName, colName)
      .select(columnArray: _*)

  }


  def transform(df: DataFrame, colNames: String*): DataFrame = {
    colNames.foldLeft(df) { (df, colName) =>
      divideByValue(df,100:Int,colName)
    }
  }

}

object conversionUDF {
  def divideByIntUdf(divider: Int): UserDefinedFunction =
    udf((columnValue: String) => {
      val result = Try(columnValue.toDouble / divider)

      result match {
        case Success(x) => Some(x.toString)
        case Failure(_) => Some(columnValue)
      }
    })
}