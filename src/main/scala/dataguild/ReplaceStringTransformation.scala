package dataguild

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import dataguild.caseclass.Replacement


object ReplaceStringTransformation {

  def replaceString(df: DataFrame, replacement: Replacement): DataFrame = {

    val columnsInOrder = df.columns
    val columnArray = columnsInOrder.map(str => col(str))

    val temporaryTransformedColumnName = s"${replacement.columnName}_TRANSFORMED"

    df.withColumn(temporaryTransformedColumnName,
      when(
        col(replacement.columnName).isNotNull,
        UDFs.replaceCharUdf(replacement.source, replacement.target, replacement.isRegex)(col(replacement.columnName))
      ).otherwise(lit(null)))
      .drop(replacement.columnName)
      .withColumnRenamed(temporaryTransformedColumnName, replacement.columnName)
      .select(columnArray: _*)

  }

  def replaceStringMultiColumn(df: DataFrame, replacement: List[Replacement]): DataFrame = {
    replacement.foldLeft(df)(replaceString)
  }

}

object UDFs {

  def replaceCharUdf(source: String, target: String, isRegex: Boolean): UserDefinedFunction =
    udf((columnValue: String) => {
      if (isRegex) {
        columnValue.replaceAll(source, target)
      } else {
        columnValue.replace(source, target)
      }
    })

}
