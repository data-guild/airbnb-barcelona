package dataguild

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object ReplaceStringTransformation {

  def replaceString(df: DataFrame, columnName: String, source: String, target: String): DataFrame = {

    val columnsInOrder = df.columns
    val columnArray = columnsInOrder.map(str => col(str))

    val temporaryTransformedColumnName = s"${columnName}_TRANSFORMED"
    df.withColumn(temporaryTransformedColumnName, UDFs.replaceCharUdf(source, target)(col(columnName)))
      .drop(columnName)
      .withColumnRenamed(temporaryTransformedColumnName, columnName)
      .select(columnArray: _*)

  }

}

object UDFs {

  def replaceCharUdf(source: String, target: String): UserDefinedFunction =
    udf((columnValue: String) => {
      columnValue.replace(source, target)
    })

}
