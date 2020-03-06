package dataguild

import dataguild.caseclass.Replacement
import org.apache.spark.sql.DataFrame

object ConvertBinaryValueToTrueFalseTransformation {
  def transform(df: DataFrame, colName: String): DataFrame = {
    val replaceStringTransformations = List(Replacement(colName, "^t$", "true", true), Replacement(colName, "^f$", "false", true))
    val replacedBinaryValueDf = ReplaceStringTransformation.replaceStringMultiColumn(df, replaceStringTransformations)
    replacedBinaryValueDf
  }

  def isNotValidString(columnValue: String): Boolean = {
    true
  }

}

