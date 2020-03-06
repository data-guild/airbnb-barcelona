package dataguild

import dataguild.caseclass.Replacement
import org.apache.spark.sql.DataFrame

object ConvertBinaryValueToTrueFalseTransformation {

  def transform(df: DataFrame, colNames: String*): DataFrame = {
    colNames.foldLeft(df) { (df, colName) =>
      val replaceStringTransformations = List(Replacement(colName, "^t$", "true", true), Replacement(colName, "^f$", "false", true))
      ReplaceStringTransformation.replaceStringMultiColumn(df, replaceStringTransformations)
    }
  }

}

