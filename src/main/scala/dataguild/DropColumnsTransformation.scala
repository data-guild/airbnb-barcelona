package dataguild

import org.apache.spark.sql.{DataFrame}

object DropColumnsTransformation {

  def dropColumn(df: DataFrame, columns: Seq[String]): DataFrame = {
    val filteredDf = df.drop(columns: _*)
    filteredDf
  }

}
