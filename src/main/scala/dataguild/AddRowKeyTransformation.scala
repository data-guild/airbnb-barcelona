package dataguild

import java.util.UUID

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object AddRowKeyTransformation {

  def transform(df: DataFrame) = {
    val allColumns = df.columns

    val rowKey = udf(() => UUID.randomUUID().toString + unix_timestamp())
    val updatedDf = df.withColumn("rowId", rowKey()).select("rowId", allColumns:_*)
    updatedDf
  }


}
