package dataguild

import java.util.{Calendar, UUID}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object AddRowKeyTransformation {

  def transform(df: DataFrame) = {
    val allColumns = df.columns

    val rowKey = udf(() => UUID.randomUUID().toString + Calendar.getInstance().getTimeInMillis)

    val updatedDf = df.withColumn("rowId", rowKey()).select("rowId", allColumns: _*)
    updatedDf
  }


}
