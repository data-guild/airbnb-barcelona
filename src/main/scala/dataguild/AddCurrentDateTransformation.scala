package dataguild

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object AddCurrentDateTransformation {
  def transform(sourceDF: DataFrame): DataFrame = {
    val format = new SimpleDateFormat("yyyyMMdd")
    val currentDate = format.format(Calendar.getInstance().getTime())

    sourceDF.withColumn("currentDate", lit(currentDate))
  }

}
