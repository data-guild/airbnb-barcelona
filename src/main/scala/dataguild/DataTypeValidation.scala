package dataguild

import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType}

import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.functions._

object DataTypeValidation {

  def validate(df: DataFrame): (DataFrame, DataFrame) = {

    val schema = List(DataColumn("rowId", "String"), DataColumn("price", "Double"))

    val spark: SparkSession =  SparkSession.getActiveSession.get
    //df.rdd.map(row =>{ })
    val dfColumns = df.columns.map(each => col(each))
    val dfWithErrors = df.withColumn("errors", ValidateUDF.validateRow(schema)(struct(dfColumns:_*)))

    //dfWithErrors.show()

    val errorsDf = dfWithErrors.select("errors")

    //df.join(errorsDf, "rowId", "anti-join")

    errorsDf.show(false)
    errorsDf.printSchema()

    //val finalErrors = errorsDf
    //implicit val encoder=Encoders.bean(classOf[ErrorMessage])
    //val finalErrors = explode(col("errors"))"

    val finalErrors =errorsDf.withColumn("errorsDf", explode(col("errors"))).

    //finalErrors.collect().foreach(println)

    //finalErrors.printSchema()

    //finalErrors.show(false)


    (df, spark.emptyDataFrame)

//    (df, errorDf)
  }

}

case class ErrorMessage(rowId: String, columnName: String, errorMessage: String)

object ValidateUDF{
  def validateRow(schema: List[DataColumn]) = udf ((row: Row) =>{
    val dfTupleList = schema.flatMap(dataColumn =>{
      val result = dataColumn.dType match {
        case "Double" => Try(row.getString(row.fieldIndex(dataColumn.name)).toDouble)
        //case _ => Try(row.getString(row.fieldIndex(dataColumn.name)))
        case _ => Failure(new Exception("Some random exception"))
      }

      result match {
        case Success(_) => List.empty
        case Failure(_) =>List(ErrorMessage(row.getString(0), dataColumn.name, "cannot do whatever" ))
      }
    })
    dfTupleList
  })
}