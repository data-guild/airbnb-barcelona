package dataguild

import dataguild.caseclass.DataColumn
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, DateType, DoubleType, IntegerType, StringType}
import org.scalatest.{FunSpec, Matchers}

class SchemaSpec extends FunSpec with Matchers with TestSparkSessionWrapper{
  it("should return a dataframe with specific schema") {

    import spark.implicits._

    val validDf = Seq(
      ("aa", "8.0", "2020-02-12","true"),
      ("cc", "27.0", "2020-01-11","false")
    ).toDF("rowId", "price", "date", "verified")

    val testSchema = List(
      DataColumn("rowId", "String"),
      DataColumn("price", "Double"),
      DataColumn("date", "Date", "yyyy-MM-dd"),
      DataColumn("verified", "Boolean"),
    )

    val finalDf = Schema.generateDfWithSchema(validDf, testSchema)

    assert(finalDf.schema("price").dataType == DoubleType)
    assert(finalDf.schema("rowId").dataType == StringType)
    assert(finalDf.schema("date").dataType == DateType)
    assert(finalDf.schema("verified").dataType == BooleanType)

  }

}
