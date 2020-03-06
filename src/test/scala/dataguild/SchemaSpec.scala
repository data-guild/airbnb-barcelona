package dataguild

import dataguild.caseclass.DataColumn
import org.apache.spark.sql.types
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.scalatest.{FunSpec, Matchers}

class SchemaSpec extends FunSpec with Matchers with TestSparkSessionWrapper{
  it("should return a dataframe with specific schema") {

    import spark.implicits._

    val validDf = Seq(
      ("aa", 8.0),
      ("cc", 27.0)
    ).toDF("rowId", "price")

    val testSchema = List(DataColumn("rowId", "String"),
      DataColumn("price", "Double"))

    val finalDf = Schema.generateDfWithSchema(validDf, testSchema)
    finalDf.printSchema()
  }
}
