package dataguild

import dataguild.caseclass.DataColumn
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.scalatest.{FunSpec, Matchers}

class DataTypeValidationSpec extends FunSpec with TestSparkSessionWrapper with Matchers {

  import spark.implicits._

  it("typeValidation test returns error dataframe with no errors") {
    val sourceDF = Seq(
      ("aa", "8"),
      ("bb", "hello"),
      ("cc", "27")
    ).toDF("rowId", "price")

    val testSchema = List(DataColumn("rowId",  "String"),
      DataColumn("price", "Double"))

    val (validDf, errorDf) = DataTypeValidation.validate(sourceDF, testSchema)

    validDf.columns should contain("rowId")
    errorDf.columns should contain("rowId")
    validDf.select("rowId").count() should be(2)
    errorDf.select("rowId").count() should be(1)
  }

}
