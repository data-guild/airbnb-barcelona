package dataguild

import dataguild.caseclass.DataColumn
import org.scalatest.{FunSpec, Matchers}

class DataTypeValidationSpec extends FunSpec with TestSparkSessionWrapper with Matchers {

  import spark.implicits._

  it("typeValidation test returns error dataframe with no errors") {
    val sourceDF = Seq(
      ("aa", "8", "2020-01-22"),
      ("bb", "hello", "2020-02-22"),
      ("cc", "27", "2020-01-12")
    ).toDF("rowId", "price", "currentDate")

    val testSchema = List(DataColumn("rowId", "String"),
      DataColumn("price", "Double"),
      DataColumn("currentDate", "Date", "yyyy-MM-dd", false)
    )

    val (validDf, errorDf) = DataTypeValidation.validate(sourceDF, testSchema)

    validDf.columns should contain("rowId")
    errorDf.columns should contain("rowId")
    validDf.select("rowId").count() should be(2)
    errorDf.select("rowId").count() should be(1)
  }

}
