package dataguild


import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{FunSpec, Matchers}
import org.apache.spark.sql.types.IntegerType

import scala.util.{Failure, Success, Try}

class DataTypeValidationSpec extends FunSpec with TestSparkSessionWrapper with Matchers {

  import spark.implicits._
  it("typeValidation test returns error dataframe with no errors") {
    val sourceDF = Seq(
      ("aa", "8"),
      ("bb", "hello"),
      ("cc", "27")
    ).toDF("rowId", "price")


    val (validDf, errorDf) = DataTypeValidation.validate(sourceDF)

    validDf.columns should contain("rowId")
    errorDf.columns should not contain("rowId")
    errorDf.select("rowId").count() should be(0)
  }

}
