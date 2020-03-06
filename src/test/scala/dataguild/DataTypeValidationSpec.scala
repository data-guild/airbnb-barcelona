package dataguild
import org.scalatest.{FunSpec, Matchers}

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
    errorDf.columns should contain("rowId")
    validDf.select("rowId").count() should be(2)
    errorDf.select("rowId").count() should be(1)
  }

}
