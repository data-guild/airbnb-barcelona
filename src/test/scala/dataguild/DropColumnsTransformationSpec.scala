package dataguild

import org.scalatest.{FunSpec, Matchers}

class DropColumnsTransformationSpec extends FunSpec with TestSparkSessionWrapper with Matchers {

  import spark.implicits._

  it("should drop unused columns from dataframe") {
    val sourceDF = Seq(
      (8, "bat", 150.0),
      (64, "mouse", 123.4),
      (27, "horse", 340.1)
    ).toDF("id", "name", "price")

    val expectedDF = Seq(
      (8, 150.0),
      (64, 123.4),
      (27, 340.1)
    ).toDF("id", "price")

    val actualDF = DropColumnsTransformation.dropColumn(sourceDF, Seq("name"))

    expectedDF.except(actualDF).count() should be(0)
  }

}
