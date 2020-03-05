package dataguild

import org.scalatest.{FunSpec, Matchers}

class ReplaceStringTransformationSpec extends FunSpec with TestSparkSessionWrapper with Matchers {

  import spark.implicits._

  it("should replace character in specified columns from dataframe") {
    val sourceDF = Seq(
      (8, "bat", "$150.0"),
      (64, "mouse", "123.4"),
      (27, "horse", "$340.1")
    ).toDF("id", "name", "price")

    val expectedDf = Seq(
      (8, "bat", "150.0"),
      (64, "mouse", "123.4"),
      (27, "horse", "340.1")
    ).toDF("id", "name", "price")

    val actualDF = ReplaceStringTransformation.
      replaceString(sourceDF, "price", "$", "")

    val exPriceList = expectedDf.select("price").collect()
    val exArrayString = exPriceList.map(row => row.getString(0))

    val actPriceList = actualDF.select("price").collect()
    val actArrayString = actPriceList.map(row => row.getString(0))

    exArrayString.mkString(",") shouldBe actArrayString.mkString(",")

  }

}
