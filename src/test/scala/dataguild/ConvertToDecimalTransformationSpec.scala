package dataguild

import org.scalatest.{FunSpec, Matchers}

class ConvertToDecimalTransformationSpec extends  FunSpec with TestSparkSessionWrapper with Matchers {

  import spark.implicits._

  it("should convert numerical value to decimal respectively") {
    val sourceDf = Seq("100", "90").toDF("some_col")
    val expectedDf = Seq("1.0", "0.9").toDF("some_col")

    val expectedValueString = expectedDf.select("some_col").collect().map(row => row.getString(0))

    val actualDf = ConvertToDecimalTransformation.transform(sourceDf, "some_col")

    val actualValueString = actualDf.select("some_col").collect().map(row => row.getString(0))

    expectedValueString.mkString(",") shouldBe actualValueString.mkString(",")
  }

  it("should not convert if it is not parse-able into a decimal value") {
    val sourceDf = Seq(
      ("100", "90"),
      ("ten", "80"))
      .toDF("col1", "col2")

    val expectedDf = Seq(
      ("1.0", "0.9"),
      ("ten", "0.8"))
      .toDF("col1", "col2")

    val expectedValueString = expectedDf.select("col1", "col2").collect().map(row => row.getString(0) + row.getString(1))

    val actualDf = ConvertToDecimalTransformation.transform(sourceDf, "col1", "col2")
    val actualValueString = actualDf.select("col1", "col2").collect().map(row => row.getString(0) + row.getString(1))

    expectedValueString.mkString(",") shouldBe actualValueString.mkString(",")
  }



}
