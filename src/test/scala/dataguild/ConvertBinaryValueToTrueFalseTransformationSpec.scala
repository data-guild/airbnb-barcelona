package dataguild

import org.scalatest.{FunSpec, Matchers}

class ConvertBinaryValueToTrueFalseTransformationSpec extends FunSpec with TestSparkSessionWrapper with Matchers {

  import spark.implicits._

  it("should convert t and f to true and false respectively") {
    val sourceDf = Seq("t", "f").toDF("some_col")
    val expectedDf = Seq("true", "false").toDF("some_col")

    val expectedValueString = expectedDf.select("some_col").collect().map(row => row.getString(0))

    val actualDf = ConvertBinaryValueToTrueFalseTransformation.transform(sourceDf, "some_col")
    val actualValueString = actualDf.select("some_col").collect().map(row => row.getString(0))

    expectedValueString.mkString(",") shouldBe actualValueString.mkString(",")
  }

  it("should not convert if value is not t or f") {
    val sourceDf = Seq(
      ("t", "f"),
      ("ten", "friend"))
      .toDF("col1", "col2")

    val expectedDf = Seq(
      ("true", "false"),
      ("ten", "friend"))
      .toDF("col1", "col2")

    val expectedValueString = expectedDf.select("col1", "col2").collect().map(row => row.getString(0) + row.getString(1))

    val actualDf = ConvertBinaryValueToTrueFalseTransformation.transform(sourceDf, "col1", "col2")
    val actualValueString = actualDf.select("col1", "col2").collect().map(row => row.getString(0) + row.getString(1))

    expectedValueString.mkString(",") shouldBe actualValueString.mkString(",")
  }
}
