package dataguild

import dataguild.caseclass.Replacement
import org.apache.spark.sql.{AnalysisException, DataFrame}
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

    val replacement = Replacement("price", "$", "")
    val actualDF = ReplaceStringTransformation.replaceString(sourceDF, replacement)

    val expectedPrice = collectColAsString(expectedDf, "price")
    val actualPrice = collectColAsString(actualDF, "price")

    expectedPrice shouldBe actualPrice

  }

  it("should replace character in multiple columns") {
    val sourceDF = Seq(
      (8, "bat", "$150.0", "$10.0"),
      (64, "mouse", "123.4", "$20.0"),
      (27, "horse", "$340.1", "$30.0")
    ).toDF("id", "name", "price", "daily")

    val expectedDf = Seq(
      (8, "bat", "150.0", "10.0"),
      (64, "mouse", "123.4", "20.0"),
      (27, "horse", "340.1", "30.0")
    ).toDF("id", "name", "price", "daily")

    val replacements = List(
      Replacement("price", "$", ""),
      Replacement("daily", "$", ""))

    val actualDF = ReplaceStringTransformation.
      replaceStringMultiColumn(sourceDF, replacements)

    val expectedPrice = collectColAsString(expectedDf, "price")
    val expectedDaily = collectColAsString(expectedDf, "daily")
    val actualPrice = collectColAsString(actualDF, "price")
    val actualDaily = collectColAsString(actualDF, "daily")

    expectedPrice shouldBe actualPrice
    expectedDaily shouldBe actualDaily

  }

  it("should skip cell if valus is null") {
    val sourceDF = Seq(
      (8, "bat", null)
    ).toDF("id", "name", "price")

    val expectedDf = Seq(
      (8, "bat", null)).toDF("id", "name", "price")

    val replacements = Replacement("price", "$", "")

    val actualDF = ReplaceStringTransformation.replaceString(sourceDF, replacements)

    val expectedPrice = collectColAsString(expectedDf, "price")
    val actualPrice = collectColAsString(actualDF, "price")

    expectedPrice shouldBe actualPrice

  }

  it("should create Replacement Case Class given columnName, source and target") {
    val replacement = Replacement("price", "$", "")
    assert(replacement.columnName == "price")
    assert(replacement.source == "$")
    assert(replacement.target == "")
  }

  it("should throw exception when column does not exist") {
    val sourceDF = Seq(
      (8, "bat", "$150.0"),
      (64, "mouse", "123.4"),
      (27, "horse", "$340.1")
    ).toDF("id", "name", "price")

    val replacement = Replacement("nonexisting_col", "$", "")

    assertThrows[AnalysisException] {
      ReplaceStringTransformation.
        replaceString(sourceDF, replacement)
    }
  }

  it("should treat source string as regex when isRegex of Replacement is true") {
    val sourceDF = Seq(
      "ten", "t", "sit"
    ).toDF("some_col")

    val expectedDf = Seq(
      "ten", "true", "sit"
    ).toDF("some_col")

    val replacement = Replacement("some_col", "^t$", "true", isRegex = true)
    val actualDF = ReplaceStringTransformation.replaceString(sourceDF, replacement)

    val expectedPrice = collectColAsString(expectedDf, "some_col")
    val actualPrice = collectColAsString(actualDF, "some_col")

    expectedPrice shouldBe actualPrice
  }

  def collectColAsString(df: DataFrame, columnName: String): String = {
    df
      .select(columnName)
      .collect()
      .map(row => row.getString(0))
      .mkString(",")
  }

}
