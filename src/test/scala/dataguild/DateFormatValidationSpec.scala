package dataguild

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.scalatest.{FunSpec, Matchers}

class DateFormatValidationSpec extends FunSpec with TestSparkSessionWrapper with Matchers {

  import spark.implicits._

  it("should reformat date to yyyyMMdd") {
    val sourceDF = Seq(
      "2020-01-01",
      "2020/01/02",
      "2020-31-03",
    ).toDF("date")

    val expectedErrorDf = Seq(
      "2020/01/02",
      "2020-31-03"
    ).toDF("date")

    val expectedValidDf = Seq(
      "2020-01-01"
    ).toDF("date")

    val (errorDf, validDf) = DateFormatValidation.validate(sourceDF, "date", "yyyy-MM-dd")

    errorDf.collect().length shouldBe 2
    validDf.collect().length shouldBe 1



  }
}
