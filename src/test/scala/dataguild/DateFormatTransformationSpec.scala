package dataguild

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.scalatest.{FunSpec, Matchers}

class DateFormatTransformationSpec extends FunSpec with TestSparkSessionWrapper with Matchers {

  import spark.implicits._

  it("should reformat date to yyyyMMdd") {
    val sourceDF = Seq(
      ("2020-01-01"),
      ("2020-01-02"),
      ("2020-01-03"),
    ).toDF("date")

    val dateInput = "2020-01-31"

    //    val date = dateFormat.parse(dateInput)
    val date = LocalDate.parse(dateInput, DateTimeFormatter.BASIC_ISO_DATE)

    println(date)
    //
    //    val actualDF = ReplaceStringTransformation.
    //      replaceString(sourceDF, "price", "$", "")
    //
    //    val exPriceList = expectedDf.select("price").collect()
    //    val exArrayString = exPriceList.map(row => row.getString(0))
    //
    //    val actPriceList = actualDF.select("price").collect()
    //    val actArrayString = actPriceList.map(row => row.getString(0))
    //
    //    exArrayString.mkString(",") shouldBe actArrayString.mkString(",")

  }
}
