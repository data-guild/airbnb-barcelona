package dataguild

import java.text.SimpleDateFormat
import java.util.Calendar

import org.scalatest.{FunSpec, Matchers}

class AddCurrentDateTransformationSpec extends FunSpec with TestSparkSessionWrapper with Matchers {
  import spark.implicits._

  it("should add currentDate to input dataframe") {
    val sourceDF = Seq(
      8,
      64,
      27
    ).toDF("id")

    val format = new SimpleDateFormat("yyyyMMdd")
    val currentDate = format.format(Calendar.getInstance().getTime())

    val expectedDF = Seq(
      (8, currentDate),
      (64, currentDate),
      (27, currentDate)
    ).toDF("id", "currentDate")

    val actualDF = AddCurrentDateTransformation.transform(sourceDF)

    expectedDF.except(actualDF).count() should be(0)
  }

}
