package dataguild

import org.scalatest.{FunSpec, Matchers}

class AddRowKeyTransformationSpec extends FunSpec with TestSparkSessionWrapper with Matchers {

  import spark.implicits._

  it("should add RowKey to each row") {
    val sourceDF = Seq(
      (8, "bat", 150.0),
      (64, "mouse", 123.4),
      (27, "horse", 340.1)
    ).toDF("id", "name", "price")

    val updatedDF = AddRowKeyTransformation.transform(sourceDF)

    updatedDF.columns should contain ("rowId")
    updatedDF.columns(0) should be ("rowId")
  }
}
