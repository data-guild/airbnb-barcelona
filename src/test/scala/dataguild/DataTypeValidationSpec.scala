package dataguild

import org.apache.spark.sql.Row
import org.scalatest.FunSpec
import org.apache.spark.sql.types.IntegerType

class DataTypeValidationSpec extends FunSpec with TestSparkSessionWrapper{
  import spark.implicits._

  it("testing") {
    val sourceDF = Seq(
      (8, "test"),
      (64, "test2"),
      (27, "test3")
    ).toDF("id", "name")

    sourceDF.foreach(row => println(row.getAs[IntegerType]("id")))
  }

}
