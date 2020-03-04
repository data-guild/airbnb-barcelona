package dataguild

import org.apache.spark.sql.DataFrame

object DataTypeValidation {
  def validate(df: DataFrame): Unit ={
    print(df.columns.length)
    df.printSchema()
    df.show()
  }
}
