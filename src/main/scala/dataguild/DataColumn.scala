package dataguild

import org.apache.spark.sql.types.DoubleType

case class DataColumn(name: String, dType: String, format:String = "", nullable:Boolean=true)

