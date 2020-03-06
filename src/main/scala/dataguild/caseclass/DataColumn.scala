package dataguild.caseclass

import org.apache.spark.sql.types.DataType

case class DataColumn(name: String, dType: String, format:String = "", nullable:Boolean=true)
