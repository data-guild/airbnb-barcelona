package dataguild.caseclass

case class Replacement(columnName: String,
                       source: String,
                       target: String,
                       isRegex: Boolean = false)
