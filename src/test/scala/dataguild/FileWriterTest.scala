package dataguild

import java.io.File

import org.scalatest.{FunSpec, Matchers}

import scala.reflect.io.Directory

class FileWriterTest extends FunSpec with TestSparkSessionWrapper with Matchers{
  import spark.implicits._
  spark.conf.set("pathToSave", "src/test/data")

  it("should write parquet file without exception"){
    val testDF = Seq("a").toDF()
    val path = spark.conf.get("pathToSave")
    val folder = ""
    FileWriter.write(testDF, "parquet", s"$path$folder")
    val outputFolder = new File(s"$path$folder")
    assert(outputFolder.exists)
    removeFolder(outputFolder)

  }

  def removeFolder(folderPath: File): Unit ={
    val directory = new Directory(folderPath)
    directory.deleteRecursively()
  }
}
