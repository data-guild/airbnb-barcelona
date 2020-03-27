package dataguild

import java.io.File

import org.scalatest.{BeforeAndAfterEach, FunSpec, Matchers}

import scala.reflect.io.Directory

class FileWriterTest extends FunSpec with TestSparkSessionWrapper with Matchers with BeforeAndAfterEach{
  import spark.implicits._
  spark.conf.set("pathToSave", "src/test/data/")

  override def afterEach(): Unit = {
    removeFolder(spark.conf.get("pathToSave"))
  }

  it("should write parquet file without exception"){
    val testDF = Seq("a").toDF()
    val path = spark.conf.get("pathToSave")
    val folder = ""
    FileWriter.write(testDF, "parquet", s"$path$folder")
    val outputFolder = new File(s"$path$folder")
    assert(outputFolder.exists)
  }

  it("should partition parquet file by current date") {
    val testDF = Seq(
      ("a", 1),
      ("b", 2),
      ("c", 3)
    ).toDF("col1", "col2")

    val path = spark.conf.get("pathToSave")
    val folder = "partition_test"

    val partitionCols = Seq("col1")
    FileWriter.write(testDF, "parquet", s"$path$folder", partitionCols)

    val output = new File(s"$path$folder")
//    val directoryList = output.listFiles(_.isDirectory)
//    directoryList.size should be(3)
  }

  def removeFolder(folderPath: String): Unit ={
    val file = new File(folderPath)
    val directory = new Directory(file)
    directory.deleteRecursively()
  }
}
