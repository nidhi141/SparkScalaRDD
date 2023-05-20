import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Encoder, SparkSession}
import org.codehaus.jettison.json.JSONObject

import java.net.URL
import java.io.{File, PrintWriter}
import scala.io.Source

object LinkJsonMap extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  //val startTime = System.nanoTime()

  // Create SparkSession
  val spark = SparkSession
    .builder()
    .appName("Nested JSON Flatten")
    .master("local[*]")
    .getOrCreate()

  val localFilePath = "D:/Scala/json/jsonexurl.json"
  val url = "https://gbfs.velobixi.com/gbfs/gbfs.json?_ga=2.177175653.1358935761.1684606791-24734961.1684606791"

  def CreateFileFromURL(inputFilePath : String, url : String): Unit = {
    val file = new File(inputFilePath)

    if (file.exists()) {
      println("File already exists.")
    }
    else {
      val source = Source.fromURL(url)
      val content = source.mkString
      source.close()

      val writer = new PrintWriter(file)
      writer.write(content)
      writer.close()

      println("File downloaded and saved successfully.")
    }
  }

  CreateFileFromURL(localFilePath,url)

  // Read the local file using Spark
  val df = spark.read
    .option("multiline", "true")
    .json(localFilePath)

  // df.show(false)
  //df.printSchema()

  val feedsArrayDF: DataFrame = df.select(explode(col("data.en.feeds")))

  val feedsDF: DataFrame = feedsArrayDF.select("col.*")

  /*  feedsDF.show(false)
  feedsDF.printSchema()
  println(feedsDF.count())*/

  // Convert selected columns to a HashMap
  val hashMap: Map[String, String] = feedsDF
    .collect()
    .map(row => (row.getString(0), row.getString(1)))
    .toMap

  // Print the HashMap
  /*  hashMap.foreach { case (key, value) =>
    println(s"Key: $key, Value: $value")
  }*/

  val pathValue = hashMap.apply("system_information")
  println(pathValue)

  val FileLoc = "D:/Scala/temp/newFile.json"
  CreateFileFromURL(FileLoc,pathValue)

  val newDF = spark.read
    .option("multiline", "true")
    .json(FileLoc)

  newDF.show(false)
  newDF.printSchema()

}


  /* Another way:
  val url = "https://gbfs.velobixi.com/gbfs/gbfs.json?_ga=2.177175653.1358935761.1684606791-24734961.1684606791"
  val result = scala.io.Source.fromURL(url).mkString
  //only one line inputs are accepted. (I tested it with a complex Json and it worked)
  val jsonResponseOneLine = result.stripLineEnd
  //You need an RDD to read it with spark.read.json! This took me some time. However it seems obvious now
  val jsonRdd = spark.sparkContext.parallelize(jsonResponseOneLine :: Nil)
  val df = spark.read.json(jsonRdd)
  df.show(false)
  df.printSchema()*/

/*  val endTime = System.nanoTime()
 val executionTime = (endTime - startTime) / 1000000  // Convert to milliseconds

 println(s"Execution time: $executionTime ms")*/


