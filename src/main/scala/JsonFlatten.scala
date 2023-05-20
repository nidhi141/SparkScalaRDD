import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object JsonFlatten extends App{

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create SparkSession
    val spark = SparkSession
      .builder()
      .appName("Nested JSON Flatten")
      .master("local[*]")
      .getOrCreate()

    // Read JSON data into DataFrame
    val df : DataFrame = spark.read
      .option("multiline","true")
      .json("D:/Scala/json/file5.json")

   // df.show(false)
  // df.printSchema()

  val restArrayDF : DataFrame = df.select(explode(col("restaurants")))

  val restDF: DataFrame = restArrayDF.select(
    "col.restaurant.*"
  )

  restDF.show(false)
  restDF.printSchema()
  println(restDF.count())
}
