import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object JsonEx extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Create SparkSession
  val spark = SparkSession
      .builder()
      .appName("Nested JSON")
      .master("local[*]")
      .getOrCreate()

  // Read JSON data into DataFrame
  val df : DataFrame = spark.read
      .option("multiline","true")
      .json("D:/Scala/json/nested.json")

  df.printSchema()

  val itemsdf = df.withColumn("items",explode(col("items")))
/*  itemsdf.show(false)
  itemsdf.printSchema()*/

  val ndf: DataFrame = itemsdf.select(
    "order_id",
    "customer.*",
    "customer.address.*",
    "items.*"
  ).drop("address")

  ndf.show(false)
  ndf.printSchema()
}
