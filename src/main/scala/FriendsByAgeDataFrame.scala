import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._

object FriendsByAgeDataFrame extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder()
    .appName("FriendsByAgeDataFrame")
    .master("local[*]")
    .getOrCreate()

  val path = "D:/Scala/Datasets/fakefriends.csv"
  val friends_df: DataFrame = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(path)
  //  Filter Friends by Age using Spark and SQL
  val filtered_friends = friends_df.filter(friends_df("Age") > 20 and friends_df("Age") < 30)
  val filtered_friends_1 = friends_df.filter("Age > 20 and Age < 30")
  friends_df.createOrReplaceTempView("friends")

  val query =
    """
      Select *
      |From friends
      |Where Age > 20 and Age < 30
      |""".stripMargin

  val sql_df = spark.sql(query)

  //  Select Age and NumOfFriends & apply column level conditions and transformations
  val subset_friends = friends_df.select("Age", "NumOfFriends")
  val num_friends_changed = subset_friends
    .withColumn("NumOfFriends",
      when(friends_df("Age") > 40, (friends_df("NumOfFriends") + 10))
        .otherwise(when(friends_df("Age") > 30 and friends_df("Age") <= 40, (friends_df("NumOfFriends") + 20))
          .otherwise(friends_df("NumOfFriends"))
        ) )
  val friends_by_age = subset_friends
    .groupBy("Age")
    .agg(round(avg("NumOfFriends")).cast("int").alias("friends_avg"))
    .sort("Age").show()



  spark.stop()
}