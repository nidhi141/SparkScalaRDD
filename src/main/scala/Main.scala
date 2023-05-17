import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello world!")

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("MyApp")
      .master("local[*]")
      .getOrCreate()

    val path = "D:/Scala/Datasets/fakefriends.csv"
    val friends_df: DataFrame = spark.read.option("inferSchema","true").csv(path)

    //Filter no. of friends based on Age
    val subset_friends = friends_df.select(
      friends_df("_c2").alias("Age"),
      friends_df("_c3").alias("NoOfFriends")
    )
    /*  subset_friends.show()
      subset_friends.printSchema()*/

    val avg_df1 = subset_friends.groupBy("Age")
      .agg(round(avg("NoOfFriends"),0).alias("AvgFriends"))
    avg_df1.show()

    val friends_grps = avg_df1
      .withColumn("AgeGroup",
        when(col("Age") < 21, "Less than 21")
          .when(col("Age") >= 21 and col("Age") <= 35, "21 to 35")
            .when(col("Age") > 35 and col("Age") <= 50, "36 to 50")
              .when(col("Age") > 50 and col("Age") <= 70, "51 to 70")
                .otherwise(when(col("Age") > 70, "Greater than 70"))
            )
    //friends_grps.show()

    val avg_df2 = friends_grps.select("AgeGroup", "AvgFriends")
        .groupBy("AgeGroup")
          .agg(round(avg("AvgFriends"),0).alias("AvgFriendsByAgeGroup"))
    avg_df2.show()

    val overallAvg = round(avg(avg_df2.col("AvgFriendsByAgeGroup"))).alias("AverageFriends")
    println("Average No. of Friends of all ages:")
    avg_df2.select(overallAvg).show()

  }
}
