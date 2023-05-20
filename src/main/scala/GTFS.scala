import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object GTFS extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName("MyApp").master("local[*]").getOrCreate()

  //Routes DataFrame
  val routePath = "D:/Scala/gtfs_stm/routes.txt"
  val routeDF: DataFrame= spark.read.option("header", "true")
    .option("inferSchema", "true").csv(routePath)
  println(routeDF.count())
  routeDF.show()

  //Trips DataFrame
  val tripPath = "D:/Scala/gtfs_stm/trips.txt"
  val tripDF: DataFrame = spark.read.option("header", "true")
    .option("inferSchema", "true").csv(tripPath)
  println(tripDF.count())
  tripDF.show()

  //Join Routes and Trips
  val routeTripDF: DataFrame = routeDF.join(tripDF,routeDF("route_id") === tripDF("route_id"))
  val joinedDFWithoutDuplicate : DataFrame= routeTripDF.drop(tripDF("route_id"))
  println(joinedDFWithoutDuplicate.count())
  joinedDFWithoutDuplicate.show()

  //Calendar DataFrame
  val calendarPath = "D:/Scala/gtfs_stm/calendar.txt"
  val calendarDF: DataFrame = spark.read.option("header", "true")
    .option("inferSchema", "true").csv(calendarPath)
  println(calendarDF.count())
  calendarDF.show()

  //Join RouteTrip and Calendar
  val routeTripCalendarDF: DataFrame = joinedDFWithoutDuplicate.join(calendarDF,
      joinedDFWithoutDuplicate("service_id") === calendarDF("service_id"))
  val joinedRTCDFWithoutDuplicate: DataFrame = routeTripCalendarDF.drop(tripDF("service_id"))
  println(joinedRTCDFWithoutDuplicate.count())
  joinedRTCDFWithoutDuplicate.show()

  // Save DataFrame as CSV
  joinedRTCDFWithoutDuplicate.write
    .format("csv")
    .option("header", "true") // Include header in the output file
    .mode("overwrite") // Overwrite the output file if it already exists
    .save("D:/Scala/outputFiles/RTC_DF.csv")

  spark.stop()
}
