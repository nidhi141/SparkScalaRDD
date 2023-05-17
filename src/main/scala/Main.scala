import org.apache.log4j.Logger

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello world!")

    Logger.getLogger("org").setLevel(LEVEL.ERROR)
  }
}
