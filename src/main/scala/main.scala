import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

object main {

  System.setProperty("hadoop.home.dir", "C:\\hadoop\\")

  val spark = SparkSession
    .builder
    .appName("hello hive")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  println("Spark Works Y'all")

  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")



  def main(args: Array[String]): Unit = {

    println("Hello World")

  }
}