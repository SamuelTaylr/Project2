package Package2

import org.apache.spark.sql.SparkSession

object main {

  //System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
  System.setProperty("hadoop.home.dir", "C:\\winutils")


  val spark = SparkSession
    .builder
    .appName("hello hive")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  println("Spark Works Y'all")
  spark.sparkContext.setLogLevel("ERROR")



  def main(args: Array[String]): Unit = {

    val menu = new menu
    menu.selectionMenu(spark)

    val sam = new mandeep
    sam.dataLoader(spark)


  }
}