package group

import org.apache.spark.sql.SparkSession

object main {

  //System.setProperty("hadoop.home.dir", "C:\\winutils")
  System.setProperty("hadoop.home.dir", "C:\\hadoop\\")

  val spark = SparkSession
    .builder
    .appName("hello hive")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  println("Spark Works Y'all")
  spark.sparkContext.setLogLevel("ERROR")


  def main(args: Array[String]): Unit = {

    /*val menu = new menu
    menu.selectionMenu(spark)*/

    val sam = new sam
    sam.dataLoader(spark, dataFrameCreator.dataLoader(spark))


  }
}
