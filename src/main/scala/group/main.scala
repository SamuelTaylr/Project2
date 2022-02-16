package group

import org.apache.spark.sql.SparkSession

object main {

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

    /*val group.menu = new group.menu
    group.menu.selectionMenu(spark)*/

    /*val sam = new sam
    sam.dataLoader(spark)*/

    val covidDF = mark.dataLoader(spark)
    covidDF.show()
    mark.sparkQuery1(covidDF, spark)
  }
}
