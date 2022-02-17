package group

import org.apache.spark.sql.SparkSession

object main {

  System.setProperty("hadoop.home.dir", "C:\\winutils")
  val spark = SparkSession
    .builder()
    .appName("Project2")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  def main(args: Array[String]): Unit = {

    //    val sam = new sam
    //    sam.dataLoader(spark)
    //    println("Hello World")
    //    println("This is a change ")

    //    start test
    val jake = new jake
    jake.printName()
    jake.dataLoader(spark)


  }
}
