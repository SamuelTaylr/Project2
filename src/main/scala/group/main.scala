package group

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

object main {

  System.setProperty("hadoop.home.dir", "C:\\winutils")

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

    /*val sam = new sam
    sam.dataLoader(spark)*/

    val covidDf = mark.dataLoader(spark)
    //mark.sparkQuery1(covidDf, spark)

    /* --I created the following to test my attempt at login verification
    covidDf.createOrReplaceTempView("toTest")

    val testing = spark.sql("select case when count(country) >= 1 then true else false end as toBool from toTest where country = 'US'")
    val boolTest = testing.first().get(0)
    val finalBool = boolTest.asInstanceOf[Boolean]
    println(finalBool)
    if(finalBool) {println("this works")} else {println("nope")}*/

    //mark.avgCasesByMonth(covidDf, spark).show()
    //mark.avgCasesByQuarter(covidDf, spark).show()

    val test = mark.queryToDataSet(covidDf, spark)
    test.show()

  }
}