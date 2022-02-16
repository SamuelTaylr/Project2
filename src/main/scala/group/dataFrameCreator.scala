package group

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.to_date

object dataFrameCreator {

  def dataFrame(): Unit = {

    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("Spark Works Y'all")

    import spark.implicits._

    //Creating initial DataFrame from csv file
    val dfTest = spark.read.option("header", true).option("inferSchema", true).format("csv").load(
      "input/covid_19_data.csv").toDF("Id", "Obs_Date", "State", "Country", "Update", "Confirmed",
      "Deaths", "Recovered")

    //Changing data type of Obs_Date column to "DateType"
    val covidDF = dfTest.withColumn("Obs_Date", to_date($"Obs_Date", "MM/dd/yyyy"))

    return covidDF
  }

}
