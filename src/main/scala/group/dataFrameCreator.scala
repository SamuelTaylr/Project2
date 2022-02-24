package group

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.types.DateType

object dataFrameCreator {

  def dataLoader(spark: SparkSession): DataFrame = {
    import spark.implicits._

    //Creating initial DataFrame from csv file
    val dfTest = spark.read.option("header", true).option("inferSchema", true).format("csv").load(
      "input/covid_19_data.csv").toDF("Id", "Obs_Date", "State", "Country", "Update", "Confirmed",
      "Deaths", "Recovered")

    val covidDF = dfTest.withColumn("Obs_Date", to_date($"Obs_Date", "MM/dd/yyyy")).persist()

    //Backup for Zeppelin
    /*import org.apache.spark.sql.SparkSession
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("Spark Works Y'all")
    spark.sparkContext.setLogLevel("ERROR")

    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")




    val dfTest = spark.read.option("header", true).option("inferSchema", true).format("csv").load(
      "covid_19_data.csv").toDF("Id", "Obs_Date", "State", "Country", "Update", "Confirmed",
      "Deaths", "Recovered")


    val dfTest3 = dfTest.withColumn("Obs_date", to_date($"Obs_Date", "MM/dd/yyyy"))





    dfTest3.createOrReplaceTempView("Covid")*/

    return covidDF

  }

}
