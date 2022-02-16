package group

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.to_date

object mark {

  def dataLoader(spark: SparkSession): DataFrame = {
    import spark.implicits._

    //Creating initial DataFrame from csv file
    val dfTest = spark.read.option("header", true).option("inferSchema", true).format("csv").load(
      "input/covid_19_data.csv").toDF("Id", "Obs_Date", "State", "Country", "Update", "Confirmed",
      "Deaths", "Recovered")

    val covidDF = dfTest.withColumn("Obs_Date", to_date($"Obs_Date", "MM/dd/yyyy"))

    return covidDF

  }

  def sparkQuery1(inputDF : DataFrame, spark : SparkSession) = {
    inputDF.createOrReplaceTempView("query1")
    val result = spark.sql("SELECT Obs_Date, SUM(Confirmed) FROM query1 GROUP BY Obs_Date ")
    result.show()
  }

}
