package group

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.to_date

object dataFrameCreator {

  def dataLoader(spark: SparkSession): DataFrame = {
    import spark.implicits._

    //Creating initial DataFrame from csv file
    val dfTest = spark.read.option("header", true).option("inferSchema", true).format("csv").load(
      "input/covid_19_data.csv").toDF("Id", "Obs_Date", "State", "Country", "Update", "Confirmed",
      "Deaths", "Recovered")

    val covidDF = dfTest.withColumn("Obs_Date", to_date($"Obs_Date", "MM/dd/yyyy"))

    return covidDF

  }

}
