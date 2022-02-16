package group

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class jake {

  def printName(): Unit ={
    println("+" + ("=" * 49) + "+" +
      s"""\nJake
         |""".stripMargin + "+" + ("=" * 49) + "+")
  }

  def dataLoader(spark: SparkSession): DataFrame = {

    import spark.implicits._

    //Creating initial DataFrame from csv file
    val dfTest = spark.read.option("header",true).option("inferSchema",true).format("csv").load(
      "input/covid_19_data.csv").toDF("Id", "Obs_Date","State","Country","Update","Confirmed",
      "Deaths","Recovered")

    //Changing data type of Obs_Date column to "DateType" -- This will be the main Data Frame that we draw our queries from
    val covidDF = dfTest.withColumn("Obs_Date", to_date($"Obs_Date", "MM/dd/yyyy"))

    //Creating temporary view "Covid" from covidDF
    covidDF.createOrReplaceTempView("Covid")

    //Testing selecting between a date range, it works as intended
    val sqlDf = spark.sql("select * from Covid where Obs_Date Between '2020-01-22' and '2020-02-10' and Country = 'US'   ")
    sqlDf.show(300)

    covidDF.show(10)

    //Shows Data types of modifiedDF as an array
    println(covidDF.dtypes.mkString("Array(", ", ", ")"))

    //Query 1 - Select * FROM covidDF WHERE Country = 'China' and Between date Jan to May
    covidDF.select()

  }
}
