package group

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class jake {

  def printName(): Unit ={
    println("+" + ("=" * 49) + "+" +
      s"""\nJake
         |""".stripMargin + "+" + ("=" * 49) + "+")
  }

  def dataLoader(spark: SparkSession): Unit = {

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
//    val sqlDf = spark.sql("select * from Covid where Obs_Date Between '2020-01-22' and '2020-02-10' and Country = 'US'   ")
//    sqlDf.show(300)

//    covidDF.show(300)

    //Shows Data types of modifiedDF as an array
    println(covidDF.dtypes.mkString("Array(", ", ", ")"))

    //Query 1 - Select * FROM covidDF WHERE Country = 'China' and Between date Jan to May
    println("+" + ("=" * 49) + "+" +
      s"""\nDebug Query 1 start
         |""".stripMargin + "+" + ("=" * 49) + "+")
    covidDF.createTempView("covidDF")
//    Mainland China
    spark.sql("SELECT count(Obs_Date) FROM covidDF WHERE Obs_Date BETWEEN '2020-01-22' and '2020-06-31' and Country = 'Mainland China'").show()
    spark.sql("SELECT * FROM covidDF WHERE Obs_Date BETWEEN '2020-01-22' and '2020-06-31' and Country = 'Mainland China'").show()
//
    spark.sql("SELECT count(Obs_Date) FROM covidDF WHERE Obs_Date BETWEEN '2020-03-22' and '2020-08-31' and Country = 'Mainland China'").show()
    spark.sql("SELECT * FROM covidDF WHERE Obs_Date BETWEEN '2020-03-1' and '2020-09-31' and Country = 'Vietnam'").show()

    println("+" + ("=" * 49) + "+" +
      s"""\nDebug Query 1 end
         |""".stripMargin + "+" + ("=" * 49) + "+")

  }
}
