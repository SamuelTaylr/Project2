package group

import group.main.spark
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

class jake {
//  case class monthsOfYear(months_of_year: String)
  def printName(): Unit ={
    println("+" + ("=" * 49) + "+" +
      s"""\nJake
         |""".stripMargin + "+" + ("=" * 49) + "+")
  }

  def dataLoader(spark: SparkSession): Unit = {


    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val spark = SparkSession
      .builder()
      .appName("Project2")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
//    spark.sparkContext.setLogLevel("ERROR")
//    println("Spark Works Y'all")
    import spark.implicits._

    //Creating initial DataFrame from csv file
    val dfTest = spark.read.option("header",true).option("inferSchema",true).format("csv").load(
      "input/covid_19_data.csv").toDF("Id", "Obs_Date","State","Country","Update","Confirmed",
      "Deaths","Recovered").persist()

    //months of year DF to work with
    val monthsDF = spark.read.option("header",true).option("inferSchema",true).format("csv").load(
      "input/months_of_year.csv").toDF().persist()

    //Changing data type of Obs_Date column to "DateType" -- This will be the main Data Frame that we draw our queries from
    val covidDF = dfTest.withColumn("Obs_Date", to_date($"Obs_Date", "MM/dd/yyyy")).persist()

    //Creating temporary view "Covid" from covidDF
    covidDF.createOrReplaceTempView("Covid")
    monthsDF.createOrReplaceTempView("Months_of_Year")

    //Shows Data types of modifiedDF as an array
//    println(covidDF.dtypes.mkString("Array(", ", ", ")"))

  }

  def query1(spark: SparkSession): Unit = {
    /* This block is for comparing China at the first 6 months of outbreak to 3 other neighboring countries from the following month 3 to month 9 of the outbreak */

    println("+" + ("=" * 49) + "+" +
      s"""\nDebug Query 1 start\n
         |""".stripMargin + "+" + ("=" * 49) + "+")

    //    Lazy val for query blocks created to optimize performance and memory
    val chinaQuery = {
      //    Mainland China
      /*
       * Note: there is a discrepancy of 1,704,425 cases when comparing Total Recovered to Total Confirmed and Total Deaths.
       * Adding Total Deaths to Total Recovered results in 12,605,246 cases reported. Where are the remaining 1,704,425 cases from total Confirmed?? O.o
       */
      println("~ Displaying cases in China from January 2020 to July 2020 ~")
      println("Total Cases (Confirmed, Death, and Recovery):")
      spark.sql("SELECT sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM Covid WHERE Obs_Date BETWEEN '2020-01-22' and '2020-07-31' and Country = 'Mainland China' OR Country = 'China'").show()
      println("Total Average Cases per day (Confirmed, Death, and Recovery):")
      spark.sql("SELECT round(avg(Confirmed), 2) as Average_Confirmed, round(avg(Deaths), 2) as Average_Deaths, round(avg(Recovered), 2) as Average_Recovered FROM Covid WHERE Obs_Date BETWEEN '2020-01-22' and '2020-07-31' and Country = 'Mainland China' OR Country = 'China'").show()
      spark.sql("SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-01-22' and '2020-07-31' and Country = 'Mainland China' OR Country = 'China'").show()
    }

    val vietQuery = {
      //    Vietnam
      println("~ Displaying cases in Vietnam from March 2020 to September 2020 ~")
      println("Total Cases (Confirmed, Death, and Recovery):")
      spark.sql("SELECT sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM Covid WHERE Obs_Date BETWEEN '2020-04-22' and '2020-09-31' and Country = 'Vietnam'").show()
      println("Total Average Cases per day (Confirmed, Death, and Recovery):")
      spark.sql("SELECT round(avg(Confirmed), 2) as Average_Confirmed, round(avg(Deaths), 2) as Average_Deaths, round(avg(Recovered), 2) as Average_Recovered FROM Covid WHERE Obs_Date BETWEEN '2020-04-22' and '2020-09-31' and Country = 'Vietnam'").show()
      spark.sql("SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-03-1' and '2020-09-31' and Country = 'Vietnam'").show()
    }


    val taiQuery = {
      //    Taiwan - results are similar to Vietnam
      println("~ Displaying cases in Taiwan from March 2020 to September 2020 ~")
      println("Total Cases (Confirmed, Death, and Recovery):")
      spark.sql("SELECT sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM Covid WHERE Obs_Date BETWEEN '2020-04-22' and '2020-09-31' and Country = 'Taiwan'").show()
      println("Total Average Cases per day (Confirmed, Death, and Recovery):")
      spark.sql("SELECT round(avg(Confirmed), 2) as Average_Confirmed, round(avg(Deaths), 2) as Average_Deaths, round(avg(Recovered), 2) as Average_Recovered FROM Covid WHERE Obs_Date BETWEEN '2020-04-22' and '2020-09-31' and Country = 'Taiwan'").show()
      spark.sql("SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-03-1' and '2020-09-31' and Country = 'Taiwan'").show()
    }

    val indiaQuery = {
      //    India - the worst of the two impacted in the same time frame
      println("~ Displaying cases in India from March 2020 to September 2020 ~")
      println("Total Cases (Confirmed, Death, and Recovery):")
      spark.sql("SELECT sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM Covid WHERE Obs_Date BETWEEN '2020-04-22' and '2020-09-31' and Country = 'India'").show()
      println("Total Average Cases per day (Confirmed, Death, and Recovery):")
      spark.sql("SELECT round(avg(Confirmed), 2) as Average_Confirmed, round(avg(Deaths), 2) as Average_Deaths, round(avg(Recovered), 2) as Average_Recovered FROM Covid WHERE Obs_Date BETWEEN '2020-04-22' and '2020-09-31' and Country = 'India'").show()
      spark.sql("SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-03-1' and '2020-09-31' and Country = 'India'").show()
    }

    println("+" + ("=" * 49) + "+" +
      s"""\nDebug Query 1 end\n
         |""".stripMargin + "+" + ("=" * 49) + "+")



  }

  def query2(spark: SparkSession): Unit ={
    /* Goal: establish new DF to query the sum of all confirmed cases in china
 * and vietnam over 6 months span (roughly Jan-Jun for China and Mar-Sept for Vietnam)
 * NOTE: need to find a way to add february numbers to display beside january
 */

    val tsvWithHeaderOptions: Map[String, String] = Map(
      //      ("delimiter", "\t"), // Uses "\t" delimiter instead of default ","
      ("header", "true"))

    val chinaQuery = {
      spark.sql("SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-01-22' and '2020-07-31' and Country = 'Mainland China' OR Country = 'China'")
    }

    val vietQuery = {
      spark.sql("SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-03-1' and '2020-09-31' and Country = 'Vietnam'")
    }

    val taiQuery = {
      spark.sql("SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-03-1' and '2020-09-31' and Country = 'Taiwan'")
    }

    val indiaQuery = {
      spark.sql("SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-03-1' and '2020-09-31' and Country = 'India'")
    }

//    val newDFMonthsChina = spark.sql("(SELECT SUM(confirmed) AS Total_Confirmed from Covid WHERE Obs_Date BETWEEN '2020-01-01' and '2020-01-31' AND Country = 'Mainland China') " +
//      "UNION ALL (SELECT SUM(confirmed) AS January from Covid WHERE Obs_Date BETWEEN '2020-02-01' and '2020-02-29' AND Country = 'Mainland China')")
//    newDFMonthsChina.show()

    chinaQuery.coalesce(1)         // Writes to a single file
      .write
      .mode(SaveMode.Overwrite)
      .options(tsvWithHeaderOptions)
      .csv("output/china6mo")

    vietQuery.coalesce(1)         // Writes to a single file
      .write
      .mode(SaveMode.Overwrite)
      .options(tsvWithHeaderOptions)
      .csv("output/vietnam6mo")

    taiQuery.coalesce(1)         // Writes to a single file
      .write
      .mode(SaveMode.Overwrite)
      .options(tsvWithHeaderOptions)
      .csv("output/taiwan6mo")

    indiaQuery.coalesce(1)         // Writes to a single file
      .write
      .mode(SaveMode.Overwrite)
      .options(tsvWithHeaderOptions)
      .csv("output/india6mo")
  }

}
