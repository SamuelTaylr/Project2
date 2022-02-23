package group

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

class jake {
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
    import spark.implicits._

    //Creating initial DataFrame from csv file
    val dfTest = spark.read.option("header",true).option("inferSchema",true).format("csv").load(
      "input/covid_19_data.csv").toDF("Id", "Obs_Date","State","Country","Update","Confirmed",
      "Deaths","Recovered").persist()

    //months of year DF to work with
    val monthsDF = spark.read.option("header",true).option("inferSchema",true).format("csv").load(
      "input/months_of_year.csv").toDF("index","Months").persist()

    //Changing data type of Obs_Date column to "DateType" -- This will be the main Data Frame that we draw our queries from
    val covidDF = dfTest.withColumn("Obs_Date", to_date($"Obs_Date", "MM/dd/yyyy")).persist()

    //Creating temporary view "Covid" from covidDF
    covidDF.createOrReplaceTempView("Covid")
    monthsDF.createOrReplaceTempView("Months_of_Year")
    //Shows Data types of modifiedDF as an array
//    println(covidDF.dtypes.mkString("Array(", ", ", ")"))

    //Partition covid by country
    /* another idea is to partition by date - or combine the two partitions in subquery */
//    spark.sql("CREATE TABLE IF NOT EXISTS CovidPartitioned(beverage String) PARTITIONED BY (Country String)")

//    covidDF.write
//    .partitionBy("Country")
//    .mode("overwrite")
//    .csv("output/CovidPartitioned")

    val covidPartPre = spark.read.option("header",true).option("inferSchema",true).format("csv").load(
      "output/CovidPartitioned").toDF("Id", "Obs_Date","State","Update","Confirmed",
      "Deaths","Recovered","Country").persist()

    //df variable that fixes Country appearing as right-most column
    val covidPartPost = covidPartPre.select("Id", "Obs_Date","State","Country","Update","Confirmed",
      "Deaths","Recovered").orderBy(asc("Id"))

    val chinaDF = covidPartPost.select("*").where("Country = 'Mainland China' OR Country = 'China'").toDF()


  }

  def rollingMonthAnalysis(spark: SparkSession): Unit = {
    /* This block is for comparing China at the first 6 months of outbreak to 3 other neighboring countries from the following month 3 to month 9 of the outbreak */

    //    Lazy val for query blocks created to optimize performance and memory
    val chinaQuery = {
      //    Mainland China
      /*
       * Note: there is a discrepancy of 1,704,425 cases when comparing Total Recovered to Total Confirmed and Total Deaths.
       * Adding Total Deaths to Total Recovered results in 12,605,246 cases reported. Where are the remaining 1,704,425 cases from total Confirmed?? O.o
       */
      println("~ Displaying cases in China from January 2020 to July 2020 ~")
      println("Total Cases (Confirmed, Death, and Recovery):")
      spark.sql("SELECT sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered, (sum(Confirmed) - sum(Recovered) - sum(Deaths)) as difference_of_totals FROM Covid WHERE Obs_Date BETWEEN '2020-01-22' and '2020-07-31' and Country = 'Mainland China' OR Country = 'China'").show()
      println("Total Average Cases per day (Confirmed, Death, and Recovery):")
      spark.sql("SELECT round(avg(Confirmed), 2) as Average_Confirmed, round(avg(Deaths), 2) as Average_Deaths, round(avg(Recovered), 2) as Average_Recovered FROM Covid WHERE Obs_Date BETWEEN '2020-01-22' and '2020-07-31' and Country = 'Mainland China' OR Country = 'China'").show()
      println("Total Cases per month (Confirmed, Death, and Recovery):")
      spark.sql("SELECT 'January' as Month, sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM (SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-01-22' and '2020-01-31' and Country = 'Mainland China' OR Country = 'China')"
      +  "UNION ALL SELECT 'February' as Month, sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM (SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-02-1' and '2020-02-31' and Country = 'Mainland China' OR Country = 'China')"
      +  "UNION ALL SELECT 'March' as Month, sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM (SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-03-1' and '2020-03-31' and Country = 'Mainland China' OR Country = 'China')"
      +  "UNION ALL SELECT 'April' as Month, sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM (SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-04-1' and '2020-04-31' and Country = 'Mainland China' OR Country = 'China')"
      +  "UNION ALL SELECT 'May' as Month, sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM (SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-05-1' and '2020-05-31' and Country = 'Mainland China' OR Country = 'China')"
      +  "UNION ALL SELECT 'June' as Month, sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM (SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-06-1' and '2020-06-31' and Country = 'Mainland China' OR Country = 'China')"
      +  "UNION ALL SELECT 'July' as Month, sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM (SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-07-1' and '2020-07-31' and Country = 'Mainland China' OR Country = 'China')").show()
      spark.sql("SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-01-22' and '2020-07-31' and Country = 'Mainland China' OR Country = 'China'").show()
    }

    val vietQuery = {
      //    Vietnam
      println("~ Displaying cases in Vietnam from March 2020 to September 2020 ~")
      println("Total Cases (Confirmed, Death, and Recovery):")
      spark.sql("SELECT sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered, (sum(Confirmed) - sum(Recovered) - sum(Deaths)) as difference_of_totals FROM Covid WHERE Obs_Date BETWEEN '2020-04-22' and '2020-09-31' and Country = 'Vietnam'").show()
      println("Total Average Cases per day (Confirmed, Death, and Recovery):")
      spark.sql("SELECT round(avg(Confirmed), 2) as Average_Confirmed, round(avg(Deaths), 2) as Average_Deaths, round(avg(Recovered), 2) as Average_Recovered FROM Covid WHERE Obs_Date BETWEEN '2020-04-22' and '2020-09-31' and Country = 'Vietnam'").show()
      println("Total Cases per month (Confirmed, Death, and Recovery):")
      spark.sql("SELECT 'March' as Month, sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM (SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-01-22' and '2020-01-31' and Country = 'Vietnam')"
        +  "UNION ALL SELECT 'April' as Month, sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM (SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-02-1' and '2020-02-31' and Country = 'Vietnam')"
        +  "UNION ALL SELECT 'May' as Month, sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM (SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-03-1' and '2020-03-31' and Country = 'Vietnam')"
        +  "UNION ALL SELECT 'June' as Month, sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM (SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-04-1' and '2020-04-31' and Country = 'Vietnam')"
        +  "UNION ALL SELECT 'July' as Month, sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM (SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-05-1' and '2020-05-31' and Country = 'Vietnam')"
        +  "UNION ALL SELECT 'August' as Month, sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM (SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-06-1' and '2020-06-31' and Country = 'Vietnam')"
        +  "UNION ALL SELECT 'September' as Month, sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM (SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-07-1' and '2020-07-31' and Country = 'Vietnam')").show()
      spark.sql("SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-03-1' and '2020-09-31' and Country = 'Vietnam'").show()
    }

    val taiQuery = {
      //    Taiwan - results are similar to Vietnam
      println("~ Displaying cases in Taiwan from March 2020 to September 2020 ~")
      println("Total Cases (Confirmed, Death, and Recovery):")
      spark.sql("SELECT sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered, (sum(Confirmed) - sum(Recovered) - sum(Deaths)) as difference_of_totals FROM Covid WHERE Obs_Date BETWEEN '2020-04-22' and '2020-09-31' and Country = 'Taiwan'").show()
      println("Total Average Cases per day (Confirmed, Death, and Recovery):")
      spark.sql("SELECT round(avg(Confirmed), 2) as Average_Confirmed, round(avg(Deaths), 2) as Average_Deaths, round(avg(Recovered), 2) as Average_Recovered FROM Covid WHERE Obs_Date BETWEEN '2020-04-22' and '2020-09-31' and Country = 'Taiwan'").show()
      println("Total Cases per month (Confirmed, Death, and Recovery):")
      spark.sql("SELECT 'March' as Month, sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM (SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-01-22' and '2020-01-31' and Country = 'Taiwan')"
        +  "UNION ALL SELECT 'April' as Month, sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM (SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-02-1' and '2020-02-31' and Country = 'Taiwan')"
        +  "UNION ALL SELECT 'May' as Month, sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM (SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-03-1' and '2020-03-31' and Country = 'Taiwan')"
        +  "UNION ALL SELECT 'June' as Month, sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM (SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-04-1' and '2020-04-31' and Country = 'Taiwan')"
        +  "UNION ALL SELECT 'July' as Month, sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM (SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-05-1' and '2020-05-31' and Country = 'Taiwan')"
        +  "UNION ALL SELECT 'August' as Month, sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM (SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-06-1' and '2020-06-31' and Country = 'Taiwan')"
        +  "UNION ALL SELECT 'September' as Month, sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM (SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-07-1' and '2020-07-31' and Country = 'Taiwan')").show()
      spark.sql("SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-03-1' and '2020-09-31' and Country = 'Taiwan'").show()
    }

    val indiaQuery = {
      //    India - the worst of the two impacted in the same time frame
      println("~ Displaying cases in India from March 2020 to September 2020 ~")
      println("Total Cases (Confirmed, Death, and Recovery):")
      spark.sql("SELECT sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered, (sum(Confirmed) - sum(Recovered) - sum(Deaths)) as difference_of_totals FROM Covid WHERE Obs_Date BETWEEN '2020-04-22' and '2020-09-31' and Country = 'India'").show()
      println("Total Average Cases per day (Confirmed, Death, and Recovery):")
      spark.sql("SELECT round(avg(Confirmed), 2) as Average_Confirmed, round(avg(Deaths), 2) as Average_Deaths, round(avg(Recovered), 2) as Average_Recovered FROM Covid WHERE Obs_Date BETWEEN '2020-04-22' and '2020-09-31' and Country = 'India'").show()
      println("Total Cases per month (Confirmed, Death, and Recovery):")
      spark.sql("SELECT 'March' as Month, sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM (SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-01-22' and '2020-01-31' and Country = 'India')"
        +  "UNION ALL SELECT 'April' as Month, sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM (SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-02-1' and '2020-02-31' and Country = 'India')"
        +  "UNION ALL SELECT 'May' as Month, sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM (SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-03-1' and '2020-03-31' and Country = 'India')"
        +  "UNION ALL SELECT 'June' as Month, sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM (SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-04-1' and '2020-04-31' and Country = 'India')"
        +  "UNION ALL SELECT 'July' as Month, sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM (SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-05-1' and '2020-05-31' and Country = 'India')"
        +  "UNION ALL SELECT 'August' as Month, sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM (SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-06-1' and '2020-06-31' and Country = 'India')"
        +  "UNION ALL SELECT 'September' as Month, sum(Confirmed) as Total_Confirmed, sum(Deaths) as Total_Deaths, Sum(Recovered) as Total_Recovered FROM (SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-07-1' and '2020-07-31' and Country = 'India')").show()
      spark.sql("SELECT * FROM Covid WHERE Obs_Date BETWEEN '2020-03-1' and '2020-09-31' and Country = 'India'").show()
    }
  }
}
