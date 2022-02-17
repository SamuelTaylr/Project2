package group

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class jake {

  def printName(): Unit ={
    println("+" + ("=" * 49) + "+" +
      s"""\nJake
         |""".stripMargin + "+" + ("=" * 49) + "+")
  }

  def dataLoader(spark: SparkSession): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val sc = new SparkContext()
    val spark = SparkSession
      .builder()
      .appName("Project2")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")



    println("Spark Works Y'all")
    spark.sparkContext.setLogLevel("ERROR")

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

    /* KEEP THIS - This output could be useful graphing the raw data from the two countries
     * compared */
//    Mainland China
//    spark.sql("SELECT count(Obs_Date) FROM covidDF WHERE Obs_Date BETWEEN '2020-01-22' and '2020-06-31' and Country = 'Mainland China'").show()
//    spark.sql("SELECT * FROM covidDF WHERE Obs_Date BETWEEN '2020-01-22' and '2020-06-31' and Country = 'Mainland China'").show()
//    Vietnam
//    spark.sql("SELECT count(Obs_Date) FROM covidDF WHERE Obs_Date BETWEEN '2020-03-22' and '2020-08-31' and Country = 'Mainland China'").show()
//    spark.sql("SELECT * FROM covidDF WHERE Obs_Date BETWEEN '2020-03-1' and '2020-09-31' and Country = 'Vietnam'").show()

      /* Goal: establish new DF to query the sum of all confirmed cases in china
       * and vietnam over 6 months span (roughly Jan-Jun for China and Mar-Sept for Vietnam)
       * NOTE: need to find a way to add february numbers to display beside january
       */
//    new table with column months to join to query
      case class monthsOfYear(months_of_year: String)
      val month_rdd = sc.parallelize(Array("January","February", "March", "April", "June", "July", "August", "September", "October", "November", "December"))
          month_rdd.collect

    val month_rdd2 = month_rdd.map(_.split(","))
    val monthsOfYearDF = month_rdd2.map(attributes => monthsOfYear(attributes(0).trim)).toDF()

    monthsOfYearDF.show()

//    val newDFMonthsChina = spark.sql("(SELECT SUM(confirmed) AS Total_Confirmed from CovidDF WHERE Obs_Date BETWEEN '2020-01-01' and '2020-01-31' AND Country = 'Mainland China') " +
//      "UNION ALL (SELECT SUM(confirmed) AS January from CovidDF WHERE Obs_Date BETWEEN '2020-02-01' and '2020-02-29' AND Country = 'Mainland China')")
//    newDFMonthsChina.show()


    println("+" + ("=" * 49) + "+" +
      s"""\nDebug Query 1 end
         |""".stripMargin + "+" + ("=" * 49) + "+")

  }
}
