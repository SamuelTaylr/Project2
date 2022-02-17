package group

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.to_date
import scala.io.StdIn

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
  /*given table name and 2 columns from that table,
      receive 2 values via user input to query in that table
      return true if the query succeeds, else false --mac
      code is currently commented out but can be re-implemented if desired
   */
  def userLogin(spark : SparkSession/*, table : String, Param1 : String, Param2 : String*/): Boolean = {
    val username = StdIn.readLine("Please enter your username: ").toUpperCase()
    val password = StdIn.readLine("Please enter your password: ").toUpperCase()

    val verifyLogin = spark.sql("select" +
      s"\n\tcase when count(username) >= 1 and count(password) >= 1 then" +
        "\n\t\ttrue" +
      "\n\telse" +
        "\n\t\tfalse" +
      "\n\tend as boolAnswer" +
      s"\nfrom login" +
        s"\n\twhere upper(username)=upper($username) and upper(password)=upper($password);")

    val verifyResult = verifyLogin.first().get(0)
    verifyResult.asInstanceOf[Boolean]
  }

  def avgCasesByMonth(inputDF : DataFrame, spark : SparkSession): Unit = {
    inputDF.createOrReplaceTempView("AvgQuery")
    spark.sql("(select round(avg(Confirmed), 2) from AvgQuery As Jan2020_Avg where Obs_Date Between '2020-01-22' and '2020-01-31') " +
      "UNION ALL(select round(avg(Confirmed), 2) from AvgQuery As Feb2020_Avg where Obs_Date Between '2020-02-01' and '2020-02-29')" +
      "UNION ALL(select round(avg(Confirmed), 2) from AvgQuery As Mar2020_Avg where Obs_Date Between '2020-03-01' and '2020-03-31')" +
      "UNION ALL(select round(avg(Confirmed), 2) from AvgQuery As Apr2020_Avg where Obs_Date Between '2020-04-01' and '2020-04-30')" +
      "UNION ALL(select round(avg(Confirmed), 2) from AvgQuery As May2020_Avg where Obs_Date Between '2020-05-01' and '2020-05-31')" +
      "UNION ALL(select round(avg(Confirmed), 2) from AvgQuery As Jun2020_Avg where Obs_Date Between '2020-06-01' and '2020-06-30')" +
      "UNION ALL(select round(avg(Confirmed), 2) from AvgQuery As Jul2020_Avg where Obs_Date Between '2020-07-01' and '2020-07-31')" +
      "UNION ALL(select round(avg(Confirmed), 2) from AvgQuery As Aug2020_Avg where Obs_Date Between '2020-08-01' and '2020-08-31')" +
      "UNION ALL(select round(avg(Confirmed), 2) from AvgQuery As Sep2020_Avg where Obs_Date Between '2020-09-01' and '2020-09-30')" +
      "UNION ALL(select round(avg(Confirmed), 2) from AvgQuery As Oct2020_Avg where Obs_Date Between '2020-10-01' and '2020-10-31')" +
      "UNION ALL(select round(avg(Confirmed), 2) from AvgQuery As Nov2020_Avg where Obs_Date Between '2020-11-01' and '2020-11-30')" +
      "UNION ALL(select round(avg(Confirmed), 2) from AvgQuery As Dec2020_Avg where Obs_Date Between '2020-12-01' and '2020-12-31')" +
      "UNION ALL(select round(avg(Confirmed), 2) from AvgQuery As Jan2021_Avg where Obs_Date Between '2021-01-01' and '2021-01-31')" +
      "UNION ALL(select round(avg(Confirmed), 2) from AvgQuery As Feb2021_Avg where Obs_Date Between '2021-02-01' and '2021-02-28')" +
      "UNION ALL(select round(avg(Confirmed), 2) from AvgQuery As Mar2021_Avg where Obs_Date Between '2021-03-01' and '2021-03-31')" +
      "UNION ALL(select round(avg(Confirmed), 2) from AvgQuery As Apr2021_Avg where Obs_Date Between '2021-04-01' and '2021-04-30')" +
      "UNION ALL(select round(avg(Confirmed), 2) from AvgQuery As May2021_Avg where Obs_Date Between '2021-05-01' and '2021-29-05')").show()
  }

}
