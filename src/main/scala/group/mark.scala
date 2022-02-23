package group

import com.esotericsoftware.kryo.io.Input
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{conv, split, to_date}

import scala.io.StdIn
import java.sql.{Connection, DriverManager}

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

  def sparkQuery1(inputDF: DataFrame, spark: SparkSession) = {
    inputDF.createOrReplaceTempView("query1")
    val result = spark.sql("SELECT Obs_Date, SUM(Confirmed) FROM query1 GROUP BY Obs_Date ")
    result.show()
  }

  /*given table name and 2 columns from that table,
      receive 2 values via user input to query in that table
      return true if the query succeeds, else false --mac
      code is currently commented out but can be re-implemented if desired
   */
  def userLogin(spark: SparkSession /*, table : String, Param1 : String, Param2 : String*/): Boolean = {
    val username = StdIn.readLine("Please enter your username: ").toUpperCase()
    val password = StdIn.readLine("Please enter your password: ").toUpperCase()
    //val verifyLogin = statement.executeQuery("") if using jdbc connection --statement is placeholder for your connection
    val verifyLogin = spark.sql("select" +
      s"\n\tcase when count(username) >= 1 and count(password) >= 1 then" +
      "\n\t\ttrue" +
      "\n\telse" +
      "\n\t\tfalse" +
      "\n\tend as boolAnswer" +
      s"\nfrom login" +
      s"\n\twhere upper(username)=upper($username) and upper(password)=upper($password);")
    //jdbc version--> val verifyResult = verifyLogin.next()
    val verifyResult = verifyLogin.first().get(0)
    verifyResult.asInstanceOf[Boolean]
  }

  def avgCasesByMonth(inputDF: DataFrame, spark: SparkSession): DataFrame = {
    inputDF.createOrReplaceTempView("AvgQuery")
    val globAvgByMonth = spark.sql("(select round(avg(Confirmed), 2) as Global_AvgConfJan20, round(avg(Deaths), 2) as Global_AvgDeathJan20, round(avg(Recovered), 2) as Global_AvgRecJan20 from AvgQuery As Jan2020_Avg where Obs_Date = '2020-01-31') " +
      "UNION ALL(select round(avg(Confirmed), 2) as Global_AvgConfFeb20, round(avg(Deaths), 2) as Global_AvgDeathFeb20, round(avg(Recovered), 2) as Global_AvgRecFeb20 from AvgQuery where Obs_Date ='2020-02-29')" +
      "UNION ALL(select round(avg(Confirmed), 2) as Global_AvgConfMar20, round(avg(Deaths), 2) as Global_AvgDeathMar20, round(avg(Recovered), 2) as Global_AvgRecMar20 from AvgQuery where Obs_Date = '2020-03-31')" +
      "UNION ALL(select round(avg(Confirmed), 2) as Global_AvgConfApr20, round(avg(Deaths), 2) as Global_AvgDeathApr20, round(avg(Recovered), 2) as Global_AvgRecApr20 from AvgQuery where Obs_Date = '2020-04-30')" +
      "UNION ALL(select round(avg(Confirmed), 2) as Global_AvgConfMay20, round(avg(Deaths), 2) as Global_AvgDeathMay20, round(avg(Recovered), 2) as Global_AvgRecMay20 from AvgQuery where Obs_Date = '2020-05-31')" +
      "UNION ALL(select round(avg(Confirmed), 2) as Global_AvgConfJun20, round(avg(Deaths), 2) as Global_AvgDeathJun20, round(avg(Recovered), 2) as Global_AvgRecJun20 from AvgQuery where Obs_Date = '2020-06-30')" +
      "UNION ALL(select round(avg(Confirmed), 2) as Global_AvgConfJul20, round(avg(Deaths), 2) as Global_AvgDeathJul20, round(avg(Recovered), 2) as Global_AvgRecJul20 from AvgQuery where Obs_Date = '2020-07-31')" +
      "UNION ALL(select round(avg(Confirmed), 2) as Global_AvgConfAug20, round(avg(Deaths), 2) as Global_AvgDeathAug20, round(avg(Recovered), 2) as Global_AvgRecAug20 from AvgQuery where Obs_Date = '2020-08-31')" +
      "UNION ALL(select round(avg(Confirmed), 2) as Global_AvgConfSep20, round(avg(Deaths), 2) as Global_AvgDeathSep20, round(avg(Recovered), 2) as Global_AvgRecSep20 from AvgQuery where Obs_Date = '2020-09-30')" +
      "UNION ALL(select round(avg(Confirmed), 2) as Global_AvgConfOct20, round(avg(Deaths), 2) as Global_AvgDeathOct20, round(avg(Recovered), 2) as Global_AvgRecOct20 from AvgQuery where Obs_Date = '2020-10-31')" +
      "UNION ALL(select round(avg(Confirmed), 2) as Global_AvgConfNov20, round(avg(Deaths), 2) as Global_AvgDeathNov20, round(avg(Recovered), 2) as Global_AvgRecNov20 from AvgQuery where Obs_Date = '2020-11-30')" +
      "UNION ALL(select round(avg(Confirmed), 2) as Global_AvgConfDec20, round(avg(Deaths), 2) as Global_AvgDeathDec20, round(avg(Recovered), 2) as Global_AvgRecDec20 from AvgQuery where Obs_Date = '2020-12-31')" +
      "UNION ALL(select round(avg(Confirmed), 2) as Global_AvgConfJan21, round(avg(Deaths), 2) as Global_AvgDeathJan21, round(avg(Recovered), 2) as Global_AvgRecJan21 from AvgQuery where Obs_Date = '2021-01-31')" +
      "UNION ALL(select round(avg(Confirmed), 2) as Global_AvgConfFeb21, round(avg(Deaths), 2) as Global_AvgDeathFeb21, round(avg(Recovered), 2) as Global_AvgRecFeb21 from AvgQuery where Obs_Date = '2021-02-28')" +
      "UNION ALL(select round(avg(Confirmed), 2) as Global_AvgConfMar21, round(avg(Deaths), 2) as Global_AvgDeathMar21, round(avg(Recovered), 2) as Global_AvgRecMar21 from AvgQuery where Obs_Date = '2021-03-31')" +
      "UNION ALL(select round(avg(Confirmed), 2) as Global_AvgConfApr21, round(avg(Deaths), 2) as Global_AvgDeathApr21, round(avg(Recovered), 2) as Global_AvgRecApr21 from AvgQuery where Obs_Date = '2021-04-30')" +
      "UNION ALL(select round(avg(Confirmed), 2) as Global_AvgConfMay21, round(avg(Deaths), 2) as Global_AvgDeathMay21, round(avg(Recovered), 2) as Global_AvgRecMay21 from AvgQuery where Obs_Date = '2021-05-05')").persist()
    globAvgByMonth
  }

  def avgCasesByQuarter(inputDF: DataFrame, spark: SparkSession): DataFrame = {
    inputDF.createOrReplaceTempView("AvgQuery")
    val globalAvgByQuarter = spark.sql("(select round(avg(Confirmed), 2) As Quarter1_AvgConf, round(avg(Deaths), 2) As Quarter1_AvgDeaths, round(avg(Recovered), 2) As Quarter1_AvgRec From AvgQuery Where Obs_Date Between '2020-01-22' and '2020-03-31')" +
      "UNION ALL(select round(avg(Confirmed), 2) As Quarter2_AvgConf, round(avg(Deaths), 2) As Quarter2_AvgDeaths, round(avg(Recovered), 2) As Quarter2_AvgRec From AvgQuery Where Obs_Date Between '2020-04-01' and '2020-06-30')" +
      "UNION ALL(select round(avg(Confirmed), 2) As Quarter3_AvgConf, round(avg(Deaths), 2) As Quarter3_AvgDeaths, round(avg(Recovered), 2) As Quarter3_AvgRec From AvgQuery Where Obs_Date Between '2020-07-01' and '2020-09-30')" +
      "UNION ALL(select round(avg(Confirmed), 2) As Quarter4_AvgConf, round(avg(Deaths), 2) As Quarter3_AvgDeaths, round(avg(Recovered), 2) As Quarter3_AvgRec From AvgQuery Where Obs_Date Between '2020-10-01' and '2020-12-31')").persist()
    globalAvgByQuarter
  }

  case class toDataSet(AvgConf: Double, AvgDead: Double, AvgRec: Double)
  def queryToDataSet(inputDF: DataFrame, spark: SparkSession): Dataset[toDataSet] = {
    val encoder = org.apache.spark.sql.catalyst.encoders.ExpressionEncoder[toDataSet]
    val convertToDataset = avgCasesByMonth(inputDF, spark).toDF("AvgConf", "AvgDead", "AvgRec")
    val didThisConvert = convertToDataset.as(encoder)
    didThisConvert
  }
}

