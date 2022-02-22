package group

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{avg, column, count, expr, floor, max, min, sum, to_date, when}
import java.sql.DriverManager
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException
import scala.io.StdIn.readLine
import scala.io.Source
import java.io.FileNotFoundException
import java.security.MessageDigest
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.types.DataType


class sam {

  def dataLoader(spark: SparkSession, covidDF: DataFrame): Unit = {

    import spark.implicits._
    //Creating temporary view "Covid" from modifiedDF
    covidDF.createOrReplaceTempView("Covid")

  }

  def subQuery1(spark: SparkSession): Unit = {

    val newDfUS = spark.sql("(select sum(confirmed) as Jan_Confirmed from Covid where Obs_Date = '2020-01-31' " +
      " and Country = 'US') UNION ALL (select sum(confirmed) as Feb_Confirmed from Covid where Obs_Date =" +
      "'2020-02-28' and Country = 'US') UNION ALL (select sum(confirmed) as Mar_Confirmed from Covid " +
      "where Obs_Date = '2020-03-31' and Country = 'US') UNION ALL (select sum(confirmed) " +
      "as Apr_Confirmed from Covid where Obs_Date = '2020-04-30' and Country = 'US') UNION ALL " +
      "(select sum(confirmed) as May_Confirmed from Covid where Obs_Date = '2020-05-31' " +
      "and Country = 'US') UNION ALL (select sum(confirmed) as Jun_Confirmed from Covid where Obs_Date = " +
      "'2020-06-30' and Country = 'US') UNION ALL (select sum(confirmed) as Jul_Confirmed from Covid " +
      "where Obs_Date = '2020-07-31' and Country = 'US') UNION ALL (select sum(confirmed) as Aug_Confirmed from Covid " +
      "where Obs_Date = '2020-08-31' and Country = 'US') UNION ALL (select sum(confirmed) as Sep_Confirmed from Covid " +
      "where Obs_Date = '2020-09-30' and Country = 'US') UNION ALL (select sum(confirmed) as Oct_Confirmed from Covid " +
      "where Obs_Date = '2020-10-31' and Country = 'US') UNION ALL (select sum(confirmed) as Nov_Confirmed from Covid " +
      "where Obs_Date = '2020-11-30' and Country = 'US') UNION ALL (select sum(confirmed) as Dec_Confirmed from Covid " +
      "where Obs_Date = '2020-12-31' and Country = 'US')").persist()

    newDfUS.select("*").persist().show(100)

  }

  def subQuery2(spark: SparkSession): Unit = {

    val newDfIndia = spark.sql("(select sum(confirmed) as Jan_Confirmed from Covid where Obs_Date = '2020-01-31' " +
      " and Country = 'India') UNION ALL (select sum(confirmed) as Feb_Confirmed from Covid where Obs_Date =" +
      "'2020-02-28' and Country = 'India') UNION ALL (select sum(confirmed) as Mar_Confirmed from Covid " +
      "where Obs_Date = '2020-03-31' and Country = 'India') UNION ALL (select sum(confirmed) " +
      "as Apr_Confirmed from Covid where Obs_Date = '2020-04-30' and Country = 'India') UNION ALL " +
      "(select sum(confirmed) as May_Confirmed from Covid where Obs_Date = '2020-05-31' " +
      "and Country = 'India') UNION ALL (select sum(confirmed) as Jun_Confirmed from Covid where Obs_Date = " +
      "'2020-06-30' and Country = 'India') UNION ALL (select sum(confirmed) as Jul_Confirmed from Covid " +
      "where Obs_Date = '2020-07-31' and Country = 'India') UNION ALL (select sum(confirmed) as Aug_Confirmed " +
      "from Covid where Obs_Date = '2020-08-31' and Country = 'India') UNION ALL (select sum(confirmed) as Sep_Confirmed " +
      "from Covid where Obs_Date = '2020-09-30' and Country = 'India') UNION ALL (select sum(confirmed) as Oct_Confirmed " +
      "from Covid where Obs_Date = '2020-10-31' and Country = 'India') UNION ALL (select sum(confirmed) as Nov_Confirmed " +
      "from Covid where Obs_Date = '2020-11-30' and Country = 'India') UNION ALL (select sum(confirmed) as Dec_Confirmed " +
      "from Covid where Obs_Date = '2020-12-31' and Country = 'India')").persist()

    newDfIndia.select("*").persist().show(100)

  }

  def subQuery3(spark: SparkSession): Unit = {

    val newDfChina = spark.sql("(select sum(confirmed) as Jan_Confirmed from Covid where Obs_Date = '2020-01-31' " +
      " and Country = 'Mainland China') UNION ALL (select sum(confirmed) as Feb_Confirmed from Covid where Obs_Date =" +
      "'2020-02-28' and Country = 'Mainland China') UNION ALL (select sum(confirmed) as Mar_Confirmed from Covid " +
      "where Obs_Date = '2020-03-31' and Country = 'Mainland China') UNION ALL (select sum(confirmed) " +
      "as Apr_Confirmed from Covid where Obs_Date = '2020-04-30' and Country = 'Mainland China') UNION ALL " +
      "(select sum(confirmed) as May_Confirmed from Covid where Obs_Date = '2020-05-31' " +
      "and Country = 'Mainland China') UNION ALL (select sum(confirmed) as Jun_Confirmed from Covid where Obs_Date = " +
      "'2020-06-30' and Country = 'Mainland China') UNION ALL (select sum(confirmed) as Jul_Confirmed from Covid " +
      "where Obs_Date = '2020-07-31' and Country = 'Mainland China') UNION ALL (select sum(confirmed) as Aug_Confirmed " +
      "from Covid where Obs_Date = '2020-08-31' and Country = 'Mainland China') UNION ALL (select sum(confirmed) as Sep_Confirmed " +
      "from Covid where Obs_Date = '2020-09-30' and Country = 'Mainland China') UNION ALL (select sum(confirmed) as Oct_Confirmed " +
      "from Covid where Obs_Date = '2020-10-31' and Country = 'Mainland China') UNION ALL (select sum(confirmed) as Nov_Confirmed " +
      "from Covid where Obs_Date = '2020-11-30' and Country = 'Mainland China') UNION ALL (select sum(confirmed) as Dec_Confirmed " +
      "from Covid where Obs_Date = '2020-12-31' and Country = 'Mainland China')").persist()

    newDfChina.select("*").persist().show(100)
  }

  def subQuery4(spark: SparkSession): Unit = {

    val newDfUSDeath = spark.sql("(select sum(deaths) as Jan_Confirmed from Covid where Obs_Date = '2020-01-31' " +
      " and Country = 'US') UNION ALL (select sum(deaths) as Feb_Confirmed from Covid where Obs_Date =" +
      "'2020-02-28' and Country = 'US') UNION ALL (select sum(deaths) as Mar_Confirmed from Covid " +
      "where Obs_Date = '2020-03-31' and Country = 'US') UNION ALL (select sum(deaths) " +
      "as Apr_Confirmed from Covid where Obs_Date = '2020-04-30' and Country = 'US') UNION ALL " +
      "(select sum(deaths) as May_Confirmed from Covid where Obs_Date = '2020-05-31' " +
      "and Country = 'US') UNION ALL (select sum(deaths) as Jun_Confirmed from Covid where Obs_Date = " +
      "'2020-06-30' and Country = 'US') UNION ALL (select sum(deaths) as Jul_Confirmed from Covid " +
      "where Obs_Date = '2020-07-31' and Country = 'US') UNION ALL (select sum(deaths) as Aug_Confirmed from Covid " +
      "where Obs_Date = '2020-08-31' and Country = 'US') UNION ALL (select sum(deaths) as Sep_Confirmed from Covid " +
      "where Obs_Date = '2020-09-30' and Country = 'US') UNION ALL (select sum(deaths) as Oct_Confirmed from Covid " +
      "where Obs_Date = '2020-10-31' and Country = 'US') UNION ALL (select sum(deaths) as Nov_Confirmed from Covid " +
      "where Obs_Date = '2020-11-30' and Country = 'US') UNION ALL (select sum(deaths) as Dec_Confirmed from Covid " +
      "where Obs_Date = '2020-12-31' and Country = 'US')").persist()

    newDfUSDeath.select("*").persist().show(100)
  }

  def subQuery5(spark: SparkSession): Unit = {

    val newDfIndiaDeath = spark.sql("(select sum(deaths) as Jan_Confirmed from Covid where Obs_Date = '2020-01-31' " +
      " and Country = 'India') UNION ALL (select sum(deaths) as Feb_Confirmed from Covid where Obs_Date =" +
      "'2020-02-28' and Country = 'India') UNION ALL (select sum(deaths) as Mar_Confirmed from Covid " +
      "where Obs_Date = '2020-03-31' and Country = 'India') UNION ALL (select sum(deaths) " +
      "as Apr_Confirmed from Covid where Obs_Date = '2020-04-30' and Country = 'India') UNION ALL " +
      "(select sum(deaths) as May_Confirmed from Covid where Obs_Date = '2020-05-31' " +
      "and Country = 'India') UNION ALL (select sum(deaths) as Jun_Confirmed from Covid where Obs_Date = " +
      "'2020-06-30' and Country = 'India') UNION ALL (select sum(deaths) as Jul_Confirmed from Covid " +
      "where Obs_Date = '2020-07-31' and Country = 'India') UNION ALL (select sum(deaths) as Aug_Confirmed " +
      "from Covid where Obs_Date = '2020-08-31' and Country = 'India') UNION ALL (select sum(deaths) as Sep_Confirmed " +
      "from Covid where Obs_Date = '2020-09-30' and Country = 'India') UNION ALL (select sum(deaths) as Oct_Confirmed " +
      "from Covid where Obs_Date = '2020-10-31' and Country = 'India') UNION ALL (select sum(deaths) as Nov_Confirmed " +
      "from Covid where Obs_Date = '2020-11-30' and Country = 'India') UNION ALL (select sum(deaths) as Dec_Confirmed " +
      "from Covid where Obs_Date = '2020-12-31' and Country = 'India')").persist()

    newDfIndiaDeath.select("*").persist().show(100)
  }

  def subQuery6(spark: SparkSession): Unit = {

    val newDfChinaDeath = spark.sql("(select sum(deaths) as Jan_Confirmed from Covid where Obs_Date = '2020-01-31' " +
      " and Country = 'Mainland China') UNION ALL (select sum(deaths) as Feb_Confirmed from Covid where Obs_Date =" +
      "'2020-02-28' and Country = 'Mainland China') UNION ALL (select sum(deaths) as Mar_Confirmed from Covid " +
      "where Obs_Date = '2020-03-31' and Country = 'Mainland China') UNION ALL (select sum(deaths) " +
      "as Apr_Confirmed from Covid where Obs_Date = '2020-04-30' and Country = 'Mainland China') UNION ALL " +
      "(select sum(deaths) as May_Confirmed from Covid where Obs_Date = '2020-05-31' " +
      "and Country = 'Mainland China') UNION ALL (select sum(deaths) as Jun_Confirmed from Covid where Obs_Date = " +
      "'2020-06-30' and Country = 'Mainland China') UNION ALL (select sum(deaths) as Jul_Confirmed from Covid " +
      "where Obs_Date = '2020-07-31' and Country = 'Mainland China') UNION ALL (select sum(deaths) as Aug_Confirmed " +
      "from Covid where Obs_Date = '2020-08-31' and Country = 'Mainland China') UNION ALL (select sum(deaths) as Sep_Confirmed " +
      "from Covid where Obs_Date = '2020-09-30' and Country = 'Mainland China') UNION ALL (select sum(deaths) as Oct_Confirmed " +
      "from Covid where Obs_Date = '2020-10-31' and Country = 'Mainland China') UNION ALL (select sum(deaths) as Nov_Confirmed " +
      "from Covid where Obs_Date = '2020-11-30' and Country = 'Mainland China') UNION ALL (select sum(deaths) as Dec_Confirmed " +
      "from Covid where Obs_Date = '2020-12-31' and Country = 'Mainland China')").persist()

    newDfChinaDeath.select("*").persist().show(100)
  }

  def query2(spark: SparkSession): Unit = {
    //covidDF.createOrReplaceTempView("Covid")

    val worldDeaths = spark.sql("(select sum(deaths) as Total_Deaths, first(country) as Country, 1 as Population_Rank from Covid where Country = 'Mainland China' and Obs_date = '2020-12-31') " +
      "union all (select sum(deaths), first(country), 2 as Population_Rank from Covid where Country = 'India' and Obs_date = '2020-12-31') " +
      "union all (select sum(deaths), first(country), 3 as Population_Rank from Covid where Country = 'US' and Obs_date = '2020-12-31') " +
      "union all (select sum(deaths), first(country), 4 as Population_Rank from Covid where Country = 'Indonesia' and Obs_date = '2020-12-31') " +
      "union all (select sum(deaths), first(country), 5 as Population_Rank from Covid where Country = 'Pakistan' and Obs_date = '2020-12-31') " +
      "union all (select sum(deaths), first(country), 6 as Population_Rank from Covid where Country = 'Brazil' and Obs_date = '2020-12-31') " +
      "union all (select sum(deaths), first(country), 7 as Population_Rank from Covid where Country = 'Nigeria' and Obs_date = '2020-12-31') " +
      "union all (select sum(deaths), first(country), 8 as Population_Rank from Covid where Country = 'Bangladesh' and Obs_date = '2020-12-31') " +
      "union all (select sum(deaths), first(country), 9 as Population_Rank from Covid where Country = 'Russia' and Obs_date = '2020-12-31') " +
      "union all (select sum(deaths), first(country), 10 as Population_Rank from Covid where Country = 'Mexico' and Obs_date = '2020-12-31') ").persist()


    worldDeaths.select("*").persist().show(100)
  }

}
