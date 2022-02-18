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

}
