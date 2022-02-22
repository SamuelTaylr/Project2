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

  def dataLoader(spark: SparkSession): Unit = {

    import spark.implicits._

    //Creating initial DataFrame from csv file
    val dfTest = spark.read.option("header",true).option("inferSchema",true).format("csv").load(
      "input/covid_19_data.csv").toDF("Id", "Obs_Date","State","Country","Update","Confirmed",
      "Deaths","Recovered")

    //Changing data type of Obs_Date column to "DateType"
    val modifiedDF = dfTest.withColumn("Obs_Date", to_date($"Obs_Date", "MM/dd/yyyy"))

    //Creating temporary view "Covid" from modifiedDF
    modifiedDF.createOrReplaceTempView("Covid")

    //Testing selecting between a date range, it works as intended
    val sqlDf = spark.sql("select * from Covid where Obs_Date Between '2020-01-22' and '2020-02-10' and Country = 'US'   ")
   sqlDf.show(300)

    //modifiedDF.show(10)
    //Shows Data types of modifiedDF as an array
    //println(modifiedDF.dtypes.mkString("Array(", ", ", ")"))

    //val sqlDf1 = spark.sql("select Obs_Date,Deaths from Covid where Obs_Date Between '1/22/2020' and '5/30/2021' and Country = 'India' ")
  //  sqlDf1.show(300)

    // val sqlDf2=spark.sql("SELECT Obs_Date,SUM(Deaths) FROM Covid WHERE Country = 'India' GROUP BY Obs_Date ORDER BY Obs_Date ASC ")
   // sqlDf2.show(300)

    //get the total death according to month on different year
    val sqlDf3 = spark.sql("select extract(MONTH from Obs_Date) as month, EXTRACT(year from Obs_Date) as year,  MAX(Deaths) as total_Death from Covid WHERE Country='India' group by month,year ORDER BY month ASC")
   sqlDf3.show(300)

    val sqlDf4 = spark.sql("select Country,Obs_Date,Confirmed,Recovered from Covid Where Confirmed >1000 and Recovered>=1000 ")
    sqlDf4.show(300)



    /*val sqlDf1=spark.sql("select Obs_Date(select Count(Distinct State)as no_of_unique_state from Covid co2\n" +
      "where co2.Obs_Date=co1.Obs_Date\n" +
      "AND(Select Count(distinct co3.Obs_Date) from Covid co3\n" +
      "where co3.Obs_Date=co2.Obs_Date\n)= Datediff(S1.Obs_Date,'2020-01-22')\n) " +
      "AS NO_OF_UNIQUE_state(select State from Covid co2\n" +
      "WHERE  S2.Obs_Date = S1.Obs_Date\nGROUP  BY State\n " +
      "ORDER  BY Count(Deaths) DESC,State ASC LIMIT  1 \n)AS MAX_SUB_unique_state(SELECT Obs_Date FROM Covid\n" +
      "WHERE no_of_unique_state = MAX_SUB_unique_state\n) AS NAME\n" +
      "FROM(SELECT DISTINCT Obs_Date FROM Covid) S1\nGROUP BY Obs_Date")
    sqlDf1.show(300)*/




  }

}
