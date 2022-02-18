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

    modifiedDF.show(10)

    //Shows Data types of modifiedDF as an array
    //println(modifiedDF.dtypes.mkString("Array(", ", ", ")"))





  }

}
