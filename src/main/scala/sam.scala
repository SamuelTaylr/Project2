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

    val preDf = spark.sparkContext.textFile("input/covid_19_data.csv")
    val df = preDf.map(_.split(",")).map{case Array(a,b,c,d,e,f,g,h) => (a,b,c,d,e,f,g,h)}.toDF("Id",
      "Obs_Date","State","Country","Update","Confirmed","Deaths","Recovered")

    val dfTest = spark.read.option("header",true).option("inferSchema",true).format("csv").load(
      "input/covid_19_data.csv").toDF("Id", "Obs_Date","State","Country","Update","Confirmed",
      "Deaths","Recovered")

    df.createOrReplaceTempView("Covid")
    dfTest.createOrReplaceTempView("Covid2")
    //val df2 = dfTest.withColumn("Obs_Date", df("Obs_Date").cast("Date"))

    val sqlDf1 = spark.sql("select Obs_date from Covid2")
    val newdf = sqlDf1.select(to_date(column("Obs_Date"),"yyyy-MM-DD").as("Date"))
    //val df2 = sqlDf1.withColumn("Obs_Date", df("Obs_Date").cast("Date"))
    val modifiedDF = dfTest.withColumn("Obs_Date", to_date($"Obs_Date", "MM/dd/yyyy"))

    modifiedDF.createOrReplaceTempView("Covid3")
    val sqlDf2 = spark.sql("select * from Covid3 where Obs_Date Between '2020-01-22' and '2020-02-10' and Country = 'US'   ")
    sqlDf2.show(300)

    sqlDf1.show()
    modifiedDF.show(10)
    println(modifiedDF.dtypes.mkString("Array(", ", ", ")"))
    //println(df2.dtypes.mkString)
    println(sqlDf1.dtypes.mkString("Array(", ", ", ")"))
    println(dfTest.dtypes.mkString("Array(", ", ", ")"))
    println(df.dtypes.mkString("Array(", ", ", ")"))



  }

}
