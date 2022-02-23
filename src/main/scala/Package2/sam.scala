package Package2

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.to_date

import java.util.Date

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

   /* val ds: Dataset[FeedbackRow] = dfTest.as[FeedbackRow]
    val theSameDS = spark.read.csv("input/covid_19_data.csv").as[FeedbackRow]*/

    //val peopleDF = spark.sparkContext




    /*  .textFile("examples/src/main/resources/people.txt")
      .map(_.split(","))
      .map(attributes => FeedbackRow(attributes(0).trim.toInt, attributes(1),attributes(2),attributes(3),attributes(4),attributes(5).trim.toInt,attributes(6).trim.toInt,attributes(7).trim.toInt))
      .toDF()*/

    /* peopleDF.createOrReplaceTempView("people")
    //Testing selecting between a date range, it works as intended
    val teenagersDF = spark.sql("select * from Covid where Obs_Date Between '2020-01-22' and '2020-02-10' and Country = 'US'   ")
    //teenagersDF.show(300)
    teenagersDF.map(teenager => "Id:"  + teenager(0)).show()
    teenagersDF.map(teenager => "Obs_Date: " + teenager.getAs[String]("Obs_Date")).show()
    teenagersDF.map(teenager => "State: " + teenager.getAs[String]("State")).show()
    teenagersDF.map(teenager => "Country: " + teenager.getAs[String]("Country")).show()
    teenagersDF.map(teenager => "Update: " + teenager.getAs[String]("Update")).show()
    teenagersDF.map(teenager => "Confirmed:" + teenager.getAs[Int]("Confirmed")).show()
    teenagersDF.map(teenager => "Deaths: " + teenager.getAs[Int]("Deaths")).show()
    teenagersDF.map(teenager => "Recovered: " + teenager.getAs[Int]("Recovered")).show()*/

   // implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]

   // teenagersDF.map(teenager => teenager.getValuesMap[Any](List("Id","Obs_Date","State","Country","Update","Confirmed","Deaths","Recovered"))).collect()



    //modifiedDF.show(10)
    //Shows Data types of modifiedDF as an array
    //println(modifiedDF.dtypes.mkString("Array(", ", ", ")"))



    /*val sqlDf2 = spark.sql("SELECT Obs_Date,SUM(Deaths) FROM Covid WHERE Country = 'India' GROUP BY Obs_Date ORDER BY Obs_Date ASC ")
    sqlDf2.show(300)*/


     /*println("get the total death according to month on different year")
      val sqlDf3 = spark.sql("select extract(MONTH from Obs_Date) as month, EXTRACT(year from Obs_Date) as year, MAX(Deaths)as total_Death,MAX(Confirmed),MAX(Recovered)  from Covid WHERE Country='India' group by month,year ORDER BY month ASC")
      sqlDf3.show(300)*/


      /* println("Display Country where Confirmed case rate more than 1000 and recovery case more than 1000")
       val sqlDf4 = spark.sql("select Country,Obs_Date,Confirmed,Recovered from Covid Where Confirmed >1000 and Recovered>=1000 ")
       sqlDf4.show(300)*/


     /* println("Print Total Death and Average Recovered Case Comparing two Countries US AND INDIA")
      val sqlDf5 = spark.sql("select first(Deaths) as total_death,round(AVG(Recovered)) as AVG_Recovered from Covid where Obs_Date='5/29/2021' and Country='India' and State='Chandigarh' " +
      "UNION ALL select first(Deaths) as total_death,round(AVG(Recovered),3) as AVG_Recovered from Covid where Obs_Date='5/29/2021' and Country='India' and State='Uttar Pradesh' " +
      "UNION ALL select first(Deaths) as total_death,round(AVG(Recovered),3) as AVG_Recovered from Covid where Obs_Date='5/29/2021' and Country='US' and State='Washington' " +
       "UNION ALL select first(Deaths) as total_death,round(AVG(Recovered),3) as AVG_Recovered from Covid where Obs_Date='5/29/2021' and Country='US' and State='North Carolina' " +
       "UNION ALL select first(Deaths) as total_death,round(AVG(Recovered),3) as AVG_Recovered from Covid where Obs_Date='5/29/2021' and Country='India' and State='Delhi' ")
     sqlDf5.show(300)*/

     //not println("get the total death according to month on different year")
     /*val sqlDf6 = spark.sql("select extract(MONTH from Obs_Date) as month, EXTRACT(year from Obs_Date) as year, MAX(Deaths) as total_Death from Covid WHERE Country='India' group by month,year ORDER BY month ASC " +
       "UNION ALL select extract(MONTH from Obs_Date) as month, EXTRACT(year from Obs_Date) as year, MAX(Deaths) as total_Death from Covid WHERE Country='US' group by month,year ORDER BY month ASC " +
       "UNION ALL select extract(MONTH from Obs_Date) as month, EXTRACT(year from Obs_Date) as year, MAX(Deaths) as total_Death from Covid WHERE Country='China' group by month,year ORDER BY month ASC " +
       "UNION ALL select extract(MONTH from Obs_Date) as month, EXTRACT(year from Obs_Date) as year, MAX(Deaths) as total_Death from Covid WHERE Country='UK' group by month,year ORDER BY month ASC ")
     sqlDf6.show(300)*/


    /* val sqlDf6 = spark.sql("select sum(Confirmed)as Total_Confirmed, sum(Deaths)as total_death from(select *from Covid where Obs_Date between '1/22/2020' and '5/29/2021' and Country='India' and State='Delhi')" +
      "UNION ALL select sum(Confirmed)as Total_Confirmed, sum(Deaths)as total_death from(select *from Covid where Obs_Date between '2/12/2020' and '5/29/2021' and Country='India' and State='Punjab')" +
      "UNION ALL select sum(Confirmed)as Total_Confirmed, sum(Deaths)as total_death from(select *from Covid where Obs_Date between '3/23/2020' and '5/29/2021' and Country='India' and State='Chandigarh')")
    sqlDf6.show(300)*/


   /* not working val sqlDf6 = spark.sql("select extract(MONTH from Obs_Date) as month, MAX(Confirmed)as Total_Confirmed, sum(Deaths)as total_death from(select Obs_Date between from Covid where Country='India')" +
                         "UNION ALL select extract(MONTH from Obs_Date) as month, MAX(Confirmed)as Total_Confirmed, sum(Deaths)as total_death from(select Obs_Date between from Covid where Country='US')" +
                         "UNION ALL select extract(MONTH from Obs_Date) as month, MAX(Confirmed)as Total_Confirmed, sum(Deaths)as total_death from(select Obs_Date between from Covid where Country='China' )group by month ")
    sqlDf6.show(300)*/




  }

}
