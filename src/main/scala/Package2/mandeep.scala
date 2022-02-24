package Package2

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.to_date

import java.util.Date

class mandeep {

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

    //modifiedDF.show(10)
    //Shows Data types of modifiedDF as an array
    //println(modifiedDF.dtypes.mkString("Array(", ", ", ")"))
  }

  def Query1(spark:SparkSession):Unit= {
    println("get the total death according to month on different year")
    val sqlDf3 = spark.sql("select extract(MONTH from Obs_Date) as month, EXTRACT(year from Obs_Date) as year, MAX(Deaths)as total_Death,MAX(Confirmed),MAX(Recovered)  from Covid WHERE Country='India' group by month,year ORDER BY month ASC")
    sqlDf3.show(300)
  }
  def Query2(spark:SparkSession):Unit= {
     println("Display Country where Confirmed case rate more than 1000 and recovery case more than 1000")
         val sqlDf4 = spark.sql("select Country,Obs_Date,Confirmed,Recovered from Covid Where Confirmed >1000 and Recovered>=1000 ")
         sqlDf4.show(300)
  }
  def Query3(spark:SparkSession):Unit= {
    println("Print Total Death and Average Recovered Case Comparing two Countries US AND INDIA")
        val sqlDf5 = spark.sql("select first(Deaths) as total_death,round(AVG(Recovered)) as AVG_Recovered from Covid where Obs_Date='2021-05-29' and Country='India' and State='Chandigarh' " +
        "UNION ALL select first(Deaths) as total_death,round(AVG(Recovered),3) as AVG_Recovered from Covid where Obs_Date='2021-05-29' and Country='India' and State='Goa' " +
        "UNION ALL select first(Deaths) as total_death,round(AVG(Recovered),3) as AVG_Recovered from Covid where Obs_Date='2021-05-29' and Country='India' and State='Delhi' " +
         "UNION ALL select first(Deaths) as total_death,round(AVG(Recovered),3) as AVG_Recovered from Covid where Obs_Date='2021-05-29' and Country='Mainland China' and State='Hubei' " +
         "UNION ALL select first(Deaths) as total_death,round(AVG(Recovered),3) as AVG_Recovered from Covid where Obs_Date='2021-05-29' and Country='Mainland China' and State='Jilin' "+
          "UNION ALL select first(Deaths) as total_death,round(AVG(Recovered),3) as AVG_Recovered from Covid where Obs_Date='2021-05-29' and Country='Mainland China' and State='Henan' ")
      sqlDf5.show(300)
  }
  def Query4(spark:SparkSession):Unit= {
     println("get the total death according to month on different year")
   val sqlDf6 = spark.sql("(select extract(MONTH from Obs_Date) as month, EXTRACT(year from Obs_Date) as year , " +
     "MAX(Deaths) as total_Death from Covid WHERE Country='India' group by month,year ORDER BY month ASC) " +
     "UNION ALL (select extract(MONTH from Obs_Date) as month, EXTRACT(year from Obs_Date) as year, MAX(Deaths) as total_Death " +
     "from Covid WHERE Country='US' group by month,year ORDER BY month ASC) " +
     "UNION ALL (select extract(MONTH from Obs_Date) as month, EXTRACT(year from Obs_Date) as year, MAX(Deaths) as total_Death " +
     "from Covid WHERE Country='China' group by month,year ORDER BY month ASC) " +
     "UNION ALL (select extract(MONTH from Obs_Date) as month, EXTRACT(year from Obs_Date) as year, MAX(Deaths) as total_Death " +
     "from Covid WHERE Country='UK' group by month,year ORDER BY month ASC) ")
   sqlDf6.show(300)
  }
  def Query5(): Unit ={
    println("exit")
  }
  /* val sqlDf6 = spark.sql("select sum(Confirmed)as Total_Confirmed, sum(Deaths)as total_death from(select *from Covid where Obs_Date between '1/22/2020' and '5/29/2021' and Country='India' and State='Delhi')" +
        "UNION ALL select sum(Confirmed)as Total_Confirmed, sum(Deaths)as total_death from(select *from Covid where Obs_Date between '2/12/2020' and '5/29/2021' and Country='India' and State='Punjab')" +
        "UNION ALL select sum(Confirmed)as Total_Confirmed, sum(Deaths)as total_death from(select *from Covid where Obs_Date between '3/23/2020' and '5/29/2021' and Country='India' and State='Chandigarh')")
      sqlDf6.show(300)*/

  //println
  /* val sqlDf6 = spark.sql("select extract(MONTH from Obs_Date) as month, MAX(Confirmed)as Total_Confirmed,Deaths from(select from Covid where  Obs_Date='2020-01-15' and Country='India')" +
         "UNION ALL select extract(MONTH from Obs_Date) as month, MAX(Confirmed)as Total_Confirmed,Deaths from(select from Covid where Obs_Date='2020-02-17' and Country='US')" +
         "UNION ALL select extract(MONTH from Obs_Date) as month, MAX(Confirmed)as Total_Confirmed,Deaths from(select from Covid where Obs_Date='2020-10-20' and Country='China' )")
   sqlDf6.show(300)*/

  /*val sqlDf2 = spark.sql("SELECT Obs_Date,SUM(Deaths) FROM Covid WHERE Country = 'India' GROUP BY Obs_Date ORDER BY Obs_Date ASC ")
    sqlDf2.show(300)*/

}

