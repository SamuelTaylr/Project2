package group

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.functions.to_date


class sam {

  def dataLoader(spark: SparkSession): Unit = {

    import spark.implicits._

    //Creating initial DataFrame from csv file
    val dfTest = spark.read.option("header",true).option("inferSchema",true).format("csv").load(
      "input/covid_19_data.csv").toDF("Id", "Obs_Date","State","Country","Update","Confirmed",
      "Deaths","Recovered")

    //Changing data type of Obs_Date column to "DateType"
    val covidDF = dfTest.withColumn("Obs_Date", to_date($"Obs_Date", "MM/dd/yyyy"))

    //Creating temporary view "Covid" from modifiedDF
    covidDF.createOrReplaceTempView("Covid")

    //Testing selecting between a date range, it works as intended
    val sqlDf = spark.sql("select sum(confirmed) from Covid as jan where Obs_Date Between '2020-01-22' and '2020-02-10' and Country = 'US' ")
    //sqlDf.show(300)

    val test = spark.sql("select sum(confirmed) from Covid as feb where Obs_Date Between '2020-02-22' and '2020-03-10' and Country = 'US'  ")

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

    newDfUS.select("*").show(100)

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

    newDfIndia.select("*").show(100)

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

    newDfChina.select("*").show(100)

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

    newDfUSDeath.select("*").show(100)

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

    newDfIndiaDeath.select("*").show(100)

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

    newDfChinaDeath.select("*").show(100)

  }

}
