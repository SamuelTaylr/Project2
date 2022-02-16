package group

import org.apache.spark.sql.SparkSession


class sam {

  def dataLoader(spark: SparkSession): Unit = {

    import spark.implicits._

    val preDf = spark.sparkContext.textFile("input/covid_19_data.csv")
    val df = preDf.map(_.split(",")).map{case Array(a,b,c,d,e,f,g,h) => (a,b,c,d,e,f,g,h)}.toDF("Id",
      "Obs_Date","State","Country","Update","Confirmed","Deaths","Recovered")

    df.show(10)



  }

}
