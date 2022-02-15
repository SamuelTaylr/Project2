import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


class sam {



  def loadDf(spark: SparkSession): Unit = {

    import spark.implicits._
    val preDf = spark.sparkContext.textFile("input/covid_19_data.csv")
    val df = preDf.map(_.split(",")).map{case Array(a,b,c,d,e,f,g,h) => (a,b,c,d,e,f,g,h)}.toDF("Id",
      "ObsDate","State","Country","LastUpdate","Confirmed","Deaths","Recovered")

    df.show(10)

  }

}
