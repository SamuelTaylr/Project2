package Package2
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.to_date
import java.util.Date

class mandeepDataSet {
  val spark = SparkSession
    .builder()
    .appName("SparkDatasetExample")
    .enableHiveSupport()
    .getOrCreate()

  def dataLoader(spark: SparkSession): Unit = {

    //Creating initial DataFrame from csv file
    val dfTest = spark.read.option("header",true).option("inferSchema",true).format("csv").load(
      "input/covid_19_data.csv").toDF("Id", "Obs_Date","State","Country","Update","Confirmed",
      "Deaths","Recovered")
    //Changing data type of Obs_Date column to "DateType"
    // val modifiedDF = dfTest.withColumn("Obs_Date", to_date($"Obs_Date", "MM/dd/yyyy"))

    //Creating temporary view "Covid" from modifiedDF
    //modifiedDF.createOrReplaceTempView("Covid")

    /*val people = spark.read.parquet("input/covid_19_data.csv").as[FeedbackRow]
    val names = people.map(_.name)  // in Scala; names is a Dataset[String]
    Dataset<String> names = people.map((FeedbackRow p) -> p.name, Encoders.STRING));*/


  }


}
