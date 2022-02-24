package Package2

import Package2.main.spark
import org.apache.spark.sql.types.{DataType, DateType}

case class FeedbackRow(Id: Int, Obs_Date: DateType, State:String, Country:String, Update: DateType, Confirmed:Int, Deaths:Int, Recovered:Int) {

  import org.apache.spark.sql.Encoders
  import spark.implicits._
 // val caseClassDS = Seq(FeedbackRow(1,"2020-01-22","Punjab","India","2020-02-22",10,12,10)).toDS()
 // caseClassDS.show()


  // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
  val path = "input/covid_19_data.csv"
  val peopleDS = spark.read.parquet(path).as[FeedbackRow]
  //peopleDS.show()
  peopleDS.filter("Id > 30").show()
   // .join(department, people("deptId") === department("id"))
  //  .groupBy(department("name"), people("gender"))
    //.agg(avg(people("salary")), max(people("age")))

}






