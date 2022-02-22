package group

import org.apache.spark.sql.SparkSession

object main {
//  Universal: Establish MySQL Connection
  def mySQLConn: Unit = {
    val dbGetConnection = new dbGetConnection
    dbGetConnection.dbConnection()
  }

  System.setProperty("hadoop.home.dir", "C:\\winutils")
  mySQLConn

  val spark = SparkSession
    .builder()
    .appName("Project2")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  def main(args: Array[String]): Unit = {

    //    start test
    val jake = new jake
    jake.printName()
    jake.dataLoader(spark)
    jake.rollingMonthAnalysis(spark)
//    jake.query2(spark)
  }
}
