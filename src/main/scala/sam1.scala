
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

class sam1 {
  def dataLoader(spark: SparkSession): Unit = {
    import org.apache.spark.rdd
    import spark.implicits._
    // for implicit conversions from Spark RDD to Dataframe
    //val dataFrame = rdd.toDF()
  }



}
