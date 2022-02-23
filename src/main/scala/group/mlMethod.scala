package group

import breeze.storage.ConfigurableDefault.fromV
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import jodd.io.FileUtil.params
import mlMethod.mlFunction
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, RegressionMetrics}
import org.apache.log4j.LogManager
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.functions.to_date
import org.spark_project.dmg.pmml.ClusteringField.CenterField
import shapeless.Strict.apply
import spire.syntax.field

object mlMethod {

  //System.setProperty("hadoop.home.dir", "C:\\winutils")
  System.setProperty("hadoop.home.dir", "C:\\hadoop\\")

  val spark = SparkSession
    .builder
    .appName("hello hive")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  println("Spark Works Y'all")
  spark.sparkContext.setLogLevel("ERROR")


  def mlFunction(): Unit = {

    val trainInput = spark.read.option("header",true).option("inferSchema",true).format("csv").load(
      "input/dataset.csv").toDF

    var cleanDF = trainInput.filter(trainInput("heart_rate_apache") > -1 && trainInput("intubated_apache") > -1 && trainInput("map_apache") > -1
      && trainInput("resprate_apache") > -1 && trainInput("temp_apache") > -1 && trainInput("ventilated_apache") > -1 &&
      trainInput("d1_diasbp_max") > -1 && trainInput("d1_diasbp_min") > -1 && trainInput("d1_glucose_max") > -1 &&
      trainInput("d1_glucose_min") > -1 && trainInput("d1_potassium_max") > -1 && trainInput("d1_potassium_min") > -1 && trainInput("aids") > -1
      && trainInput("cirrhosis") > -1 && trainInput("diabetes_mellitus")  > -1  && trainInput("hepatic_failure") > -1 &&
      trainInput("immunosuppression") > -1  && trainInput("leukemia") > -1 && trainInput("lymphoma") > -1 &&
      trainInput("hospital_death") > -1 )

    /**  Now comes something more complicated.  Our dataframe has the column headings
      *  we created with the schema.  But we need a column called “label” and one called
    * “features” to plug into the LR algorithm.  So we use the VectorAssembler() to do that.
      * Features is a Vector of doubles.  These are all the values like patient age, etc. that
    * we extracted above.  The label indicated whether the patient has cancer.
    */
    val featureCols = Array("heart_rate_apache","intubated_apache","map_apache","resprate_apache","temp_apache","ventilated_apache",
      "d1_diasbp_max","d1_diasbp_min", "d1_glucose_max","d1_glucose_min","d1_potassium_max","d1_potassium_min",
      "aids" , "cirrhosis" , "diabetes_mellitus" , "hepatic_failure" , "immunosuppression" , "leukemia" , "lymphoma")
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val df2 = assembler.transform(cleanDF)
    /**
     * Then we use the StringIndexer to take the column hospital_death and make that the label.
     *  FNDX is the 1 or 0 indicator that shows whether the patient has cancer.
     * Like the VectorAssembler it will add another column to the dataframe.
     */
    val labelIndexer = new StringIndexer().setInputCol("hospital_death").setOutputCol("label")
    val df3 = labelIndexer.fit(df2).transform(df2)


    // Split the Train and Test Data
    val Array(trainData, testData) = df3.randomSplit(Array(0.7,0.3))

    // Define the Logistic regression object
    val lr = new LogisticRegression().setMaxIter(20).setRegParam(0.3).setElasticNetParam(0.8)

    //fit the model with the training data
    val model = lr.fit(trainData)

    //Get results using test data
    val predictions = model.transform(testData)
    val predictions2 = model.transform(df3)
    val dfnew = predictions.withColumn("prediction", col("prediction").cast("Double"))
    dfnew.createOrReplaceTempView("test")
    val dfnew2 = predictions2.withColumn("prediction", col("prediction").cast("Double"))
    dfnew2.createOrReplaceTempView("test2")

    /*val dfTest = spark.sql("select rawPrediction, label from test")


    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")

    val accuracy = evaluator.evaluate(dfTest)
    println("Accuracy: " + accuracy)
    evaluator.explainParams()



    val predictionAndLabels = predictions
      .select("prediction", "label")
      .rdd.map(x => (x(0).asInstanceOf[Double], x(1)
      .asInstanceOf[Double]))

    val metrics = new BinaryClassificationMetrics(predictionAndLabels)

    val areaUnderPR = metrics.areaUnderPR
    println("Area under the precision-recall curve: " + areaUnderPR)

    val areaUnderROC = metrics.areaUnderROC
    println("Area under the receiver operating characteristic (ROC) curve: " + areaUnderROC)*/

    val df = spark.sql("select * from test").show(10)

    val lp = predictions.select("label", "prediction")
    val counttotal = predictions.count()
    val correct = lp.filter("label = prediction").count()
    val wrong = lp.filter("label != prediction").count()
    val ratioWrong = wrong.toDouble / counttotal.toDouble
    val ratioCorrect = correct.toDouble / counttotal.toDouble

    println("Total Count of Rows In Test Data: " + counttotal)
    println("Correct Predictions: " + correct)
    println("Incorrect Predictions: " + wrong)
    println("Ratio  of incorrect predictions: " + ratioWrong)
    println("Ratio of correct predictions: " + ratioCorrect)
    spark.sql("Select first(probability) as Probability_of_Accurate_Prediction from test").show()






    /*val df4 = spark.sql("select max(probability), min(probability) from test").show()
    val df5 = spark.sql("(select count(label), 'Prediction is Accurate' as Accuracy from test where label = prediction) union all" +
      "(select count(label), 'Prediction is not Accurate' as Accuracy from test where label != prediction)").show()
    val dfNew = spark.sql("select label, probability, prediction from test2 where label = prediction").show(10)
    val df4New = spark.sql("select max(probability), min(probability) from test2").show()
    val df5New = spark.sql("(select count(label), 'Prediction is Accurate' as Accuracy from test2 where label = prediction) union all" +
      "(select count(label), 'Prediction is not Accurate' as Accuracy from test2 where label != prediction)").show()*/

    /*val model = new LogisticRegression().fit(df3)
    val predictions = model.transform(df3)*/
    /**
     *  Now we print it out.  Notice that the LR algorithm added a “prediction” column
     *  to our dataframe.   The prediction in almost all cases will be the same as the label.  That is
     * to be expected it there is a strong correlation between these values.  In other words
     * if the chance of getting cancer was not closely related to these variables then LR
     * was the wrong model to use.  The way to check that is to check the accuracy of the model.
     *  You could use the BinaryClassificationEvaluator Spark ML function to do that.
     * Adding that would be a good exercise for you, the reader.
     */
    /*predictions.select ("features", "label", "prediction")
    predictions.createOrReplaceTempView("Testing")
    val tester = spark.sql("select label, probability, prediction from Testing where label = prediction ")
    val tester2 = spark.sql("select label, probability, prediction from Testing where label = prediction and label = 1 ")
    val dfnew = predictions.withColumn("prediction", col("prediction").cast("Double"))
    dfnew.createOrReplaceTempView("Testing2")
    val dfDf = spark.sql("select max(prediction), min(prediction) from Testing2").show()*/
  }
}
