package ml.logistic.regression

import com.kj.spark.session.Context
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.RegressionModel

object LogisticRegressionExample extends  App  with  Context {

  ////load(getClass.getResource("src_main_resources_scores.csv").getPath)



  // 1 load data set
  // 2 add feature column
  // 3 add label column
  // 4 Build Logistic regression model(fit and transform) and random split data into train data and test data
  // 5 evaluate model


  import sparkSession.implicits._
  val df1 = Seq(
    (70.66150955499435, 92.92713789364831,1),
    (76.97878372747498, 47.57596364975532,1),
    (67.37202754570876, 42.83843832029179,1),
    (89.67677575072079, 65.79936592745237,1),
    (50.534788289883, 48.85581152764205,0)
  ).toDF("score1", "score2","result")
  df1.show()

  // create feature

   val assembler =new VectorAssembler().setInputCols(Array("score1", "score2","result")).setOutputCol("features")
   val featuredf= assembler.transform(df1)

   featuredf.printSchema()
   featuredf.show(10)

  // add label column ..need to use stringIndexer
  // StringIndexer define new 'label' column with 'result' column

  val stringIndexer = new StringIndexer().setInputCol("result").setOutputCol("label");

 val labeldf  = stringIndexer.fit(featuredf).transform(featuredf)

   labeldf.printSchema()
   labeldf.show(20)

  // build regression model
  // split data set training and test
  // training data set - 70%
  // test data set - 30%

     val Array(trainingdata,testdata) = labeldf.randomSplit(Array(0.7,0.3),5043)

   trainingdata.show(10)

  //// train logistic regression model with training data set

     val regression =new LogisticRegression().setMaxIter(100).setRegParam(0.02).setElasticNetParam(0.8)
     val regressionModel = regression.fit(trainingdata)

  // run model with test data set to get predictions
  // this will add new columns rawPrediction, probability and prediction

    val predecationdf =  regressionModel.transform(testdata)
       predecationdf.show(10)


  // evaluate model with area under ROC
  val evaluator = new BinaryClassificationEvaluator().setLabelCol("label")
    .setRawPredictionCol("prediction")
    .setMetricName("areaUnderROC")

  // measure the accuracy
  val accuracy = evaluator.evaluate(predecationdf)
  println(accuracy)
}

