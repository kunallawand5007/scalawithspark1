package ml.clustering

import com.kj.spark.session.Context
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}

object KMeansClusteringExample extends App with Context {

   println(getClass.getResource("/goodhealth.txt").getPath)


     // by default spark expecting paraqi file

     //defined schema

    val schema = StructType(
        StructField("id",LongType,nullable = true)::
        StructField("timestamp",TimestampType,nullable = true)::
        StructField("desc",StringType,nullable = true)::
        Nil
    )




  sparkSession.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

   var fileloadedasdf = sparkSession.read
       .format("csv").
     option("header",false).
     option("delimiter","|").
     option("mode", "DROPMALFORMED").
     load("D:\\lockdown\\data\\Health-News-Tweets\\Health-Tweets\\goodhealth.txt")
   //println(fileloadedasdf.show(10))

   fileloadedasdf.printSchema()

  // wholse sale customer data set
   // convert it to double/long
 import  sparkSession.sqlContext.implicits._

///EEE MMM dd kk:mm:ss +0000 yyyy
  val ts = functions.to_timestamp(fileloadedasdf("_c1"),"EEE MMM dd kk:mm:ss +0000 yyyy")


  var converteddf = fileloadedasdf.withColumn("_c0",fileloadedasdf("_c0").cast(LongType)).
      withColumn("_c1",ts.cast(LongType)).toDF().na.drop()



  converteddf.show(20)

  converteddf.printSchema()

   // creating vector assembler
    var arr = Array("_c0","_c1")

   var assembler =new VectorAssembler().setInputCols(arr).setOutputCol("features")
   var featuredf = assembler.transform(converteddf)

  featuredf.show(20)
  featuredf.printSchema()

    // create test data

  val Array(trainingdata,testdata) =  featuredf.randomSplit(Array(0.7,0.3),123)

   var kmean= new KMeans().setK(10).setFeaturesCol("features").setPredictionCol("prediction")

   var trainmodel = kmean.fit(trainingdata)

    trainmodel.clusterCenters.foreach(println)


  val finaldf=  trainmodel.transform(testdata)

    finaldf.show()


  finaldf.groupBy("prediction").count().show()

  // Evaluate clustering by computing Silhouette score
  val evaluator = new ClusteringEvaluator()

  val silhouette = evaluator.evaluate(finaldf)
  println(s"Silhouette with squared euclidean distance = $silhouette")




}
