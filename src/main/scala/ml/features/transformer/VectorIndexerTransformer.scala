package ml.features.transformer

import com.kj.spark.session.Context
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.sql.functions
import org.apache.spark.sql.types._

object VectorIndexerTransformer extends App with Context {

  //VectorIndexer helps index categorical features in datasets of Vectors
  //It can both automatically decide which features are categorical and convert original values to category indices


  // by default spark expecting paraqi file

  //defined schema

  val schema = StructType(
    StructField("id", LongType, nullable = true) ::
      StructField("timestamp", TimestampType, nullable = true) ::
      StructField("desc", StringType, nullable = true) ::
      Nil
  )


  sparkSession.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

  var fileloadedasdf = sparkSession.read
    .format("csv").
    option("header", false).
    option("delimiter", "|").
    option("mode", "DROPMALFORMED").
    load("D:\\lockdown\\data\\Health-News-Tweets\\Health-Tweets\\goodhealth.txt")
  //println(fileloadedasdf.show(10))

  fileloadedasdf.printSchema()

  // wholse sale customer data set
  // convert it to double/long

  ///EEE MMM dd kk:mm:ss +0000 yyyy
  val ts = functions.to_timestamp(fileloadedasdf("_c1"), "EEE MMM dd kk:mm:ss +0000 yyyy")


  var converteddf = fileloadedasdf.withColumn("_c0", fileloadedasdf("_c0").cast(LongType)).
    withColumn("_c1", ts.cast(LongType)).toDF().na.drop()

  converteddf.show(20)

  converteddf.printSchema()

  // creating vector assembler
  var arr = Array("_c0", "_c1")

  var assembler = new VectorAssembler().setInputCols(arr).setOutputCol("features")
  var featuredf = assembler.transform(converteddf)

  featuredf.show(20)
  featuredf.printSchema()

  var vIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexed").setMaxCategories(10)

  var vindexerModel = vIndexer.fit(featuredf)

  val categoricalFeatures: Set[Int] = vindexerModel.categoryMaps.keys.toSet
  println(s"Chose ${categoricalFeatures.size} " +
    s"categorical features: ${categoricalFeatures.mkString(", ")}")

  //   var vindexerdf=vindexerModel.transform(featuredf)
  //   vindexerdf.show()

}
