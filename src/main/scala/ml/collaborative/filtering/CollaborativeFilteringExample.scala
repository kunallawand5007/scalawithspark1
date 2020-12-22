package ml.collaborative.filtering

import com.kj.spark.session.Context
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.IntegerType

object CollaborativeFilteringExample extends App with Context {

 // load file


 var fileDf = sparkSession.read.format("csv").option("header", true)
   .load("D:\\lockdown\\data\\RCdata\\rating_final.csv").toDF()

 // df.show()
 //fileDf.printSchema()

 println("After converting to Int type....")
 //convert to numeric type


 var newdf = fileDf.withColumn("userID", functions.regexp_replace(fileDf("userID"), "U", "").cast(IntegerType))
   .withColumn("placeID", fileDf("placeID").cast(IntegerType))
   .withColumn("rating", fileDf("rating").cast(IntegerType))

 // newdf.printSchema()

 newdf.show()

 // random split data

 var Array(training, test) = newdf.randomSplit(Array(0.8, 0.2))

 // build recommendation using ALs on training data

 val alsmodel = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userID").setItemCol("placeID").setRatingCol("rating")

 alsmodel.setColdStartStrategy("drop")

 val trainingDf = alsmodel.fit(training)

 val predict = trainingDf.transform(test)

 predict.show()

 var specificUsers = newdf.select(alsmodel.getUserCol).distinct().limit(3)


 //var userRecommend= trainingDf.recommendForAllUsers(10)
 //var placeRecommend =trainingDf.recommendForAllItems(10)
 //placeRecommend.show()
 //placeRecommend.collect().foreach(println)


 var sq = Seq((1, "1067"))
 var seqdf = sparkSession.createDataFrame(sq).toDF("ID", "userID").drop("ID")


 var convertedDF = seqdf.withColumn("userID", seqdf("userID").cast(IntegerType))

 var top10Recommend = trainingDf.recommendForUserSubset(specificUsers, 10)
 top10Recommend.show()


}
