package ml.features.transformer

import com.kj.spark.session.Context
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}

object StringIndexerTransformer extends App  with Context{

//var seq = Seq((0,"a"),(1,"b"),(2,"c"),(3,"d"),(4,"b"),(5,"e"),(6,"f"),(7,"d"))

 var seq =Seq((1,1),(2,2),(3,3),(4,2),(5,1),(6,4),(7,5),(8,6))


 var createddf= sparkSession.createDataFrame(seq).toDF("Id","category")

  createddf.printSchema()



 var indexer= new StringIndexer().setInputCol("category").setOutputCol("categoryIndexer").setHandleInvalid("skip")



 var indexerDf=indexer.fit(createddf).transform(createddf)

  indexerDf.show()


  //IndextoString

  var indextoStr =new IndexToString().setInputCol("categoryIndexer").setOutputCol("originalCol")

  var indexToStringDf= indextoStr.transform(indexerDf)

   indexToStringDf.show()

}
