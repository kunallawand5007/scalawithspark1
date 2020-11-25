package ml.features.transformer

import com.kj.spark.session.Context
import ml.clustering.KMeansClusteringExample.{fileloadedasdf, sparkSession}
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.sql.{Column, functions}
import shapeless.Poly1

object TokenizerTransformer extends App with Context{

  Console.BOLD
  println(s"Tokenizer :the process of taking text (such as a sentence) and breaking it into individual terms (usually words")

  //sparkSession.c

   var seq =Seq((0,"Hey Kunal Lawand "),
     (1,"I am Good"),
     (2,"How is it going"),
     (3,"Its going well,what about you")
    )

  println("Sequence is as fallow...")

   seq.foreach(s=>{
     println(s"${s._1}:${s._2}")
   })

   var seqdf =sparkSession.createDataFrame(seq).toDF()
   seqdf.show()
  import  sparkSession.sqlContext.implicits._

    var tokenforma =new Tokenizer().setInputCol("_2").setOutputCol("Tokens").transform(seqdf)
  tokenforma.printSchema()
  tokenforma.show()






}
