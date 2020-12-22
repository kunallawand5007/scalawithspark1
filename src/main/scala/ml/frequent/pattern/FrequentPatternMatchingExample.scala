package ml.frequent.pattern

import com.kj.spark.session.Context

object FrequentPatternMatchingExample extends App with Context {

  // Mining frequent items is usually among the first steps to analyze a large-scale datase
  //D:\lockdown\data\Online Retail.xlsx

  sparkSession.sql("set spark.sql.files.ignoreCorruptFiles=true")

  val fileLoadedDf = sparkSession.read
    .load("D:\\lockdown\\data\\Online-Retail.xlsx")
    .toDF()

  fileLoadedDf.show(5)

}
