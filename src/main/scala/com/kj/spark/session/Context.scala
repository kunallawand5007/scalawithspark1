package com.kj.spark.session

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


trait Context {

   lazy  val sparkConf=new SparkConf().setAppName("KJ").setMaster("local[*]").set("spark.cores.max","2")
   lazy  val sparkSession=SparkSession.builder().config(sparkConf).getOrCreate()

   // lazy val sparkContextObj=new SparkContext("local[*]", "WordCount")


    println(Console.BOLD)
   println("Spark Context Created...!!!!")
}
