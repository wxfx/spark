package Charpter2

import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCountApp  extends App{
  val conf = new SparkConf().setMaster("local[2]").setAppName("SparkWordCountApp")
  val sc = new SparkContext(conf)

  val rdd = sc.textFile("file:///root/IdeaProjects/spark_train/src/main/data/sparksql/input.txt")
      .flatMap(_.split(","))
      .map((_, 1))
      .reduceByKey(_+_)
      .map(x=>(x._2, x._1))
      .sortByKey(false)
      .map(x=>(x._2, x._1))
      .saveAsTextFile("filee:///root/IdeaProjects/spark_train/src/main/data/sparksql/out")
      //.collect()
      //.foreach(println)

  sc.stop()
}
