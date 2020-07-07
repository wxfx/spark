package Charpter2

import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCountSubmitApp  extends App{
  val conf = new SparkConf()
  val sc = new SparkContext(conf)

  val rdd = sc.textFile(args(0))
      .flatMap(_.split(","))
      .map((_, 1))
      .reduceByKey(_+_)
      .map(x=>(x._2, x._1))
      .sortByKey(false)
      .map(x=>(x._2, x._1))
      .saveAsTextFile(args(1))
      //.collect()
      //.foreach(println)

  sc.stop()
}
