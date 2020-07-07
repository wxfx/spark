package Charpter4

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SQLContextApp extends App{
  val sparkConf = new SparkConf()
    .setAppName("HelloSparkSQL")
    .setMaster("local")
  //一定要传入SparkConf
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  val df = sqlContext.read.text("file:///root/IdeaProjects/spark_train/src/main/data/sparksql/input.txt")
  df.show()

  sc.stop()

}
