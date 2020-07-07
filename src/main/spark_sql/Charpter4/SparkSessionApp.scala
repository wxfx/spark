package Charpter4

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSessionApp extends App{
  //DF、DS编程的入口点
  val spark: SparkSession = SparkSession.builder()
    .appName("SparkSessionApp")
    .master("local")
    .getOrCreate()
  //读取文件的API
  val df: DataFrame = spark.read.text("file:///root/IdeaProjects/spark_train/src/main/data/sparksql/input.txt")

  //TODO... 业务逻辑处理，肯定要通过DF/DS提供的API来完成我们的业务
  df.printSchema()
  //展示出来只有一个子段,string类型的value
  df.show()

  spark.stop()

}
