package Charpter4

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameAPIApp extends App{
  val spark = SparkSession.builder()
    .master("local")
    .appName("DataFrameApp")
    .getOrCreate()

  val people: DataFrame = spark.read.json("file:///root/IdeaProjects/spark_train/src/main/data/sparksql/people.json")
  //查看DF的内部结构：列名、列的数据类型、是否可以为空
  //people.printSchema()
  //展示出DF内部的数据
  //people.show()

  import spark.implicits._
  //TODO... DF只有两列，只要name列 ==> select name from people
  people.select("name").show()
  //这个方法用了隐式转换不行，但源码是有的
  //people.select(expr("colB as newName")).show();
  people.selectExpr("name as new_name").show()
  //这个方法用了隐式转换
  people.select($"name").show()




}
