package Charpter4

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DatasetApp extends App{
  private val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("DatasetApp")
    .getOrCreate()
  import spark.implicits._

  private val ds: Dataset[Person] = Seq(Person("PK", "30")).toDS()
  //ds.show()


  //用多个字符来生成DS不行
  //private val primitiveDS: Any = Seq('a', 'b', 'c').toDS()
  private val primitiveDS: Dataset[Int] = Seq(1, 2, 3).toDS()
  //primitiveDS.map((_, "hi")).collect().foreach(println)

  private val peopleDF: DataFrame = spark.read.json("file:///root/IdeaProjects/spark_train/src/main/data/sparksql/people.json")
  private val peopleDS: Dataset[Person] = peopleDF.as[Person]
  //peopleDS.show(truncate = false)
  //peopleDS.show(numRows = 1,truncate = false)

  //编译期报错:Exception in thread "main" org.apache.spark.sql.AnalysisException: cannot resolve '`anme`' given input columns: [age, name];;
  //peopleDF.select("anme").show()

  //运行期报错
  //peopleDS.map(_.anme).show()

  peopleDS.map(_.name).show

  spark.stop()
}
case class Person(name: String, age: String)
