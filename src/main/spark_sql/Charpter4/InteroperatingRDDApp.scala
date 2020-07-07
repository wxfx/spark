package Charpter4

import org.apache.spark.sql.{SparkSession, Row, DataFrame}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object InteroperatingRDDApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("DatasetApp")
      .getOrCreate()
    //runInferSchema(spark)


    runProgrammaticSchema(spark)


    spark.stop()
  }

  /**
   * 第二种方式自定义编程
   * @param spark
   * @return
   */
  def runProgrammaticSchema(spark:SparkSession) = {
    import spark.implicits._

    //step1
    val peopleRDD = spark.sparkContext.textFile("file:///root/IdeaProjects/spark_train/src/main/data/sparksql/people.txt")
    val peopleRowRDD = peopleRDD.map(_.split(",")) //RDD
      .map(x => Row(x(0), x(1).trim.toInt))

    //step2
    val struct = StructType(StructField("name", StringType, true) ::
      StructField("age", IntegerType, false) :: Nil)

    //step3
    val peopleDF = spark.createDataFrame(peopleRowRDD, struct)

    peopleDF.show()
    peopleRowRDD
  }

  /**
   * 第一种方式：反射
   * 1)定义case class
   * 2)RDD map, map中每一行数据转换成case class
   */

  def runInferSchema(spark: SparkSession): Unit = {
    import spark.implicits._

    val peopleRDD = spark.sparkContext.textFile("file:///root/IdeaProjects/spark_train/src/main/data/sparksql/people.txt")

    //TODO... RDD => DF
    val peopleDF = peopleRDD.map(_.split(","))
      .map(x => People(x(0), x(1).trim.toInt))
      .toDF()
    //peopleDF.show(false)

    peopleDF.createOrReplaceTempView("people")
    val queryDF = spark.sql("select name, age from people where age between 19 and 29")
    //queryDF.show()

    //queryDF.map(x => "name:" + x(0)).show() //from index
    queryDF.map(x => "name:" + x.getAs[String]("name")).show() //from field
  }
}
case class People(name:String, age:Int)
