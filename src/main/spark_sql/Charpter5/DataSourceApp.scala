package Charpter5

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}

object DataSourceApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .getOrCreate()
    //text(spark)
    //json(spark)
    //common(spark)
    //parquet(spark)
    //convertJson2Parquet(spark)
    //jdbc(spark)
    import spark.implicits._
    val config = ConfigFactory.load()
    val url = config.getString("db.default.url")
    val user = config.getString("db.default.user")
    val password = config.getString("db.default.password")
    val driver = config.getString("db.default.driver")
    val database = config.getString("db.default.database")
    val table = config.getString("db.default.table")
    val sinkTable = config.getString("db.default.sink.table")

    val connectionProperties = new Properties()
    connectionProperties.put("user", user)
    connectionProperties.put("password", password)

    val jdbcDF = spark.read.jdbc(url, s"$database.$table", connectionProperties)
    //jdbcDF.show()
    jdbcDF.write.jdbc(url, s"$database.$sinkTable", connectionProperties)




  }


  private def jdbc(spark: SparkSession) = {
    import spark.implicits._

    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://master:3306")
      .option("dbtable", "spark.student")
      .option("user", "root")
      .option("password", "123456")
      .load()
    jdbcDF.show(10)

    //死去活来法

    val url = "jdbc:mysql://master:3306"
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "123456")

    val jdbcDF2 = spark.read.jdbc(url, "spark.student", connectionProperties)
    jdbcDF2.filter($"id" > 1).write.jdbc(url, "spark.student2", connectionProperties)

    spark.read.jdbc(url, "spark.student2", connectionProperties).show()
  }

  //存储类型转换:JSON=>Parquet
  private def convertJson2Parquet(spark: SparkSession) = {
    import spark.implicits._
    val jsonDF = spark.read.format("json").load("file:///root/IdeaProjects/spark_train/src/main/data/sparksql/people.json")
    //jsonDF.show()
    jsonDF.filter("age>20")
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save("out")
    spark.read.parquet("file:///root/IdeaProjects/spark_train/out").show()
  }

  //parquet数据源
  private def parquet(spark: SparkSession) = {
    import spark.implicits._

    val parquetDF = spark.read.parquet("file:///root/IdeaProjects/spark_train/src/main/data/sparksql/users.parquet")
    //parquetDF.printSchema()
    //parquetDF.show()

    parquetDF.select("name", "favorite_numbers")
      .write.mode("overwrite")
      .option("compress", "none")
      .parquet("out")
    spark.read.parquet("file:///root/IdeaProjects/spark_train/out").show()
  }

  //标准api写法
  private def common(spark: SparkSession) = {
    import spark.implicits._
    //源码
    val textDF = spark.read.format("text").load("file:///root/IdeaProjects/spark_train/src/main/data/sparksql/people.txt")
    val jsonDF = spark.read.format("json").load("file:///root/IdeaProjects/spark_train/src/main/data/sparksql/people.json")
    //textDF.show()
    //println("~~~~~~~~~~~~~~~~~~~~~~")
    //jsonDF.show()

    jsonDF.write.format("json").mode(SaveMode.Overwrite).save("out")
  }

  //json
  private def json(spark: SparkSession) = {
    import spark.implicits._
    val jsonDF = spark.read.json("file:///root/IdeaProjects/spark_train/src/main/data/sparksql/people.json")
    //jsonDF.show()
    //TODO... 只要age>20的数据
    //jsonDF.filter("age > 20")
    //  .select("name")
    //  .write.mode(SaveMode.Overwrite)
    //  .json("out")

    //将列为json数据拆开
    val jsonDF2 = spark.read.json("file:///root/IdeaProjects/spark_train/src/main/data/sparksql/people2.json")
    jsonDF2.select($"name", $"age", $"info.work".as("word"),
      $"info.home".as("home")).write.mode("overwrite").json("out")
  }

  //文本
  private def text(spark: SparkSession) = {
    import spark.implicits._
    val textDF = spark.read.text("/root/IdeaProjects/spark_train/src/main/data/sparksql/people.txt")
    //textDF.show()
    val result = textDF.map(x => {
      val splits = x.getString(0)
        .split(",")
      (splits(0).trim) //, splits(1).trim
    })
    //result.collect().foreach(println)
    //只支持一列，如果有两列就会报错, 默认输出到当前项目的根目录的out目录下
    //overwrite 覆盖
    result.write.mode("overwrite").text("out")
  }
}
