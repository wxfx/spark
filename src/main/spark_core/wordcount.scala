import org.apache.spark.{SparkContext, SparkConf}
object wordcount {
  def main(args: Array[String]): Unit = {
    println("HelloSpark!")
    val sparkConf = new SparkConf().setAppName("wordcount").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.textFile("/root/IdeaProjects/spark_train/src/main/data/hello.txt")
    val data = rdd.flatMap(_.split(" "))
                  .map((_ , 1))
                  //.reduceByKey(_+_).map(x =>x._1+'\t'+x._2)
        .groupByKey().map(x=>(x._1, x._2.sum))
    data.saveAsTextFile("/root/IdeaProjects/spark_train/src/main/data/wordcount")
    data.foreach(println)
    sc.stop()
  }
}
