import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.math.sqrt

object itemCF extends App{
  val conf = new SparkConf().setMaster("local[2]").setAppName("itemCF")
  val sc = new SparkContext(conf)
  val input_path = sc.textFile("/root/IdeaProjects/spark_train/src/main/data/train_new.data")

  //step 1 normalization, UI matrix
  val UIMatrix = input_path.map{x =>
    val ss = x.split("\t")
    val user_id = ss(0)
    val item_id = ss(1)
    val score = ss(2).toDouble
    (item_id, (user_id, score))
  }.groupByKey().flatMap{x =>
    val item_id = x._1
    val userScore = x._2
    var sumScore = 0.0
    val us_list = userScore.toArray
    val u_is_arr = new ArrayBuffer[(String, (String, Double))]
    for (i <- 0 until us_list.length){
      val score = us_list(i)._2.toDouble
      sumScore += score * score
    }
    sumScore = sqrt(sumScore)
    for (i <- 0 until us_list.length){
      val user_id = us_list(i)._1
      val score = us_list(i)._2.toDouble
      u_is_arr += ((user_id, (item_id, score / sumScore)))
    }
    u_is_arr
  }.groupByKey()
  //pair
  val iipair = UIMatrix.flatMap{x=>
    val user_id = x._1
    val is_arr = x._2.toArray
    val ii_s_arr = new ArrayBuffer[((String, String), Double)]

    for(i <- 0 until is_arr.length-1){
      for (j <- i+1 until is_arr.length){
        val item_i = is_arr(i)._1
        val item_j = is_arr(j)._1
        val s = is_arr(i)._2 * is_arr(j)._2
        ii_s_arr += (((item_i, item_j), s))
        ii_s_arr += (((item_j, item_i), s))
      }
    }
    ii_s_arr
  }.groupByKey()
  //step3 sum pari
  iipair.map{x=>
    val itemij = x._1 //map
    val ijscores_array = x._2.toArray //array
    var ijs = 0.0
    for (i <- 0 until ijscores_array.length){
      ijs += ijscores_array(i).toDouble
    }
    itemij._1 + "\t" + itemij._2 + "\t" + ijs.toString
  }.saveAsTextFile("/root/IdeaProjects/spark_train/src/main/data/itemCF")



  sc.stop()
}
