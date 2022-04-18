package week6.spark.rdd.hw

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object InvertedIndex {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("InvertedIndex").setMaster("local")
    val sc = new SparkContext(conf)

    val path = "pandi/spark/rdd/input/"

    sc.wholeTextFiles(path) //读源
      .flatMapValues(v => v.split(" ")) //将内容按" "切分=>(fileName, word)
      .flatMap {
        case (path, word) => path.split("/").last //获取文件名
          .map(fileName => (word, fileName))
      }
      .groupByKey() //根据key分组计算词频和单词出现在哪些文件中
      .map {
        case (word, fileNames) =>
          val wordCountMap = new mutable.HashMap[String, Int]()
          for (fn <- fileNames) {
            wordCountMap.put(fn.toString, wordCountMap.getOrElseUpdate(fn.toString, 0) + 1)
          }
          (word, wordCountMap)
      }
      .sortByKey() //按照作业格式输出
      .map(word => s"${word._1}: ${word._2.toArray.mkString("{", ", ", "}")}")
      .foreach(println)

    sc.stop()
  }

}
