# 第六&七周SPARK-RDD作业
## 题目一
### 使用 RDD API 实现带词频的倒排索引
```java
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

```
### 输出结果
![hw1](https://user-images.githubusercontent.com/16860476/163822163-273551cf-ed9d-42ab-b3b2-b98d0dfad3ce.png)

## 题目二
### Distcp 的 Spark 实现
```java
package week6.spark.rdd.hw

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

case class SparkDistCpOptions(maxConcurrence: Int, ignoreFailures: Boolean)

object DistCp {


  def getCopyDirList(sc: SparkContext, soPath: Path, tarPath: Path, fileList: ArrayBuffer[(Path, Path)], options: SparkDistCpOptions): Unit = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.listStatus(soPath) //获取文件列表
      .foreach(cuPath => {
        if (cuPath.isDirectory) {
          val childPath = cuPath.getPath.toString.split(soPath.toString)(1)
          val nextTargetPath = new Path(tarPath + childPath)
          try {
            fs.mkdirs(nextTargetPath) //在目标路径下建立对应目录
          } catch {
            case e: Exception => if (!options.ignoreFailures) throw e else e.getMessage
          }
          getCopyDirList(sc, cuPath.getPath, nextTargetPath, fileList, options) //递归调用生产文件树
        } else {
          fileList.append((cuPath.getPath, tarPath))
        }
      })
  }

  def doCopy(sc: SparkContext, fileList: ArrayBuffer[(Path, Path)], options: SparkDistCpOptions): Unit = {
    val fileRdd = sc.parallelize(fileList, options.maxConcurrence)

    fileRdd.mapPartitions(ite => {
      val conf = new Configuration()
      ite.foreach(tup => {
        try {
          FileUtil.copy(tup._1.getFileSystem(conf), tup._1, tup._2.getFileSystem(conf), tup._2, false, conf) //使用FileUtil，根据fileList将文件复制
        } catch {
          case e: Exception => if (!options.ignoreFailures) throw e else e.getMessage
        }
      })
      ite
    }).collect()
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DistCp").setMaster("local")
    val sc = new SparkContext(conf)

    val soPath = new Path(args.head) //源
    val tarPath = new Path(args(1)) //目标

    val fileList = new ArrayBuffer[(Path, Path)]()

    val options = SparkDistCpOptions(args(2).toInt, args(3).toBoolean) //discp参数: -m:最大并发数；-i 忽略失败

    getCopyDirList(sc, soPath, tarPath, fileList, options)//拿到文件树

    doCopy(sc, fileList, options)//复制

    sc.stop()
  }
}
```
### 输出结果
执行命令
```
spark-submit --class week6.spark.rdd.hw.DistCp geek-time-big-data-1.0-SNAPSHOT.jar pandi/spark pandi/sparkCopy 4 true
```
结果
![image](https://user-images.githubusercontent.com/16860476/163822539-0386d639-0749-401f-a391-07663d5985a1.png)
参数设置
![1650292103(1)](https://user-images.githubusercontent.com/16860476/163822752-47b14ac0-6e9c-4063-aaa2-fc31346bb9b8.png)
