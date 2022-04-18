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
