## Code
```scala
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{sum, avg}

case class Info(className: Int, stuName: String, age: Int, sex: String, course: String, score: Int)

object SparkHW {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hw")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    //1. 读取文件的数据test.txt
    val rdd = spark.read.textFile(this.getClass.getClassLoader.getResource("test.txt").getPath).map(x => x.split(" "))
    val df = rdd.map(x => Info(x(0).toInt, x(1), x(2).toInt, x(3), x(4), x(5).toInt)).toDF().cache()
    //2. 一共有多少个小于20岁的人参加考试？
    df.where("age <= 20").selectExpr("count(distinct stuName) stus").show()
    //3. 一共有多少个等于20岁的人参加考试？
    df.where("age = 20").selectExpr("count(distinct stuName) stus").show()
    //4. 一共有多少个大于20岁的人参加考试？
    df.where("age > 20").selectExpr("count(distinct stuName) stus").show()
    //5. 一共有多个男生参加考试？
    df.where("sex = '男'").selectExpr("count(distinct stuName) stus").show()
    //6. 一共有多少个女生参加考试？
    df.where("sex = '女'").selectExpr("count(distinct stuName) stus").show()
    //7. 12班有多少人参加考试？
    df.where("className = 12").selectExpr("count(distinct stuName) stus").show()
    //8. 13班有多少人参加考试？
    df.where("className = 13").selectExpr("count(distinct stuName) stus").show()
    //9. 语文科目的平均成绩是多少？
    df.where("course = 'chinese'").selectExpr("avg(score) avg_score").show()
    //10. 数学科目的平均成绩是多少？
    df.where("course = 'math'").selectExpr("avg(score) avg_score").show()
    //11. 英语科目的平均成绩是多少？
    df.where("course = 'english'").selectExpr("avg(score) avg_score").show()
    //12. 每个人平均成绩是多少？
    df.groupBy("stuName").avg("score").show()
    //13. 12班平均成绩是多少？
    df.groupBy("className").avg("score").where("className = 12").show()
    //14. 12班男生平均总成绩是多少？
    df.where("className = 12 and sex = '男'").groupBy("className").avg("score").show()
    //15. 12班女生平均总成绩是多少？
    df.where("className = 12 and sex = '女'").groupBy("className").avg("score").show()
    //16. 13班平均成绩是多少？
    df.where("className = 13").groupBy("className").avg("score").show()
    //17. 13班男生平均总成绩是多少？
    df.where("className = 13 and sex = '男'").groupBy("className").avg("score").show()
    //18. 13班女生平均总成绩是多少？
    df.where("className = 13 and sex = '女'").groupBy("className").avg("score").show()
    //19. 全校语文成绩最高分是多少？
    df.where("course = 'chinese'").sort($"score".desc).limit(1).show()
    //20. 12班语文成绩最低分是多少？
    df.where("className = 12 and course = 'chinese'").sort($"score").limit(1).show()
    //21. 13班数学最高成绩是多少？
    df.where("className = 13 and course = 'math'").sort($"score".desc).limit(1).show()
    //22. 总成绩大于150分的12班的女生有几个？
    df.where("className = 12 and sex = '女'").groupBy("stuName").sum("score")
      .where("sum(score) > 150").selectExpr("count(stuName)").show()
    //23. 总成绩大于150分，且数学大于等于70，且年龄大于等于19岁的学生的平均成绩是多少？
    df.groupBy("stuName").agg(sum("score").alias("total_score"), avg("score").alias("avg_score"))
      .where("total_score > 150")
      .join(df.where("course = 'math' and score >= 70 and age >= 19"), "stuName")
      .select("stuName", "avg_score")
      .show()

    spark.close()
  }
}
```
## Answer
```
+----+
|stus|
+----+
|   4|
+----+

+----+
|stus|
+----+
|   2|
+----+

+----+
|stus|
+----+
|   2|
+----+

+----+
|stus|
+----+
|   4|
+----+

+----+
|stus|
+----+
|   2|
+----+

+----+
|stus|
+----+
|   3|
+----+

+----+
|stus|
+----+
|   3|
+----+

+------------------+
|         avg_score|
+------------------+
|58.333333333333336|
+------------------+

+------------------+
|         avg_score|
+------------------+
|63.333333333333336|
+------------------+

+------------------+
|         avg_score|
+------------------+
|63.333333333333336|
+------------------+

+-------+------------------+
|stuName|        avg(score)|
+-------+------------------+
|   王英| 73.33333333333333|
|   李逵|63.333333333333336|
|   杨春|              70.0|
|   宋江|              60.0|
|   林冲|53.333333333333336|
|   吴用|              50.0|
+-------+------------------+

+---------+----------+
|className|avg(score)|
+---------+----------+
|       12|      60.0|
+---------+----------+

+---------+----------+
|className|avg(score)|
+---------+----------+
|       12|      55.0|
+---------+----------+

+---------+----------+
|className|avg(score)|
+---------+----------+
|       12|      70.0|
+---------+----------+

+---------+------------------+
|className|        avg(score)|
+---------+------------------+
|       13|63.333333333333336|
+---------+------------------+

+---------+------------------+
|className|        avg(score)|
+---------+------------------+
|       13|58.333333333333336|
+---------+------------------+

+---------+-----------------+
|className|       avg(score)|
+---------+-----------------+
|       13|73.33333333333333|
+---------+-----------------+

+---------+-------+---+---+-------+-----+
|className|stuName|age|sex| course|score|
+---------+-------+---+---+-------+-----+
|       12|   杨春| 19| 女|chinese|   70|
+---------+-------+---+---+-------+-----+

+---------+-------+---+---+-------+-----+
|className|stuName|age|sex| course|score|
+---------+-------+---+---+-------+-----+
|       12|   宋江| 25| 男|chinese|   50|
+---------+-------+---+---+-------+-----+

+---------+-------+---+---+------+-----+
|className|stuName|age|sex|course|score|
+---------+-------+---+---+------+-----+
|       13|   王英| 19| 女|  math|   80|
+---------+-------+---+---+------+-----+

+--------------+
|count(stuName)|
+--------------+
|             1|
+--------------+

+-------+-----------------+
|stuName|        avg_score|
+-------+-----------------+
|   王英|73.33333333333333|
|   杨春|             70.0|
+-------+-----------------+
```
