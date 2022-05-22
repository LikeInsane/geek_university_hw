# 第十周SPARK-RDD作业
## 题目一
### 实现 Compact table command
#### 1.要求：
添加 compact table 命令，用于合并小文件，例如表 test1 总共有 50000 个文件，每个 1MB，通过该命令，合成为 500 个文件，每个约 100MB。
#### 2.语法：
COMPACT TABLE table_identify [partitionSpec] [INTO fileNum FILES]；
#### 3.说明：
基本要求是完成以下功能：COMPACT TABLE test1 INTO 500 FILES；
如果添加 partitionSpec，则只合并指定的 partition 目录的文件；
如果不加 into fileNum files，则把表中的文件合并成 128MB 大小。

##### SqlBase.g4:
![image](https://user-images.githubusercontent.com/16860476/169702515-56fd31a5-961d-4491-9bae-415d297fa9d2.png)

##### SparkSqlParser.scala:
```scala
override def visitCompactTable(ctx: CompactTableContext): LogicalPlan = withOrigin(ctx) {
    val table: TableIdentifier = visitTableIdentifier(ctx.tableIdentifier())
    
    // 可选，所以对空值处理
    val fileNum = if (ctx.INTEGER_VALUE() != null) {
      Some(ctx.INTEGER_VALUE().getText.toInt)
    } else {
      None
    }
    
    // 可选，所以对空值处理
    val partition = if (ctx.partitionSpec() != null) {
      Some(ctx.partitionSpec().getText)
    } else {
      None
    }

    CompactTableCommand(table, fileNum, partition)
  }
```

##### SparkSqlParser.scala:
```scala
case class CompactTableCommand(
                                table: TableIdentifier,
                                fileNum: Option[Int],
                                partition: Option[String]
                              ) extends LeafRunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val dataDF: DataFrame = sparkSession.table(table)
    
    //未加fileNum，默认128mb合并
    val num: Int = fileNum match {
      case Some(i) => i
      case None =>
        (sparkSession
          .sessionState
          .executePlan(dataDF.queryExecution.logical)
          .optimizedPlan
          .stats.sizeInBytes / (1024L * 1024L * 128L)
          ).toInt
    }
    log.warn(s"fileNum is $num")
    val tmpTableName = table.identifier + "_tmp"
    dataDF.write.mode(SaveMode.Overwrite).saveAsTable(tmpTableName)

    if (partition.isEmpty) {
      sparkSession.table(tmpTableName)
        .repartition(num)
        .write.mode(SaveMode.Overwrite)
        .saveAsTable(table.identifier)
    } else {
    //选择按分区合并，需开启dynamic，否则会覆盖其余分区
      sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    //组装where条件
      val conExpr = partition.get.trim.stripPrefix("partition(").dropRight(1)
      log.warn(s"partition is $conExpr")

      sparkSession.table(tmpTableName)
        .where(conExpr)
        .repartition(num)
        .write.mode(SaveMode.Overwrite)
        .saveAsTable(table.identifier)
    }
    sparkSession.sql(s"drop table if exists $tmpTableName")
    log.warn("Compacte Table Completed.")
    Seq()
  }
}
```

### 输出结果
![image](https://user-images.githubusercontent.com/16860476/169703069-3a747cb2-1b6e-43d4-afa3-1ee104217962.png)
