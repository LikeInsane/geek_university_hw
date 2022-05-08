# 第六&七周SPARK-RDD作业
## 题目一
### 为 Spark SQL 添加一条自定义命令
1. SqlBase.g4添加语法规则
```
statement
| SHOW VERSION #showVersion
ansiNonReserved
| VERSION
nonReserved
| VERSION
//--SPARK-KEYWORD-LIST-START
VERSION: 'VERSION'
```
2. Maven antlr4 插件编译规则
3. 增加 ShowVersionCommand 类
```scala
package org.apache.spark.sql.execution.command

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.StringType


case class ShowVersionCommand() extends LeafRunnableCommand {

  override val output: Seq[Attribute] = Seq(AttributeReference("version", StringType)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sparkVersion = sparkSession.version
    val javaVersion = System.getProperty("java.version")
    val output = "Spark Version: %s, Java Version: %s".format(sparkVersion, javaVersion)
    Seq(Row(output))
  }
}

```
4. 修在 SparkSqlParser.scala 中增加 visitShowVersion 方法
```scala
override def visitShowVersion(ctx: ShowVersionContext): LogicalPlan = withOrigin(ctx) {
    ShowVersionCommand()
  }
```
### 输出结果
![微信截图_20220508223013](https://user-images.githubusercontent.com/16860476/167303819-4320916f-f572-4a36-b319-2440e99a92ab.png)

## 题目二
### 构建 SQL 满足如下要求
1.构建一条 SQL，同时 apply 下面三条优化规则：
CombineFilters
CollapseProject
BooleanSimplification
```sql
CREATE TABLE t1(a1 INT, a2 INT) USING parquet;
SELECT a11, (a2 + 1) AS a21 
FROM (
SELECT (a1 + 1) AS a11, a2 FROM t1 WHERE a1 > 10
) WHERE a11 > 1 AND 1 = 1;
```
### 输出结果
```
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.PushDownPredicates ===
 Project [a11#72, (a2#3 + 1) AS a21#73]             Project [a11#72, (a2#3 + 1) AS a21#73]
!+- Filter ((a11#72 > 1) AND (1 = 1))               +- Project [(a1#2 + 1) AS a11#72, a2#3]
!   +- Project [(a1#2 + 1) AS a11#72, a2#3]            +- Filter ((a1#2 > 10) AND (((a1#2 + 1) > 1) AND (1 = 1)))
!      +- Filter (a1#2 > 10)                              +- Relation pandi.t1[a1#2,a2#3] parquet
!         +- Relation pandi.t1[a1#2,a2#3] parquet

22/05/08 23:43:24 WARN [main] PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.CollapseProject ===
!Project [a11#72, (a2#3 + 1) AS a21#73]                          Project [(a1#2 + 1) AS a11#72, (a2#3 + 1) AS a21#73]
!+- Project [(a1#2 + 1) AS a11#72, a2#3]                         +- Filter ((a1#2 > 10) AND (((a1#2 + 1) > 1) AND (1 = 1)))
!   +- Filter ((a1#2 > 10) AND (((a1#2 + 1) > 1) AND (1 = 1)))      +- Relation pandi.t1[a1#2,a2#3] parquet
!      +- Relation pandi.t1[a1#2,a2#3] parquet

22/05/08 23:43:24 WARN [main] PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ConstantFolding ===
 Project [(a1#2 + 1) AS a11#72, (a2#3 + 1) AS a21#73]         Project [(a1#2 + 1) AS a11#72, (a2#3 + 1) AS a21#73]
!+- Filter ((a1#2 > 10) AND (((a1#2 + 1) > 1) AND (1 = 1)))   +- Filter ((a1#2 > 10) AND (((a1#2 + 1) > 1) AND true))
    +- Relation pandi.t1[a1#2,a2#3] parquet                      +- Relation pandi.t1[a1#2,a2#3] parquet

22/05/08 23:43:24 WARN [main] PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.BooleanSimplification ===
 Project [(a1#2 + 1) AS a11#72, (a2#3 + 1) AS a21#73]      Project [(a1#2 + 1) AS a11#72, (a2#3 + 1) AS a21#73]
!+- Filter ((a1#2 > 10) AND (((a1#2 + 1) > 1) AND true))   +- Filter ((a1#2 > 10) AND ((a1#2 + 1) > 1))
    +- Relation pandi.t1[a1#2,a2#3] parquet                   +- Relation pandi.t1[a1#2,a2#3] parquet

22/05/08 23:43:24 WARN [main] PlanChangeLogger:
=== Result of Batch Operator Optimization before Inferring Filters ===
!Project [a11#72, (a2#3 + 1) AS a21#73]             Project [(a1#2 + 1) AS a11#72, (a2#3 + 1) AS a21#73]
!+- Filter ((a11#72 > 1) AND (1 = 1))               +- Filter ((a1#2 > 10) AND ((a1#2 + 1) > 1))
!   +- Project [(a1#2 + 1) AS a11#72, a2#3]            +- Relation pandi.t1[a1#2,a2#3] parquet
!      +- Filter (a1#2 > 10)
!         +- Relation pandi.t1[a1#2,a2#3] parquet

22/05/08 23:43:24 WARN [main] PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.InferFiltersFromConstraints ===
 Project [(a1#2 + 1) AS a11#72, (a2#3 + 1) AS a21#73]   Project [(a1#2 + 1) AS a11#72, (a2#3 + 1) AS a21#73]
!+- Filter ((a1#2 > 10) AND ((a1#2 + 1) > 1))           +- Filter (isnotnull(a1#2) AND ((a1#2 > 10) AND ((a1#2 + 1) > 1)))
    +- Relation pandi.t1[a1#2,a2#3] parquet                +- Relation pandi.t1[a1#2,a2#3] parquet
```

2.构建一条 SQL，同时 apply 下面五条优化规则：
ConstantFolding
PushDownPredicates
ReplaceDistinctWithAggregate
ReplaceExceptWithAntiJoin
FoldablePropagation
```sql
CREATE TABLE t1(a1 INT, a2 INT) USING parquet;
CREATE TABLE t2(b1 INT, b2 INT) USING parquet;
SELECT DISTINCT a1, a2, 'custom' a3 
FROM (
SELECT * FROM t1 WHERE a2 = 10 AND 1 = 1
) WHERE a1 > 5 AND 1 = 1
EXCEPT SELECT b1, b2, 1.0 b3 FROM t2 WHERE b2 = 10 ;
```
### 输出结果
```
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.PushDownPredicates ===
 Aggregate [a1#2, a2#3, a3#76], [a1#2, a2#3, a3#76]                                  Aggregate [a1#2, a2#3, a3#76], [a1#2, a2#3, a3#76]
 +- Join LeftAnti, (((a1#2 <=> b1#78) AND (a2#3 <=> b2#79)) AND (a3#76 <=> b3#80))   +- Join LeftAnti, (((a1#2 <=> b1#78) AND (a2#3 <=> b2#79)) AND (a3#76 <=> b3#80))
    :- Aggregate [a1#2, a2#3, a3#76], [a1#2, a2#3, a3#76]                               :- Aggregate [a1#2, a2#3, a3#76], [a1#2, a2#3, a3#76]
    :  +- Project [a1#2, a2#3, custom AS a3#76]                                         :  +- Project [a1#2, a2#3, custom AS a3#76]
!   :     +- Filter ((a1#2 > 5) AND (1 = 1))                                            :     +- Filter (((a2#3 = 10) AND (1 = 1)) AND (a1#2 > 5))
!   :        +- Filter ((a2#3 = 10) AND (1 = 1))                                        :        +- Relation pandi.t1[a1#2,a2#3] parquet
!   :           +- Relation pandi.t1[a1#2,a2#3] parquet                                 +- Project [b1#78, b2#79, cast(b3#77 as string) AS b3#80]
!   +- Project [b1#78, b2#79, cast(b3#77 as string) AS b3#80]                              +- Project [b1#78, b2#79, 1.0 AS b3#77]
!      +- Project [b1#78, b2#79, 1.0 AS b3#77]                                                +- Filter (b2#79 = 10)
!         +- Filter (b2#79 = 10)                                                                 +- Relation pandi.t2[b1#78,b2#79] parquet
!            +- Relation pandi.t2[b1#78,b2#79] parquet

22/05/08 23:45:21 WARN [main] PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.PushDownLeftSemiAntiJoin ===
 Aggregate [a1#2, a2#3, a3#76], [a1#2, a2#3, a3#76]                                  Aggregate [a1#2, a2#3, a3#76], [a1#2, a2#3, a3#76]
!+- Join LeftAnti, (((a1#2 <=> b1#78) AND (a2#3 <=> b2#79)) AND (a3#76 <=> b3#80))   +- Aggregate [a1#2, a2#3, a3#76], [a1#2, a2#3, a3#76]
!   :- Aggregate [a1#2, a2#3, a3#76], [a1#2, a2#3, a3#76]                               +- Project [a1#2, a2#3, custom AS a3#76]
!   :  +- Project [a1#2, a2#3, custom AS a3#76]                                            +- Join LeftAnti, (((a1#2 <=> b1#78) AND (a2#3 <=> b2#79)) AND (custom <=> b3#80))
!   :     +- Filter (((a2#3 = 10) AND (1 = 1)) AND (a1#2 > 5))                                :- Filter (((a2#3 = 10) AND (1 = 1)) AND (a1#2 > 5))
!   :        +- Relation pandi.t1[a1#2,a2#3] parquet                                          :  +- Relation pandi.t1[a1#2,a2#3] parquet
!   +- Project [b1#78, b2#79, cast(b3#77 as string) AS b3#80]                                 +- Project [b1#78, b2#79, cast(b3#77 as string) AS b3#80]
!      +- Project [b1#78, b2#79, 1.0 AS b3#77]                                                   +- Project [b1#78, b2#79, 1.0 AS b3#77]
!         +- Filter (b2#79 = 10)                                                                    +- Filter (b2#79 = 10)
!            +- Relation pandi.t2[b1#78,b2#79] parquet                                                 +- Relation pandi.t2[b1#78,b2#79] parquet

22/05/08 23:45:21 WARN [main] PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.CollapseProject ===
 Aggregate [a1#2, a2#3, a3#76], [a1#2, a2#3, a3#76]                                         Aggregate [a1#2, a2#3, a3#76], [a1#2, a2#3, a3#76]
 +- Aggregate [a1#2, a2#3, a3#76], [a1#2, a2#3, a3#76]                                      +- Aggregate [a1#2, a2#3, a3#76], [a1#2, a2#3, a3#76]
    +- Project [a1#2, a2#3, custom AS a3#76]                                                   +- Project [a1#2, a2#3, custom AS a3#76]
       +- Join LeftAnti, (((a1#2 <=> b1#78) AND (a2#3 <=> b2#79)) AND (custom <=> b3#80))         +- Join LeftAnti, (((a1#2 <=> b1#78) AND (a2#3 <=> b2#79)) AND (custom <=> b3#80))
          :- Filter (((a2#3 = 10) AND (1 = 1)) AND (a1#2 > 5))                                       :- Filter (((a2#3 = 10) AND (1 = 1)) AND (a1#2 > 5))
          :  +- Relation pandi.t1[a1#2,a2#3] parquet                                                 :  +- Relation pandi.t1[a1#2,a2#3] parquet
!         +- Project [b1#78, b2#79, cast(b3#77 as string) AS b3#80]                                  +- Project [b1#78, b2#79, cast(1.0 as string) AS b3#80]
!            +- Project [b1#78, b2#79, 1.0 AS b3#77]                                                    +- Filter (b2#79 = 10)
!               +- Filter (b2#79 = 10)                                                                     +- Relation pandi.t2[b1#78,b2#79] parquet
!                  +- Relation pandi.t2[b1#78,b2#79] parquet

22/05/08 23:45:21 WARN [main] PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.FoldablePropagation ===
!Aggregate [a1#2, a2#3, a3#76], [a1#2, a2#3, a3#76]                                         Aggregate [a1#2, a2#3, custom], [a1#2, a2#3, custom AS a3#76]
!+- Aggregate [a1#2, a2#3, a3#76], [a1#2, a2#3, a3#76]                                      +- Aggregate [a1#2, a2#3, custom], [a1#2, a2#3, custom AS a3#76]
    +- Project [a1#2, a2#3, custom AS a3#76]                                                   +- Project [a1#2, a2#3, custom AS a3#76]
!      +- Join LeftAnti, (((a1#2 <=> b1#78) AND (a2#3 <=> b2#79)) AND (custom <=> b3#80))         +- Join LeftAnti, (((a1#2 <=> b1#78) AND (a2#3 <=> b2#79)) AND (custom <=> cast(1.0 as string)))
          :- Filter (((a2#3 = 10) AND (1 = 1)) AND (a1#2 > 5))                                       :- Filter (((a2#3 = 10) AND (1 = 1)) AND (a1#2 > 5))
          :  +- Relation pandi.t1[a1#2,a2#3] parquet                                                 :  +- Relation pandi.t1[a1#2,a2#3] parquet
          +- Project [b1#78, b2#79, cast(1.0 as string) AS b3#80]                                    +- Project [b1#78, b2#79, cast(1.0 as string) AS b3#80]
             +- Filter (b2#79 = 10)                                                                     +- Filter (b2#79 = 10)
                +- Relation pandi.t2[b1#78,b2#79] parquet                                                  +- Relation pandi.t2[b1#78,b2#79] parquet

22/05/08 23:45:21 WARN [main] PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ConstantFolding ===
 Aggregate [a1#2, a2#3, custom], [a1#2, a2#3, custom AS a3#76]                                            Aggregate [a1#2, a2#3, custom], [a1#2, a2#3, custom AS a3#76]
 +- Aggregate [a1#2, a2#3, custom], [a1#2, a2#3, custom AS a3#76]                                         +- Aggregate [a1#2, a2#3, custom], [a1#2, a2#3, custom AS a3#76]
    +- Project [a1#2, a2#3, custom AS a3#76]                                                                 +- Project [a1#2, a2#3, custom AS a3#76]
!      +- Join LeftAnti, (((a1#2 <=> b1#78) AND (a2#3 <=> b2#79)) AND (custom <=> cast(1.0 as string)))         +- Join LeftAnti, (((a1#2 <=> b1#78) AND (a2#3 <=> b2#79)) AND false)
!         :- Filter (((a2#3 = 10) AND (1 = 1)) AND (a1#2 > 5))                                                     :- Filter (((a2#3 = 10) AND true) AND (a1#2 > 5))
          :  +- Relation pandi.t1[a1#2,a2#3] parquet                                                               :  +- Relation pandi.t1[a1#2,a2#3] parquet
!         +- Project [b1#78, b2#79, cast(1.0 as string) AS b3#80]                                                  +- Project [b1#78, b2#79, 1.0 AS b3#80]
             +- Filter (b2#79 = 10)                                                                                   +- Filter (b2#79 = 10)
                +- Relation pandi.t2[b1#78,b2#79] parquet                                                                +- Relation pandi.t2[b1#78,b2#79] parquet

22/05/08 23:45:21 WARN [main] PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.BooleanSimplification ===
 Aggregate [a1#2, a2#3, custom], [a1#2, a2#3, custom AS a3#76]                 Aggregate [a1#2, a2#3, custom], [a1#2, a2#3, custom AS a3#76]
 +- Aggregate [a1#2, a2#3, custom], [a1#2, a2#3, custom AS a3#76]              +- Aggregate [a1#2, a2#3, custom], [a1#2, a2#3, custom AS a3#76]
    +- Project [a1#2, a2#3, custom AS a3#76]                                      +- Project [a1#2, a2#3, custom AS a3#76]
!      +- Join LeftAnti, (((a1#2 <=> b1#78) AND (a2#3 <=> b2#79)) AND false)         +- Join LeftAnti, false
!         :- Filter (((a2#3 = 10) AND true) AND (a1#2 > 5))                             :- Filter ((a2#3 = 10) AND (a1#2 > 5))
          :  +- Relation pandi.t1[a1#2,a2#3] parquet                                    :  +- Relation pandi.t1[a1#2,a2#3] parquet
          +- Project [b1#78, b2#79, 1.0 AS b3#80]                                       +- Project [b1#78, b2#79, 1.0 AS b3#80]
             +- Filter (b2#79 = 10)                                                        +- Filter (b2#79 = 10)
                +- Relation pandi.t2[b1#78,b2#79] parquet                                     +- Relation pandi.t2[b1#78,b2#79] parquet

22/05/08 23:45:21 WARN [main] PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.RemoveRedundantAggregates ===
 Aggregate [a1#2, a2#3, custom], [a1#2, a2#3, custom AS a3#76]      Aggregate [a1#2, a2#3, custom], [a1#2, a2#3, custom AS a3#76]
!+- Aggregate [a1#2, a2#3, custom], [a1#2, a2#3, custom AS a3#76]   +- Project [a1#2, a2#3, custom AS a3#76]
!   +- Project [a1#2, a2#3, custom AS a3#76]                           +- Join LeftAnti, false
!      +- Join LeftAnti, false                                            :- Filter ((a2#3 = 10) AND (a1#2 > 5))
!         :- Filter ((a2#3 = 10) AND (a1#2 > 5))                          :  +- Relation pandi.t1[a1#2,a2#3] parquet
!         :  +- Relation pandi.t1[a1#2,a2#3] parquet                      +- Project [b1#78, b2#79, 1.0 AS b3#80]
!         +- Project [b1#78, b2#79, 1.0 AS b3#80]                            +- Filter (b2#79 = 10)
!            +- Filter (b2#79 = 10)                                             +- Relation pandi.t2[b1#78,b2#79] parquet
!               +- Relation pandi.t2[b1#78,b2#79] parquet

22/05/08 23:45:21 WARN [main] PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ColumnPruning ===
 Aggregate [a1#2, a2#3, custom], [a1#2, a2#3, custom AS a3#76]   Aggregate [a1#2, a2#3, custom], [a1#2, a2#3, custom AS a3#76]
!+- Project [a1#2, a2#3, custom AS a3#76]                        +- Project [a1#2, a2#3]
!   +- Join LeftAnti, false                                         +- Project [a1#2, a2#3]
!      :- Filter ((a2#3 = 10) AND (a1#2 > 5))                          +- Join LeftAnti, false
!      :  +- Relation pandi.t1[a1#2,a2#3] parquet                         :- Filter ((a2#3 = 10) AND (a1#2 > 5))
!      +- Project [b1#78, b2#79, 1.0 AS b3#80]                            :  +- Relation pandi.t1[a1#2,a2#3] parquet
!         +- Filter (b2#79 = 10)                                          +- Project
!            +- Relation pandi.t2[b1#78,b2#79] parquet                       +- Project
!                                                                               +- Filter (b2#79 = 10)
!                                                                                  +- Relation pandi.t2[b1#78,b2#79] parquet

22/05/08 23:45:21 WARN [main] PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.CollapseProject ===
 Aggregate [a1#2, a2#3, custom], [a1#2, a2#3, custom AS a3#76]   Aggregate [a1#2, a2#3, custom], [a1#2, a2#3, custom AS a3#76]
 +- Project [a1#2, a2#3]                                         +- Project [a1#2, a2#3]
!   +- Project [a1#2, a2#3]                                         +- Join LeftAnti, false
!      +- Join LeftAnti, false                                         :- Filter ((a2#3 = 10) AND (a1#2 > 5))
!         :- Filter ((a2#3 = 10) AND (a1#2 > 5))                       :  +- Relation pandi.t1[a1#2,a2#3] parquet
!         :  +- Relation pandi.t1[a1#2,a2#3] parquet                   +- Project
!         +- Project                                                      +- Filter (b2#79 = 10)
!            +- Project                                                      +- Relation pandi.t2[b1#78,b2#79] parquet
!               +- Filter (b2#79 = 10)
!                  +- Relation pandi.t2[b1#78,b2#79] parquet

22/05/08 23:45:21 WARN [main] PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.RemoveNoopOperators ===
 Aggregate [a1#2, a2#3, custom], [a1#2, a2#3, custom AS a3#76]   Aggregate [a1#2, a2#3, custom], [a1#2, a2#3, custom AS a3#76]
!+- Project [a1#2, a2#3]                                         +- Join LeftAnti, false
!   +- Join LeftAnti, false                                         :- Filter ((a2#3 = 10) AND (a1#2 > 5))
!      :- Filter ((a2#3 = 10) AND (a1#2 > 5))                       :  +- Relation pandi.t1[a1#2,a2#3] parquet
!      :  +- Relation pandi.t1[a1#2,a2#3] parquet                   +- Project
!      +- Project                                                      +- Filter (b2#79 = 10)
!         +- Filter (b2#79 = 10)                                          +- Relation pandi.t2[b1#78,b2#79] parquet
!            +- Relation pandi.t2[b1#78,b2#79] parquet

22/05/08 23:45:21 WARN [main] PlanChangeLogger:
=== Result of Batch Operator Optimization before Inferring Filters ===
!Aggregate [a1#2, a2#3, a3#76], [a1#2, a2#3, a3#76]                                  Aggregate [a1#2, a2#3, custom], [a1#2, a2#3, custom AS a3#76]
!+- Join LeftAnti, (((a1#2 <=> b1#78) AND (a2#3 <=> b2#79)) AND (a3#76 <=> b3#80))   +- Join LeftAnti, false
!   :- Aggregate [a1#2, a2#3, a3#76], [a1#2, a2#3, a3#76]                               :- Filter ((a2#3 = 10) AND (a1#2 > 5))
!   :  +- Project [a1#2, a2#3, custom AS a3#76]                                         :  +- Relation pandi.t1[a1#2,a2#3] parquet
!   :     +- Filter ((a1#2 > 5) AND (1 = 1))                                            +- Project
!   :        +- Filter ((a2#3 = 10) AND (1 = 1))                                           +- Filter (b2#79 = 10)
!   :           +- Relation pandi.t1[a1#2,a2#3] parquet                                       +- Relation pandi.t2[b1#78,b2#79] parquet
!   +- Project [b1#78, b2#79, cast(b3#77 as string) AS b3#80]
!      +- Project [b1#78, b2#79, 1.0 AS b3#77]
!         +- Filter (b2#79 = 10)
!            +- Relation pandi.t2[b1#78,b2#79] parquet

22/05/08 23:45:21 WARN [main] PlanChangeLogger:
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.InferFiltersFromConstraints ===
 Aggregate [a1#2, a2#3, custom], [a1#2, a2#3, custom AS a3#76]   Aggregate [a1#2, a2#3, custom], [a1#2, a2#3, custom AS a3#76]
 +- Join LeftAnti, false                                         +- Join LeftAnti, false
!   :- Filter ((a2#3 = 10) AND (a1#2 > 5))                          :- Filter ((isnotnull(a2#3) AND isnotnull(a1#2)) AND ((a2#3 = 10) AND (a1#2 > 5)))
    :  +- Relation pandi.t1[a1#2,a2#3] parquet                      :  +- Relation pandi.t1[a1#2,a2#3] parquet
    +- Project                                                      +- Project
!      +- Filter (b2#79 = 10)                                          +- Filter (isnotnull(b2#79) AND (b2#79 = 10))
          +- Relation pandi.t2[b1#78,b2#79] parquet                       +- Relation pandi.t2[b1#78,b2#79] parquet
```
## 题目三
### 实现自定义优化规则（静默规则）
第一步：实现自定义规则
```scala
package week8.spark.sql.extensions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Literal, Multiply}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

case class MyPushDown(spark: SparkSession) extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions  {
    case Multiply(left, right, failOnError) if right.isInstanceOf[Literal] && right.asInstanceOf[Literal].value.asInstanceOf[Int] == 1 => left
    case Multiply(left, right, failOnError) if left.isInstanceOf[Literal] && left.asInstanceOf[Literal].value.asInstanceOf[Int] == 1 => right
  }
}
```
第二步：创建自己的 Extension 并注入
```scala
package week8.spark.sql.extensions

import org.apache.spark.sql.SparkSessionExtensions

class MySparkSessionExtension extends (SparkSessionExtensions => Unit) {
  override def apply(v1: SparkSessionExtensions): Unit = {
    v1.injectOptimizerRule {
      session => new MyPushDown(session)
    }
  }
}
```
第三步：通过 spark.sql.extensions 提交
### 输出结果
```
=== Applying Rule week8.spark.sql.extensions.MyPushDown ===
!Project [a1#10, (a2#11 * 1) AS (a2 * 1)#12]   Project [a1#10, a2#11 AS (a2 * 1)#12]
 +- Relation default.t1[a1#10,a2#11] parquet   +- Relation default.t1[a1#10,a2#11] parquet
```
