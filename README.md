# 毕业设计
## 题目一: 分析一条 TPCDS SQL
### 分析一条 TPCDS SQL（请基于 Spark 3.1.1 版本解答）

#### 1.运行该 SQL，如 q38，并截图该 SQL 的 SQL 执行图
q38运行截图：
![1661066905(1)](https://user-images.githubusercontent.com/16860476/185780520-bc634e15-f816-4e59-b940-5e30df75531d.png)

#### 2.该 SQL 用到了哪些优化规则（optimizer rules）
##### 该 SQL 用到了如下优化规则：

org.apache.spark.sql.catalyst.optimizer.ColumnPruning

org.apache.spark.sql.catalyst.optimizer.ReplaceIntersectWithSemiJoin

org.apache.spark.sql.catalyst.optimizer.ReplaceDistinctWithAggregate

org.apache.spark.sql.catalyst.optimizer.ReorderJoin

org.apache.spark.sql.catalyst.optimizer.PushDownPredicates

org.apache.spark.sql.catalyst.optimizer.PushDownLeftSemiAntiJoin

org.apache.spark.sql.catalyst.optimizer.CollapseProject

org.apache.spark.sql.catalyst.optimizer.EliminateLimits

org.apache.spark.sql.catalyst.optimizer.ConstantFolding

org.apache.spark.sql.catalyst.optimizer.RemoveNoopOperators

org.apache.spark.sql.catalyst.optimizer.InferFiltersFromConstraints

org.apache.spark.sql.catalyst.optimizer.RewritePredicateSubquery

#### 3.请各用不少于 200 字描述其中的两条优化规则
##### RewritePredicateSubquery：

1.这个规则将谓词子查询重写为left semi/anti join。支持以下谓词：EXISTS/NOT EXISTS将被重写为semi/anti join，Filter中未解析的条件将被提取为join条件。IN/NOT IN将被重写为semi/anti join，Filter中未解析的条件将作为join条件被拉出，value=selected列也将用作join条件。

2.RewritePredicateSubquery是属于RewriteSubquery，RewriteSubquery还包含其它三个规则：ColumnPruning，CollapseProject，RemoveRedundantProject

##### ReorderJoin：

ReorderJoin 规则对 join 进行了重新排序，并将所有的条件下推到 join 中，使得过滤操作可以尽早发生。实则是贪心算法，基于代价的优化器，Spark 会根据 join 的成本选择代价最小的 join 操作，也就是有多个表 join，cbo 优化会按特定的顺序进行 join。多表连接顺序优化算法使用了动态规划寻找最优 join 顺序，优势在于动态规划算法能够求得整个搜索空间中最优解，而缺点在于当联接表数量增加时，算法需要搜索的空间增加的非常快，计算最优联接顺序代价很高。


## 题目二：架构设计题
### 你是某互联网公司的大数据平台架构师，请设计一套基于 Lambda 架构的数据平台架构，要求尽可能多的把课程中涉及的组件添加到该架构图中。并描述 Lambda 架构的优缺点，要求不少于 300 字。
#### Lambda架构图
![1661071675(1)](https://user-images.githubusercontent.com/16860476/185783318-4326ffb4-6936-4189-8bdc-70b6b11b1881.png)


#### 架构分析
单从lambda架构来看，主要分为三层：speed layer，batch layer，serveing layer；
数据接入：一般是通过flume采集日志，sqoop对接数据库，或者使用阿里的datax；实时则走kafka；数据存储在hdfs中并基于数仓分层（ods, dwd, dm, dim）,dim层一般会使用redis和hbase存储。

数据处理：batch layer基于spark或者mr来t+1处理，产生的offline view可以放在mysql（数据量小），或者hbase（数据量大）来查询；speed layer则使用flink, jstorm, ss，产生的realime view可以放在例如redis中查询。

数据服务：serveing layer主要用于合并两者的view，将最后结果输出到下游系统库中，一般可使用spark实现。

##### 架构优点
1.鲁棒性和容错能力。由于批处理层被设计为追加式，即包含了自开始以来的整体数据集，因此该系统具有一定的容错能力。如果任何数据被损坏，该架构则可以删除从损坏点以来的所有数据，并替换为正确的数据。

2.可扩展性。Lambda体系架构的设计层是作为分布式系统被构建的。因此，通过简单地添加更多的主机，最终用户可以轻松地对系统进行水平扩展。

3.低延迟的读取和更新。在Lambda体系架构中，speed layer为大数据系统提供了对于最新数据集的实时查询。

##### 架构缺点
1.因为要对数据进行大量存储，并且根据业务需求，两系统可能同时需要占用资源，对资源需求大。

2.部署复杂，需要部署离线及实时计算两套系统，给运维造成的负担比较重。

3.两种计算口径，业务需要根据不同口径分开编码维护，数据源的任何变化均涉及到两个部署的修改，任务量大，难以灵活应对。

4.随着数据量的急剧增加、批处理窗口时间内可能无法完成处理，并对存储也挑战巨大。
