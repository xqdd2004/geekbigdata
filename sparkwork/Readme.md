

# 一、作业1概述
##     1、倒排索引概念（来自维基百科的定义）

    倒排索引（英语：Inverted index），也常被称为反向索引、置入档案或反向档案，是一种索引方法，被用来存储在全文搜索下某个单词在一个文档或者一组文档中的存储位置的映射。它是文档检索系统中最常用的数据结构。    
    有两种不同的反向索引形式：    
    一条记录的水平反向索引（或者反向档案索引）包含每个引用单词的文档的列表。
    一个单词的水平反向索引（或者完全反向索引）又包含每个单词在一个文档中的位置。

## 2、作业要求

```
倒排索引(Inverted index)，也被称为反向索引。它是文档检索系统中最常用的数据结构。被广泛 地应用于全文搜索引擎。

例子如下，被索引的文件为(0，1，2代表文件名)

0. "it is what it is"
1. "what is it"
2. "it is a banana" 

我们就能得到下面的反向文件索引: 

"a": {2}
"banana": {2}
"is": {0, 1, 2}
"it": {0, 1, 2}
"what": {0, 1} 

再加上词频为:
"a": {(2,1)}
"banana": {(2,1)}
"is": {(0,2), (1,1), (2,1)} 
"it": {(0,2), (1,1), (2,1)}
"what": {(0,1), (1,1)}
```

## 3、运行结果

代码类为：com.dd.spark.invertedindex.InvertedIndex
在Idea本地测试运行结果
不加词频的运行结果
```
(a,CompactBuffer(2))
(it,CompactBuffer(1, 0, 2))
(banana,CompactBuffer(2))
(what,CompactBuffer(0, 1))
(is,CompactBuffer(1, 2, 0))
```
加上词频的运行结果

```
(a,CompactBuffer((2,1)))
(it,CompactBuffer((1,1), (0,2), (2,1)))
(banana,CompactBuffer((2,1)))
(is,CompactBuffer((1,1), (2,1), (0,2)))
(what,CompactBuffer((0,1), (1,1)))
```

用spark-submit提交执行
spark-submit --master  spark://localhost:7077 --class com.dd.spark.invertedindex.InvertedIndex --executor-memory 1g  com.geekbang.bd.chenjd.spark-1.0-SNAPSHOT.jar  data
执行结果同ide运行结果


# 二、作业2

## 1、作业要求

作业二 Distcp的spark实现

使用Spark实现Hadoop 分布式数据传输工具 DistCp (distributed copy)，只要求实现最基础的copy功 能，对于-update、-diff、-p不做要求

对于HadoopDistCp的功能与实现，可以参考

https://hadoop.apache.org/docs/current/hadoop-distcp/DistCp.html https://github.com/apache/hadoop/tree/release-2.7.1/hadoop-tools/hadoop-distcp Hadoop使用MapReduce框架来实现分布式copy，在Spark中应使用RDD来实现分布式copy 应实现的功能为:

sparkDistCp hdfs://xxx/source hdfs://xxx/target

得到的结果为，启动多个task/executor，将hdfs://xxx/source目录复制到hdfs://xxx/target，得到 hdfs://xxx/target/source

需要支持source下存在多级子目录
 需支持-i Ignore failures 参数
 需支持-m max concurrence参数，控制同时copy的最大并发task数

## 2、运行结果
代码为：com.dd.spark.distcp.SparkDistCP
实现递归复制文件目录
