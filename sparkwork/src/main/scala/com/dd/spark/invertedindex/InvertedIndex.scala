package com.dd.spark.invertedindex

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 实现多个文件中的倒排索引，并且计算每个单词的在各文件中的词频
  */
object InvertedIndex {

  def main(args: Array[String]): Unit = {

    //初始化sc环境变量
    val conf =
      new SparkConf().setAppName("Inverted Index").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //读取文件，从文件目录中获取数据，目录可以从参数传进，本地测试先写固定目录
    //用sc.wholeTextFiles得到的全路径文件名+文件内容
    var inputPath = args(0)
    val files = sc.wholeTextFiles("data")

    val file_name_length = files.map(x => x._1.split("/").length).collect()(0)
    val file_name_context =
      files.map(x => (x._1.split("/")(file_name_length - 1), x._2)).sortByKey()

    //将文件内容分词，然后赋值每个词及其所在文件名
    val words = file_name_context.flatMap(f = x => {
      val rdd1 = x._2.split(" ")
      val rdd2 = rdd1.map(word => (word, x._1))
      rdd2
    })

    //不计算词频的倒排索引
    val invertedIndexRDD = words.mapValues(x=>x)
      .distinct()
      .groupByKey()
      .sortByKey()
    invertedIndexRDD.foreach(println)

    //就算词频的倒排索引
    val wordCountIndexedRDD = words.map((_,1))
      .reduceByKey(_+_)
      .map(x=>(x._1._1,(x._1._2,x._2)))
      .groupByKey()
      .sortByKey()
    wordCountIndexedRDD.foreach(println)

    //释放资源
    sc.stop()
  }
}
