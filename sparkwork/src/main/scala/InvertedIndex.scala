
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object InvertedIndex {

  def main(args: Array[String]): Unit = {
//    val sc = SparkSession.builder.master("local[*]").appName("Inverted Index").getOrCreate()
    val conf = new SparkConf().setAppName("Inverted Index2").setMaster("local[*]")
    val sc =new SparkContext(conf)

    val files =sc.wholeTextFiles("data")
    files.foreach(elem => {
          println(elem)
        })

    val file_name_length = files.map(x=>x._1.split("/").length).collect()(0)
    val file_name_context= files.map(x=>(x._1.split("/")(file_name_length-1),x._2)).sortByKey()

    val words =file_name_context.flatMap(x=>{
      //首先要根据行来切分
      val line =x._2.split("\n")
      //每个文件生成的（文件名，单词）对用链表给串起来。注意按照下面的方法生成的list是带有一个空节点的指针，也就是说它的第一元素是null
      val list =mutable.LinkedList[(String,String)]()
      //这里和C++有点像，temnp相当于是一个临时的指针，用来给list插入元素的。在scala语言中，val是不可变的，var是可变的
      var temp =list
      //对每一行而言，需要根据单词拆分，然后把每个单词组成（文件名，单词）对，链到list上
      for(i <- 0 to line.length-1){
        val word =line(i).split(" ").iterator
        while (word.hasNext){
          temp.next=mutable.LinkedList[(String,String)]((x._1,word.next()))
          temp=temp.next
        }
      }
      //我们得到的list的第一个元素是null，drop函数是去掉前n个数，这里是1，我们要把第一个元素null给去掉
      val list_end=list.drop(1)
      //这个list_end是这个flatMap算子中x所要得到的东西，scala语言居然可以这样写，我也是醉了
      list_end
    }).distinct()//需要去重
    //首先按照文件名排序，然后调换map的位置，将文件名串起来，再根据单词排序，最后保存
    words.sortByKey().map(x=>(x._2,x._1)).reduceByKey((a,b)=>a+";"+b).sortByKey().saveAsTextFile("index")

//    // 读取数据
//    val file: RDD[String] = sc.read.textFile("data").rdd

//
//    // 切分并压平
//    val words: RDD[String] = file.flatMap(_.split(" "))
//    // 组装
//    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))
//    // 分组聚合
//    val result: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)
//
//    // 排序 降序
//    // 第一种 -   第二种 ： 第二个参数
//    val finalRes: RDD[(String, Int)] = result.sortBy(_._2, false)
//    // 直接存储到hdfs中
////    finalRes.saveAsTextFile(output)
//    finalRes.foreach(elem => {
//            println(elem)
//          })
    // 释放资源
    sc.stop()


  }

}
