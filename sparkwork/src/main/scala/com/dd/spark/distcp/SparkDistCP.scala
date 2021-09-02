package com.dd.spark.distcp

import java.io.{FileSystem => _, _}

import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object SparkDistCP {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("spark://hadoop0:7077").setAppName("Spark DisctCP")
    val sc = new SparkContext(conf)
    val hdfs : FileSystem = FileSystem.get(sc.hadoopConfiguration)
    if (args.length < 3) {
      println("参数不对，SparkDistCp cp srcPath destPath")
      System.exit(-1)
    }

    args(0) match {
      case "cp" => copyFolder(hdfs, args(1), args(2))
    }

    sc.stop()
  }

  /**
   * 复制整个目录，递归复制文件
   * @param hdfs
   * @param sourceFolder
   * @param targetFolder
   */
  def copyFolder(hdfs : FileSystem, sourceFolder: String, targetFolder: String): Unit = {
    val holder : ListBuffer[String] = new ListBuffer[String]
    val children : List[String] = listChildren(hdfs, sourceFolder, holder).toList
    for(child <- children)
      copyFile(hdfs, child, child.replaceFirst(sourceFolder, targetFolder))
  }

  /**
   * 获取子目录文件列表
   * @param hdfs
   * @param fullName
   * @param holder
   * @return
   */
  def listChildren(hdfs : FileSystem, fullName : String, holder : ListBuffer[String]) : ListBuffer[String] = {

    val filesStatus = hdfs.listStatus(new Path(fullName), new MyPathFilter)
    for(status <- filesStatus){
      val filePath : Path = status.getPath
      if(isFile(hdfs,filePath))
        holder += filePath.toString
      else
        listChildren(hdfs, filePath.toString, holder)
    }
    holder
  }

  def isDir(hdfs : FileSystem, name : String) : Boolean = {
    hdfs.getFileStatus(new Path(name)).isDirectory()
  }
  def isDir(hdfs : FileSystem, name : Path) : Boolean = {
    hdfs.getFileStatus(name).isDirectory()
  }
  def isFile(hdfs : FileSystem, name : String) : Boolean = {
    hdfs.getFileStatus(new Path(name)).isFile()
  }
  def isFile(hdfs : FileSystem, name : Path) : Boolean = {
    hdfs.getFileStatus(name).isFile()
  }

  class MyPathFilter extends PathFilter {
    override def accept(path: Path): Boolean = true
  }

  /**
   * 复制文件
   * @param hdfs
   * @param source
   * @param target
   */
  def copyFile(hdfs : FileSystem, source: String, target: String): Unit = {

    val sourcePath = new Path(source)
    val targetPath = new Path(target)

    if(!exists(hdfs, targetPath))
      createFile(hdfs, targetPath)

    val inputStream : FSDataInputStream = hdfs.open(sourcePath)
    val outputStream : FSDataOutputStream = hdfs.create(targetPath)
    transport(inputStream, outputStream)
  }

  def exists(hdfs : FileSystem, name : String) : Boolean = {
    hdfs.exists(new Path(name))
  }

  def exists(hdfs : FileSystem, name : Path) : Boolean = {
    hdfs.exists(name)
  }

  def createFile(hdfs : FileSystem, name : String) : Boolean = {
    hdfs.createNewFile(new Path(name))
  }

  def createFile(hdfs : FileSystem, name : Path) : Boolean = {
    hdfs.createNewFile(name)
  }


  def transport(inputStream : InputStream, outputStream : OutputStream): Unit ={
    val buffer = new Array[Byte](64 * 1000)
    var len = inputStream.read(buffer)
    while (len != -1) {
      outputStream.write(buffer, 0, len - 1)
      len = inputStream.read(buffer)
    }
    outputStream.flush()
    inputStream.close()
    outputStream.close()
  }

}
