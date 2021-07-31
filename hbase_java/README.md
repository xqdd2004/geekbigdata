# 一作业要求

作业：编程实践，使用 Java API 操作 HBase
主要实践建表、插入数据、删除数据、查询等功能。要求建立一个如下所示的表：

- 表名：$your_name:student
  空白处自行填写, 姓名学号一律填写真实姓名和学号

<img src="https://static001.infoq.cn/resource/image/21/89/21ceb17dda135b92d718a5db94603689.png" alt="img" style="zoom:50%;" />

- 服务器版本为 2.1.0（hbase 版本和服务器上的版本可以不一致，但尽量保证一致）
  `<dependency>`
      `<groupId>org.apache.hbase</groupId>`
      `<artifactId>hbase-client</artifactId>`
      `<version>2.1.0</version>`
  `</dependency>`
- 作业提交链接： https://jinshuju.net/f/rWuZQM
  作业截止日期：8 月 1 日 23:59 前

# 二实现过程

## 1、安装Hbase

本地安装一个三台机器的Hbase集群分布式环境

1）安装zookeeper3.7.0的集群环境，3台服务器的分布式环境，启动

2）安装Hbase2.4版本，3台服务器的分布式环境

3）利用前面课程安装的hadoop机器环境

## 2、编写代码

1）代码包括HBaseConnectionFactory工厂类、HBaseUtil工具类和调用类

2）实现简单的创建表格、删除表、添加数据、删除数据、修改数据、查询数据等基本功能



