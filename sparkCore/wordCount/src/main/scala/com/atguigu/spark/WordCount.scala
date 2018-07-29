package com.atguigu.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    //新建sparkconf对象
    val  conf = new SparkConf().setAppName("wordcount").setMaster("local[*]")

    //创建spark'Context
    val sc = new SparkContext(conf)

    //读取数据
    val textfile = sc.textFile("./wordcount")

    //按空格切分
    val words = textfile.flatMap(_.split(","))

    //转换为kv结构
    val k2v = words.map((_,1))

    //将相同key的合并
    val result = k2v.reduceByKey(_+_)


    val value = result.sortBy( _._2, false).take(3)

    //输出结果
    value.foreach(println _)

    //Array(1,2,3).toList.sortWith()

    //关闭连接

    sc.stop()
  }

}
