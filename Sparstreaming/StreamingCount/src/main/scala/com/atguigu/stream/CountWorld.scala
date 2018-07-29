package com.atguigu.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object CountWorld {
  def main(args: Array[String]): Unit = {

    //创建SparkConf对象
    val conf = new SparkConf().setAppName("Streaming").setMaster("local[*]")

    //创建StreamingContext对象
    val ssc = new StreamingContext(conf, Seconds(5))

    //创建一个接收器来接受数据 DStream[String]
    val linesDSteam = ssc.socketTextStream("master01",9999)

    //flatMap转换成为单词 DStream[String]
    val wordsDStream = linesDSteam.flatMap(_.split(" "))

    //将单词转换为KV结构 DStream[(String,1)]
    val kvDStream = wordsDStream.map((_,1))

    //将相同单词个数进行合并
    val result = kvDStream.reduceByKey(_+_)

    result.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
