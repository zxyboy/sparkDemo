package com.atguigu.practice

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Accu {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("partittoner").setMaster("local")
    val sc = new SparkContext(conf)

    var sum = new LongAccumulator()
    //var sum = 0
    sc.register(sum,"累加器")
    val rdd = sc.makeRDD(Array(1,2,3,4,5))

    rdd.map{ x =>
      sum.add(x)

    }.collect()


    println(sum.value)

    sc.stop()
  }

}
