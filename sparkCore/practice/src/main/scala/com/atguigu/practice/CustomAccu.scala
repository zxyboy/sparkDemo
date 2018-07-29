package com.atguigu.practice

import java.util

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

class CustomAccu extends AccumulatorV2[String,java.util.Set[String]]{

  private val _logArray:java.util.Set[String] = new util.HashSet[String]()

  //分区中的暂存变量是否为空
  override def isZero: Boolean = {
    _logArray.isEmpty
  }

  //复制一个对象
  override def copy(): AccumulatorV2[String, util.Set[String]] = {
    val newAcc = new CustomAccu()
    _logArray.synchronized{
      newAcc._logArray.addAll(_logArray)
    }
    newAcc
  }

  //重启你的对象状态
  override def reset(): Unit = {
    _logArray.clear()
  }

  //在分区内增加数据
  override def add(v: String): Unit = {
    _logArray.add(v)
  }

  //将多个分区的累加器相加
  override def merge(other: AccumulatorV2[String, util.Set[String]]): Unit = {
    other match {
      case o:CustomAccu => _logArray.addAll(o.value)
    }
  }

  //读取最终的值
  override def value: util.Set[String] = {
    _logArray
  }
}

object LogAccu{

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("accu").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //创建一个累加器实例
    val accu = new CustomAccu
    //注册实例
    sc.register(accu,"accu")

    val rdd = sc.makeRDD(Array("1","2","3a","4a","5a","6","7"))

    rdd.filter{ line =>

      if(line.endsWith("a")){
        accu.add(line)
      }

      true
    }.collect()

    accu.value

    sc.stop()
  }

}
