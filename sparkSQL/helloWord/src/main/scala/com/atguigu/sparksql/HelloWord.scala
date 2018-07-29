package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object HelloWord {

  def main(args: Array[String]): Unit = {

    val conf  = new SparkConf().setAppName("sql").setMaster("local[*]")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val sc = spark.sparkContext

    val df = spark.read.json("C:\\Users\\wuyufei\\Desktop\\spark\\2.code\\spark\\sparkSql\\doc\\employees.json")

    //注意，导入隐式转换，
    import spark.implicits._

    //展示整个表
    df.show()

    //展示整个表的Scheam
    df.printSchema()

    //DSL风格查询
    df.filter($"salary" > 3300).show

    //SQL风格

    //注册一个表名
    df.createOrReplaceTempView("employee")

    //查询
    spark.sql("select * from employee where salary > 3300").show()

    spark.close()
  }

}
