package com.atguigu.practice

import org.apache.spark.{SparkConf, SparkContext}

object PricticeMy {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()

        val sc = new SparkContext("local[*]", "test", conf)

        val fileRDD = sc.textFile("./agent.log")
        //需求：统计每一个省份点击TOP3的广告ID
//        fileRDD.map(x => {
//            val line = x.split(" ");
//            (line(1) + "_" + line(4), 1)
//        })
//            .reduceByKey(_ + _)
//            .map(x => {
//                val keys = x._1.split("_");
//                (keys(0), (x._2, keys(1)))
//            })
//            .groupByKey()
//            .mapValues(items => items.toList.sortWith(_._1 > _._1).take(3))
//            .collect().foreach(println)

        // 1516609143867 6 7 64 16

        // (6_16,1)

        //  (6_16,n)

        // (6,(n,16))

        // (6,[(n,16),(n,17),...])

        // 对上面数据的值排序取前三


        //需求：统计每一个省份每一个小时的TOP3广告的ID  最小粒度  pro_hour_ad

        // 格式化时间，取到小时

        // (pro_hour_adv,1)

        // (pro_hour_adv,n)

        // (pro_hour,(n,_adv))

        //(pro_hour,(n1,_adv1),(n2,_adv2),...))

        // 对value排序取前三


        sc.stop()
    }
}
