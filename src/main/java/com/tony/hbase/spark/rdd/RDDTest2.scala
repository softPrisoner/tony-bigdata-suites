package com.tony.hbase.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author tony
  * @describe Rdd2DF
  * @date 2019-08-20
  */
object RDDTest2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf();
    conf.setAppName("rddTest");
    conf.setMaster("local[2]");
    val sc = new SparkContext(conf)
    var rddInt = sc.makeRDD(Array(1, 2, 3, 4, 5, 3))
    var rddInt1 = sc.makeRDD(Array(2, 3, 4))
    var rddStr = sc.makeRDD(List("aa,aa", "b,bb", "ccc", "ddd"))
    println("map->" + rddInt.map(x => x + 2).collect().mkString("_"))
    println("fliter->" + rddInt.filter(x => x > 3).collect().mkString(","))
    println("flatMap->" + rddStr.flatMap { x => x.split(",") }.collect().mkString("--"))
    println("distanct->" + rddInt.distinct().collect().mkString(","));

    println("union->" + rddInt.union(rddInt1).collect().mkString("__"))
    //insection(){}结果无序
    println("insection->" + rddInt.intersection(rddInt1).collect().mkString("-"))
    //subtract  差集union
    println("subtract->" + rddInt.subtract(rddInt1).collect().mkString(","))
    //action,统计总数
    println("count->" + rddInt.count())
    //countByValue是对集合元素进行分别统计
    println("countByValue->" + rddInt.countByValue())
    //reduce 将前两个元素传入输入函数，并返回结果值，新产生的值与rdd下一个元素，组成两个元素，在传输给
    //输入函数
    val res = rddInt.reduce((x, y) => {
      x + y
    })
    println("reduce->" + res)
    //reduceByKey对元素的key/value中key/value相同key的元素进行操作。
    val kvrdd = sc.parallelize(List((1, 3), (1, 5), (2, 5)))
    val res2 = kvrdd.reduceByKey((x, y) => x + y).collect().mkString(" ")
    println("reduceByKey" + res2)
  }

}
