package com.tony.hbase.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}
/**
  * @author tony
  * @describe Rdd2DF
  * @date 2019-08-20
  */
object RDDTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf();
    conf.setAppName("rddTest");
    conf.setMaster("local[2]");
    val sc = new SparkContext(conf)
    /**
      * 使用rdd创建rdd,可以通过List或者Array来创建
      */
    val rdd0 = sc.makeRDD(List(1, 2, 3, 4, 6))
    val rdd1 = sc.makeRDD(Array(1, 2, 3, 4, 5, 7))
    val rdd2 = rdd0.map { x => x * x }
    println(rdd2.collect().mkString(","))

    /**
      * 使用parallelize创建rdd
      */
    val rdd3 = sc.parallelize(List(3, 5, 7), 1)
    val rdd4 = rdd3.map(x => x + x)
    println(rdd4.collect().mkString("-"));
    /**
      * rdd本质是一个数组，因此可以使用List或者Array创建
      */
    var lines = sc.textFile("D:\\hello.txt");
    val read = lines.flatMap { x => x.split(",") }
    println(read.collect().mkString("--"))


  }
}
