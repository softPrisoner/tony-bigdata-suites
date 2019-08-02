package com.tony.hbase.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author tony
  * @describe SparkTest
  * @date 2019-08-02
  */
object SparkTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("spark-test")
      .set("spark.cores.max", "2")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val text = sc.textFile("/home/tony/spark/test.txt")
    val result = text.flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _).collect()
    result.foreach(println)
  }

}
