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
      //本机运行,local[2]
      .setMaster("local[2]")
      //设置jar
//      .setJars(Seq())
      //设置spark home
//      .setSparkHome("/home/tony/spark/")
    //配置线程池
    //.setExecutorEnv()
    val sc = new SparkContext(conf)
    val text = sc.textFile("hdfs://localhost:9000/test1/test.txt")
    val result = text.flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _).collect()
    //查询交易记录文件
    //将交易记录文件
//    用户交易消费信息挖掘
    //用户浏览商品类型  用户浏览次数 商品的类型 top5
    //推荐商品
    result.foreach(println)
  }
}
