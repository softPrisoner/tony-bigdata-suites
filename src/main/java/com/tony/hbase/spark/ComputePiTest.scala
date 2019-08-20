package com.tony.hbase.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author tony
  * @describe ComputePITest
  * @date 2019-08-20
  */
object ComputePiTest {
  val NUMBER_SAMPLE = 1000000000

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("pi compute")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val count = sc.parallelize(1 to NUMBER_SAMPLE).filter { _ =>
      val x = math.random
      val y = math.random
      x * x + y * y < 1
    }.count()
    println(count)
    println("Pi value is roughly is" + 4.0 * count / NUMBER_SAMPLE)
  }
}
