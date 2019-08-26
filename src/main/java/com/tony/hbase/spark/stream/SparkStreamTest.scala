package com.tony.hbase.spark.stream

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreamTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("spark Stream")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    var lines = sc.textFile("D:\\hello.txt");
    val sqlContext = new SQLContext(sc);
    //rdd转化dataframe,使用关键字toDF
    import sqlContext.implicits._
    val df = lines.map { x =>
      val data = x.split(",")
      (data(0), data(1), data(2), data(3))
    }.toDF("id", "name", "type", "intro")
    df.show(2);
    df.printSchema()
    df.select("name").show()
    df.filter(df("id") > 13)
    df.groupBy("name").count()
    val rdd = df.rdd
    println(rdd.collect().mkString(","))
    rdd.foreach(println)
    val ds = rdd.map { x =>
      (x.get(0).toString, x.get(1).toString, x.get(2).toString, x.get(3).toString)
    }.toDS()
    ds.show()
    val df1 = ds.toDF("id")
    df1.show()
    val dftods = df1.as("test")
    dftods.show()


  }
}
