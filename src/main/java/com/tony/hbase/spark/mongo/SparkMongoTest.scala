package com.tony.hbase.spark.mongo

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

case class Record(
                   id: String,
                   companyName: String,
                   state: String,
                   desc: String
                 )

object SparkMongoTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Spark Mongo")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //输入user,输出uri
      .set("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/test.person?readPreference=primaryPreferred")
      .set("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/test.person") //设置的是输出地址及集合
      .set("spark.mongodb.keep_alive_ms", "1000000")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val persons = MongoSpark.load(sqlContext)
    persons.take(10).foreach(println)
    persons.registerTempTable("person")
    val df = sqlContext.sql("select age,address from person").persist()
    df.take(10).foreach(println)
    MongoSpark.save(df)
  }
}
