package com.tony.hbase.spark.tfidf

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 这里当做一个文档进行处理
  */
case class Record(
                   id: String,
                   componentName: String,
                   direction: String,
                   productinfo: String
                 )

object TFIDFTest2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("tf-idf-2")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc);
    import sqlContext.implicits._
    val records = sc.textFile("D://test.txt").map { x =>
      val data = x.split(",")
      Record(data(0), data(1), data(2), data(3))
    }.toDF().cache()
    val wordsData = new Tokenizer().setInputCol("productinfo").setOutputCol("productwords").transform(records)
    val tfData = new HashingTF()
      .setInputCol("productwords").setOutputCol("productFeatures").setNumFeatures(20).transform(wordsData)
    val idfModel = new IDF().setInputCol("productFeatures").setOutputCol("features").fit(tfData)
    val idfData = idfModel.transform(tfData)
    idfData.select("id", "componentName", "features").show()
  }
}
