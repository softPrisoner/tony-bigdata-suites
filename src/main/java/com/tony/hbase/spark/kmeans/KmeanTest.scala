package com.tony.hbase.spark.kmeans

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

case class Record(
                   id: String,
                   companyName: String,
                   direction: String,
                   productInfo: String
                 )

//机器学习最主要的是性能的调优，及一些参数的设置。
object KmeansTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("kmean test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val records = sc.textFile("D://test.txt")
      .map { x =>
        val data = x.split(",")
        Record(data(0), data(1), data(2), data(3))
      }.toDF().cache()

    val wordData = new Tokenizer()
      .setInputCol("productInfo")
      .setOutputCol("productWords")
      .transform(records)
    val tfData = new HashingTF()
      .setNumFeatures(20)
      .setInputCol("productWords")
      .setOutputCol("productFeatures")
      .transform(wordData)
    val idfModel = new IDF()
      .setInputCol("productFeatures")
      .setOutputCol("features")
      .fit(tfData)
    val idfData = idfModel.transform(tfData)
    val trainingData = idfData.select("id", "companyName", "features")
    val kmeans = new KMeans()
      .setK(10).setMaxIter(5).setFeaturesCol("features").setPredictionCol("prediction")
    val kmeansModel = kmeans.fit(trainingData)
    val kmeansData = kmeansModel.transform(trainingData).cache()
    kmeansData.show()
  }
}
