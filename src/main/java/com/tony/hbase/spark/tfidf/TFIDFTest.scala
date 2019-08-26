package com.tony.hbase.spark.tfidf

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object TFIDFTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("TF-IDF")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val sData = sqlContext.createDataFrame(Seq(
      (0, "Hi i ha  test"),
      (1, "i wish java could use case classes"),
      (2, "logistic regression models are neat")
    )).toDF("label", "sentence")
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordData = tokenizer.transform(sData)
    wordData.show()
    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
    val featuredData = hashingTF.transform(wordData)
    featuredData.show()
    val idf = new IDF()
      .setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featuredData)
    val rescaledData = idfModel.transform(featuredData)
    rescaledData.select("features", "label").take(3).foreach(println)

  }
}
