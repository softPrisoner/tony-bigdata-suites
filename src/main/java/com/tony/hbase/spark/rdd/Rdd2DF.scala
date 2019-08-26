package com.tony.hbase.spark.rdd

import org.apache.spark.sql.SparkSession

/**
  * @author tony
  * @describe Rdd2DF
  * @date 2019-08-20
  */
object Rdd2DF {
  def rdd2DF(session: SparkSession, sourcePath: String): Unit = {
    val rdd = session.sparkContext.textFile(sourcePath)
    import session.implicits._
    val df = rdd.toDF("line")
  }
}
