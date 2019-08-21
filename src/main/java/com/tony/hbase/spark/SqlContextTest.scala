package com.tony.hbase.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext, sql}

/**
  * @author tony
  * @describe SqlContextTest
  * @date 2019-08-20
  */
object SqlContextTest {
  val url = "jdbc:mysql://localhost:3306/test?user=root&password=123456&useSSL=false"

  def transfer(sqlContext: SQLContext): Unit = {
    Class.forName("com.mysql.jdbc.Driver")
    val df = sqlContext
      .read
      .format("jdbc")
      .option("url", url)
      .option("dbtable", "test")
      .load()
    df.printSchema()
    df.filter("age>19")
    df.show()
    val countsByAge = df
      .groupBy("age")
      .count()
    println(countsByAge)
    //    countsByAge.write.format("json").save("file:/home/tony/sql")
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("sqltest")
    val sqlContext = new sql.SparkSession.Builder()
      .config(conf)
      .getOrCreate()
      .sqlContext
    transfer(sqlContext)
    //    conf.setSparkHome()
    //    conf.setJars()
    //    conf.setExecutorEnv()

  }

}
