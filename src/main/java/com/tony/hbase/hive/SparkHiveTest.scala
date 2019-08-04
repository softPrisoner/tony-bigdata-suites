package com.tony.hbase.hive

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @author tony
  * @describe SparkHiveTest
  * @date 2019-08-04
  */
object SparkHiveTest {
  val warehouseLocation = "hdfs://localhost:9000/usr/local/hive/warehouse"

  def main(args: Array[String]): Unit = {
    val properties: Properties = new Properties()
    properties.put("user", "tony")
    //    properties.put("password", "wabslygzj")
    //    properties.put("driver", "com.mysql.cj.jdbc.Driver")
    Class.forName("com.mysql.cj.jdbc.Driver").newInstance()
    // 版本问题   Unable to instantiate SparkSession with Hive support because Hive classes are not found
    //https://stackoverflow.com/questions/53394691/unable-to-access-to-hive-warehouse-directory-with-spark
    val conf = new SparkConf()
      .setAppName("spark-hive")
      .setMaster("local[2]")
      .set("spark.sql.warehouse.dir", warehouseLocation)
      //http://www.luyixian.cn/news_show_23263.aspx
      .set("spark.sql.hive.verifyPartitionPath", "false")
    //https://blog.csdn.net/jisuanjiguoba/article/details/82585854
    val spark_session = SparkSession
      .builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
    //    spark_session.catalog.listDatabases().show(false)
    //    spark_session.catalog.listTables().show(false)
    //    spark_session.conf.getAll.mkString("\n")
    import spark_session.sql
    sql("use product_db")
    //从数据上看hive不符合数据库范式设计,新版本支持ACID
    val rdd = spark_session.sql("select * from product_db.account")
    val df = rdd.toDF("id", "name","age")
   val tt=df.groupBy("id").count()
    tt.show()
    //    val rdd = sparkSession.sql("select id,name,productId,productInfo from  product_db")


  }
}
