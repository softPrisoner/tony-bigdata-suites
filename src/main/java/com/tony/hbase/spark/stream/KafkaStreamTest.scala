package com.tony.hbase.spark.stream

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaStreamTest {
  def main(args: Array[String]): Unit = {
    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val conf = new SparkConf().setAppName("kafka stream").setMaster("local[2]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds.apply(2));
    val topics = Array("streaming")
    //    val stream = KafkaUtils.createDirectStream[String, String](
    //      ssc,
    //      PreferConsistent,
    //      Subscribe[String, String](topics, kafkaParams)
    //    )
    //    stream.map(record => (record.key, record.value))


  }
}
