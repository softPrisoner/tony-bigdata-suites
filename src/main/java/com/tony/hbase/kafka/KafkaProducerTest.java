package com.tony.hbase.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

/**
 * @author tony
 * @describe KafkaProducerTest
 * @date 2019-08-04
 */
@EnableScheduling
@Component
public class KafkaProducerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerTest.class);
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Value("${kafka.app.topic.test}")
    private String topic;

    @Scheduled(cron = "00/5 * * * * ?")
    public void send() {
        String message = "Hello World---" + System.currentTimeMillis();
        LOGGER.info("topic=" + topic + ",message=" + message);
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, message);
        future.addCallback(success -> LOGGER.info("KafkaMessageProducer 发送消息成功！"),
                fail -> LOGGER.error("KafkaMessageProducer 发送消息失败！"));
    }
}
