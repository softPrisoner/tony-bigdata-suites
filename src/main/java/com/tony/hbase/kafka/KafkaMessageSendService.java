package com.tony.hbase.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

/**
 * @author tony
 * @describe KafkaMessageSendService
 * @date 2019-08-04
 */
@Component
@EnableAsync
public class KafkaMessageSendService {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMessageSendService.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.app.topic.test}")
    private String topic;

    @Async //开启异步消息发送,防止网络抖动导致信息丢失造成阻塞
    public void sendMessage(String message) {
        LOGGER.info("topic:{}  message:{}", topic, message);
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, message);
        future.addCallback(success -> LOGGER.info("send message success data:{}"
                , success != null ? success.getRecordMetadata() : "")
                , fail -> LOGGER.error("send message failed caused by:{}"
                        , fail.getMessage()));
    }
}
