package com.tony.hbase.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * @author tony
 * @describe KafkaMessageReceiveService
 * @date 2019-08-04
 */
@Component
public class KafkaMessageReceiveService {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMessageReceiveService.class);

    @KafkaListener(topics ={"${kafka.app.topic.test}"})
    public void receive(@Payload String message, @Headers MessageHeaders headers) {
        LOGGER.info("kafka consumer receive message:{}", message);
        headers.keySet().forEach(key -> LOGGER.info("{}:{}", key, headers.get(key)));
    }
}
