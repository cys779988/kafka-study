package com.example.demo.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaConsumer {

    @KafkaListener(
            topics = "standby",
            containerFactory = "fooKafkaListenerContainerFactory")
    public void listenWithHeaders(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        System.out.println("Received Message: " + message + " from partition: " + partition);
    }

    @KafkaListener(
            topics = "standby",
            containerFactory = "barKafkaListenerContainerFactory")
    public void listenWithFilter(String message) {
        System.out.println("Received Message in filtered listener: " + message);
    }

    @KafkaListener(
            topics = "standby",
            containerFactory = "corpKafkaListenerContainerFactory")
    public void corpMessagelistener(CorpMessage message) {
        System.out.println("corp name : " + message.getName() + ", corp message : " + message.getMsg());
    }
}
