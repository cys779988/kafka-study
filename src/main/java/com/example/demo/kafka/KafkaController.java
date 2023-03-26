package com.example.demo.kafka;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class KafkaController {
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value(value = "${spring.kafka.template.default-topic}")
    private String topic;

    @GetMapping("/{msg}")
    public void send(@PathVariable String msg) {
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, msg);
        future.completable().whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("Sent message=[" + msg + "] with offset=[" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to send message=[" + msg + "] due to : " + ex.getMessage());
            }
        });
    }

    @PostMapping
    public String receive() {
        ConsumerRecord<String, String> result = kafkaTemplate.receive("standby", 1, (short) 1);
        return result.value();
    }
}
