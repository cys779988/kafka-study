package com.example.demo.kafka;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

@RestController
@RequiredArgsConstructor
public class KafkaController {
    @Resource(name = "kafkaTemplate")
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Resource(name = "corpKafkaTemplate")
    private final KafkaTemplate<String, CorpMessage> corpKafkaTemplate;

    @Resource(name = "multiTypeKafkaTemplate")
    private final KafkaTemplate<String, Object> multiTypeKafkaTemplate;

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

    @GetMapping("/corp/{name}/{msg}")
    public void sendCorpMessage(@PathVariable String name, @PathVariable String msg) {
        ListenableFuture<SendResult<String, CorpMessage>> future = corpKafkaTemplate.send("corp", new CorpMessage(name, msg));
        future.completable().whenComplete((result, ex) -> {
        });
    }

    @GetMapping("/multi/{name}/{msg}")
    public void sendMultiTypeMessage(@PathVariable String name, @PathVariable String msg) {
        multiTypeKafkaTemplate.send("multitype", new CorpMessage(name, msg));
        multiTypeKafkaTemplate.send("multitype", new BranchMessage(name, msg));
    }
}
