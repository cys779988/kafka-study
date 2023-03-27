package com.example.demo.kafka;

import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@KafkaListener(id = "multiGroup", topics = "multitype")
public class MultiTypeKafkaListner {

    @KafkaHandler
    public void handleCorp(CorpMessage corp) {
        System.out.println("Corp received: " + corp);
    }

    @KafkaHandler
    public void handleBranch(BranchMessage branch) {
        System.out.println("Branch received: " + branch);
    }

    @KafkaHandler(isDefault = true)
    public void unknown(Object object) {
        System.out.println("Unknown type received: " + object);
    }
}
