package com.example.kafkaproducer.service;

import com.example.kafkaproducer.model.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Slf4j
@Service
public class KafkaService {
    private final KafkaTemplate<String, User> kafkaTemplate;
    private static final String TOPIC = "src-topic";

    public KafkaService(KafkaTemplate<String, User> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String message) {
        ListenableFuture<SendResult<String, User>> listenableFuture = kafkaTemplate.send(TOPIC,
                new User(message, "John", "Wick", 100, System.currentTimeMillis()));

        listenableFuture.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onSuccess(SendResult<String, User> stringUserSendResult) {
                log.info("Sent message = [" + message + "] with offset = [" +
                        stringUserSendResult.getRecordMetadata().offset() + "]");
            }

            @Override
            public void onFailure(Throwable throwable) {
                log.info("Unable to send message=[" + message + "] due to : " + throwable.getMessage());
            }
        });
    }
}
