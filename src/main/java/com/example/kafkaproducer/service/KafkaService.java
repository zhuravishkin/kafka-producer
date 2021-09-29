package com.example.kafkaproducer.service;

import com.example.kafkaproducer.model.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import javax.annotation.PostConstruct;

@PropertySource("classpath:environment.yaml")
@Slf4j
@Service
public class KafkaService {
    private final KafkaTemplate<String, User> kafkaTemplate;
    private final String outputTopic;

    public KafkaService(KafkaTemplate<String, User> kafkaTemplate,
                        @Value("${out}") String outputTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.outputTopic = outputTopic;
    }

    @PostConstruct
    public void sendMessage() {
        ListenableFuture<SendResult<String, User>> listenableFuture = kafkaTemplate.send(
                outputTopic,
                "john_wick",
                new User("message", "John", "Wick", 100, System.currentTimeMillis()));

        listenableFuture.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onSuccess(SendResult<String, User> stringUserSendResult) {
                log.info("Sent message = [" + "message" + "] with offset = [" +
                        stringUserSendResult.getRecordMetadata().offset() + "]");
            }

            @Override
            public void onFailure(Throwable throwable) {
                log.info("Unable to send message=[" + "message" + "] due to : " + throwable.getMessage());
            }
        });
    }
}
