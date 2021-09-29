package com.example.kafkaproducer.service;

import com.example.kafkaproducer.model.User;
import com.github.javafaker.Faker;
import com.google.common.util.concurrent.RateLimiter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import javax.annotation.PostConstruct;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

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
        RateLimiter rateLimiter = RateLimiter.create(1.0);

        Faker faker = new Faker();

        Map<String, User> map = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            String phoneNumber = faker.phoneNumber().subscriberNumber(10);
            map.put(phoneNumber, User.builder()
                    .phoneNumber(phoneNumber)
                    .firstName(faker.name().firstName())
                    .surName(faker.name().lastName())
                    .balance(new Random().nextInt())
                    .eventTime(Instant.now().toEpochMilli())
                    .build());
        }

        map.forEach((s, user) -> {
            rateLimiter.acquire();

            ListenableFuture<SendResult<String, User>> listenableFuture = kafkaTemplate.send(
                    outputTopic,
                    s,
                    user);

            listenableFuture.addCallback(new ListenableFutureCallback<>() {
                @Override
                public void onSuccess(SendResult<String, User> stringUserSendResult) {
                    log.info("Sent message = [" + "message" + "] with offset = [" +
                            stringUserSendResult.getRecordMetadata().offset() + "]");
                }

                @Override
                public void onFailure(@NonNull Throwable throwable) {
                    log.info("Unable to send message=[" + "message" + "] due to : " + throwable.getMessage());
                }
            });
        });
    }
}
