package com.example.kafkaproducer.controller;

import com.example.kafkaproducer.model.PublishingStatus;
import com.example.kafkaproducer.model.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("kafka")
public class Controller {
    private KafkaTemplate<String, User> kafkaTemplate;
    private static final String TOPIC = "test";

    public Controller(KafkaTemplate<String, User> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping(path = "/publish/{message}")
    public ResponseEntity<PublishingStatus> getEmail(@PathVariable("message") String message) {
        ListenableFuture<SendResult<String, User>> listenableFuture = kafkaTemplate
                .send(TOPIC, new User(message, "John", 33));
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<String, User>>() {
            @Override
            public void onSuccess(SendResult<String, User> stringUserSendResult) {
                log.info("Sent message = [" + message + "] with offset = [" +
                        stringUserSendResult.getRecordMetadata().offset() + "]");
            }

            @Override
            public void onFailure(Throwable throwable) {
                log.info("Unable to send message=[" + message + "] due to : " + throwable.getMessage());
            }

//            @Override
//            public void onSuccess(SendResult<String, String> stringStringSendResult) {
//                log.info("Sent message = [" + message + "] with offset = [" +
//                        stringStringSendResult.getRecordMetadata().offset() + "]");
//            }
        });
        return new ResponseEntity<>(new PublishingStatus(), HttpStatus.OK);
    }
}
