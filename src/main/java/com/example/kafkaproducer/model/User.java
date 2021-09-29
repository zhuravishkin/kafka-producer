package com.example.kafkaproducer.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.ToString;

@ToString
@Builder
@AllArgsConstructor
public class User {
    @JsonProperty("phone_number")
    private final String phoneNumber;

    @JsonProperty("first_name")
    private final String firstName;

    @JsonProperty("sur_name")
    private final String surName;

    @JsonProperty("balance")
    private final Integer balance;

    @JsonProperty("event_time")
    private final Long eventTime;
}
