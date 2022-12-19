package com.lizza.spring.kafka.consumer.controller;

import com.lizza.spring.kafka.consumer.config.KafkaProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

import static com.lizza.spring.kafka.consumer.util.Constants.TOPIC_QUICK_START;

@RestController
@RequestMapping
public class BackdoorController {

    @Value("${spring.application.name}")
    private String name;

    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;

    @GetMapping("hello")
    public String hello() {
        return "hello, " + name;
    }

    @GetMapping("sendMessage")
    public void sendMessage(String message) throws Exception {
        kafkaTemplate.send(TOPIC_QUICK_START, message).get();
    }
}
