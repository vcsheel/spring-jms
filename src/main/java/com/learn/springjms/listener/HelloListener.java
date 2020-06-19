package com.learn.springjms.listener;

import com.learn.springjms.config.JmsConfig;
import com.learn.springjms.model.HelloWorldMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import javax.jms.Message;

@Component
@Slf4j
public class HelloListener {

    @JmsListener(destination = JmsConfig.MY_QUEUE)
    public void listen(@Payload HelloWorldMessage helloWorldMessage,
                       @Headers MessageHeaders messageHeaders, Message message) {

        log.info("Message received");

        log.info(String.valueOf(helloWorldMessage));
    }
}
