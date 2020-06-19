package com.learn.springjms.sender;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.springjms.config.JmsConfig;
import com.learn.springjms.model.HelloWorldMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import java.util.UUID;

@RequiredArgsConstructor
@Component
@Slf4j
public class HelloSender {

    private final JmsTemplate jmsTemplate;
    private final ObjectMapper objectMapper;

    @Scheduled(fixedRate = 2000)
    public void SendMessage() {

        // Uncomment logs to see response
        //log.info("Sending a new message ... ");

        HelloWorldMessage message = HelloWorldMessage
                .builder()
                .id(UUID.randomUUID())
                .message("Hello World!")
                .build();

        jmsTemplate.convertAndSend(JmsConfig.MY_QUEUE, message);

        //log.info("Message sent");
    }

    @Scheduled(fixedRate = 2000)
    public void SendAndRecieveMessage() throws JMSException {

        log.info("Sending a new message ... ");

        HelloWorldMessage message = HelloWorldMessage
                .builder()
                .id(UUID.randomUUID())
                .message("Hello")
                .build();

        Message receivedMsg = jmsTemplate.sendAndReceive(JmsConfig.SEND_RCV_QUEUE, new MessageCreator() {
            @Override
            public Message createMessage(Session session) throws JMSException {
                Message helloMsg = null;

                try {
                    helloMsg = session.createObjectMessage(message);
                    helloMsg.setStringProperty("_type", "com.learn.springjms.model.HelloWorldMessage");

                    log.info("Sending hello ... ");

                    return helloMsg;

                } catch (Exception e) {
                    throw new JMSException("crashed");
                }

            }
        });

        log.info("Received message: " + String.valueOf(receivedMsg.getBody(HelloWorldMessage.class).getMessage()));

    }
}
