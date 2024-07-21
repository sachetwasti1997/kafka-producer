package com.microservices.demo.kafka.producer.service.impl;

import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import com.microservices.demo.kafka.producer.service.KafkaProducer;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class TwitterKafkaProducer implements KafkaProducer<Long, TwitterAvroModel> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterKafkaProducer.class);

    private final KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate;

    public TwitterKafkaProducer(KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PreDestroy
    public void close() {
        if (kafkaTemplate != null) {
            LOGGER.info("Destroying the kafkaTemplate!");
            kafkaTemplate.destroy();
        }
    }

    @Override
    public void send(String topicName, Long key, TwitterAvroModel message) {
        LOGGER.info("Sending message: {} to the topic: {}", message, topicName);
        CompletableFuture<SendResult<Long, TwitterAvroModel>> completableFuture
                = kafkaTemplate.send(topicName, key, message);
        completableFuture.whenComplete((res, err) -> {
            if (null != res) {
                RecordMetadata recordMetadata = res.getRecordMetadata();
                LOGGER.debug("Received new Metadata. Topic: {}, Partition: {}, Offset: {}, TimeStamp: {}, at Time: {}",
                        recordMetadata.topic(),
                        recordMetadata.partition(),
                        recordMetadata.offset(),
                        recordMetadata.timestamp(),
                        System.currentTimeMillis()
                );
            }else {
                LOGGER.error("Received error while sending the message: {}, to Topic: {}, {}",
                        message,
                        topicName,
                        err.getMessage()
                );
            }
        });
    }

    private static void addCallBack(String topicName, TwitterAvroModel message, CompletableFuture<SendResult<Long, TwitterAvroModel>> completableFuture) {

    }
}
