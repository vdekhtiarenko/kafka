package ru.kafka.orders.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import ru.kafka.orders.model.OrderEvent;

import java.util.concurrent.CompletableFuture;

@Service
public class OrderProducer {

    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final String createdTopic;

    public OrderProducer(
            KafkaTemplate<Object, Object> kafkaTemplate,
            @Value("${app.kafka.topics.created}") String createdTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.createdTopic = createdTopic;
    }

    public CompletableFuture<SendResult<Object, Object>> send(OrderEvent event) {
        return kafkaTemplate.send(createdTopic, event.getOrderId(), event);
    }
}
