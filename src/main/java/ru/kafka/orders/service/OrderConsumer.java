package ru.kafka.orders.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import ru.kafka.orders.model.OrderEvent;
import ru.kafka.orders.model.OrderResult;
import ru.kafka.orders.model.OrderStatus;

import java.math.BigDecimal;
import java.util.concurrent.CompletableFuture;

@Component
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final String processedTopic;

    public OrderConsumer(
            KafkaTemplate<Object, Object> kafkaTemplate,
            @Value("${app.kafka.topics.processed}") String processedTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.processedTopic = processedTopic;
    }

    @KafkaListener(topics = "${app.kafka.topics.created}")
    public CompletableFuture<?> process(OrderEvent event) {
        OrderStatus status = event.getAmount() != null && event.getAmount().compareTo(BigDecimal.ZERO) > 0
                ? OrderStatus.ACCEPTED
                : OrderStatus.REJECTED;
        OrderResult result = new OrderResult(event.getOrderId(), status);

        try {
            return kafkaTemplate.send(processedTopic, event.getOrderId(), result)
                    .whenComplete((sendResult, error) -> {
                        if (error != null) {
                            log.error("Failed to publish result: orderId={}, status={}",
                                    event.getOrderId(), status, error);
                        } else {
                            log.info("Processed order: orderId={}, amount={}, status={}",
                                    event.getOrderId(), event.getAmount(), status);
                        }
                    });
        } catch (RuntimeException error) {
            // Return a failed future so Spring Kafka can handle the failure consistently.
            return CompletableFuture.failedFuture(error);
        }
    }
}
