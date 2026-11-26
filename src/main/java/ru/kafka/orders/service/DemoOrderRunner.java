package ru.kafka.orders.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.support.SendResult;
import ru.kafka.orders.model.OrderEvent;

import java.math.BigDecimal;
import java.util.UUID;

@Configuration
@Profile("demo")
public class DemoOrderRunner {

    private static final Logger log = LoggerFactory.getLogger(DemoOrderRunner.class);

    @Bean
    ApplicationRunner publishDemoOrders(OrderProducer producer) {
        return args -> {
            producer.send(new OrderEvent("demo-valid-" + UUID.randomUUID(), new BigDecimal("125.50")))
                    .whenComplete((result, error) -> logSendResult("valid", result, error));
            producer.send(new OrderEvent("demo-invalid-" + UUID.randomUUID(), BigDecimal.ZERO))
                    .whenComplete((result, error) -> logSendResult("invalid", result, error));
        };
    }

    private void logSendResult(String scenario, SendResult<Object, Object> result, Throwable error) {
        if (error != null) {
            log.error("Failed to publish {} demo order", scenario, error);
        } else {
            log.info("Published {} demo order to topic {} at offset {}", scenario,
                    result.getRecordMetadata().topic(), result.getRecordMetadata().offset());
        }
    }
}
