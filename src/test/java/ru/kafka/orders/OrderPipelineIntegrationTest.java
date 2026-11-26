package ru.kafka.orders;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import ru.kafka.orders.model.OrderEvent;
import ru.kafka.orders.model.OrderResult;
import ru.kafka.orders.model.OrderStatus;
import ru.kafka.orders.service.OrderProducer;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(SpringExtension.class)
@SpringBootTest
@Testcontainers
class OrderPipelineIntegrationTest {

    private static final String PROCESSED_TOPIC = "orders.processed";

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("apache/kafka:3.9.1"));

    @Autowired
    private OrderProducer orderProducer;

    @DynamicPropertySource
    static void configureKafka(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    static Stream<Arguments> orderScenarios() {
        return Stream.of(
                Arguments.of("positive amount is accepted", new BigDecimal("10.50"), OrderStatus.ACCEPTED),
                Arguments.of("zero amount is rejected", BigDecimal.ZERO, OrderStatus.REJECTED),
                Arguments.of("negative amount is rejected", new BigDecimal("-1.00"), OrderStatus.REJECTED)
        );
    }

    @ParameterizedTest(name = "{0}: amount={1}, expected={2}")
    @MethodSource("orderScenarios")
    void shouldPublishExpectedOrderResult(String scenario, BigDecimal amount, OrderStatus expectedStatus)
            throws Exception {
        // У каждого параллельного сценария свой ID заказа и своя группа consumer-а.
        String orderId = "test-" + UUID.randomUUID();
        String groupId = "order-result-test-" + UUID.randomUUID();
        Map<String, Object> consumerProperties = new java.util.HashMap<>();
        // Адрес брокера, который Testcontainers запустил специально для этого набора тестов.
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        // Kafka хранит состояние чтения отдельно для каждой группы consumer-ов.
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // Если группа новая и позиции чтения ещё нет, начинаем с начала топика.
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Тест сам решает, когда закончить чтение; автосохранение позиции не требуется.
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        
        // Ключ читаем как строку, а JSON-значение сразу преобразуем в OrderResult.
        try (Consumer<String, OrderResult> resultConsumer = new KafkaConsumer<>(
                consumerProperties,
                new StringDeserializer(),
                new JsonDeserializer<>(OrderResult.class, false))) {
            // Подписываемся на топик с результатами.
            resultConsumer.subscribe(java.util.List.of(PROCESSED_TOPIC));
            // Kafka назначает раздел не сразу, поэтому ждем готовности consumer-а перед отправкой.
            // Общий лимит — 10 секунд; Awaitility проверяет условие каждые 100 мс по умолчанию.
            await().atMost(10, TimeUnit.SECONDS)
                    .until(() -> {
                        resultConsumer.poll(Duration.ofMillis(100));
                        return !resultConsumer.assignment().isEmpty();
                    });

            // Ждем подтверждение Kafka о записи события. результат обработки ниже.
            orderProducer.send(new OrderEvent(orderId, amount)).get(10, TimeUnit.SECONDS);

            // Ждем ответ именно для этого заказа: в топике могут быть результаты других сценариев.
            OrderResult[] receivedResult = new OrderResult[1];
            await().atMost(10, TimeUnit.SECONDS)
                    .pollInterval(Duration.ofMillis(100))
                    .until(() -> {
                        for (ConsumerRecord<String, OrderResult> record
                                : KafkaTestUtils.getRecords(resultConsumer, Duration.ofMillis(100))) {
                            if (orderId.equals(record.key())) {
                                receivedResult[0] = record.value();
                                return true;
                            }
                        }
                        return false;
                    });

            // Проверяем, что вернулся тот же заказ и что сумма получила ожидаемый статус.
            assertEquals(orderId, receivedResult[0].getOrderId(), "Result must match the submitted order");
            assertEquals(expectedStatus, receivedResult[0].getStatus(), "Unexpected validation status");
            System.out.printf("Scenario '%s' passed: orderId=%s amount=%s status=%s%n",
                    scenario, orderId, amount, receivedResult[0].getStatus());
        }
    }
}
