# Kafka order processing demo

Демонстрационный Spring Boot сервис обрабатывает заказы через асинхронный Kafka-пайплайн:

```text
OrderProducer -> orders.created -> OrderConsumer (amount > 0) -> orders.processed
```

Положительная сумма получает статус `ACCEPTED`, нулевая, отрицательная или отсутствующая — `REJECTED`. Сообщения `OrderEvent` и `OrderResult` сериализуются в JSON через Jackson.

## Требования

- JDK 17 или новее
- Maven 3.9+
- Docker Desktop с запущенным Docker Engine

Для тестов отдельный Kafka не нужен: Testcontainers поднимает изолированный контейнер и завершает его после выполнения тестов.

## Запуск интеграционных тестов

```powershell
mvn test
```

Тесты отправляют события через Spring Kafka producer, читают результат из `orders.processed` и используют Awaitility для ожидания асинхронной обработки. Набор сценариев задаётся через `@MethodSource`:

| Сумма | Ожидаемый результат |
|---:|---|
| `10.50` | `ACCEPTED` |
| `0` | `REJECTED` |
| `-1.00` | `REJECTED` |

У каждого сценария свой ID заказа и consumer group, поэтому сценарии могут выполняться параллельно.

## Демонстрация сервиса

Запустите локальный Kafka в Docker:

```powershell
docker compose up -d
```

Затем во втором терминале запустите приложение в профиле `demo`:

```powershell
mvn spring-boot:run "-Dspring-boot.run.profiles=demo"
```

Демо публикует один заказ с суммой `125.50` и один с суммой `0`. В логах приложения появятся отправка сообщений и строки обработки, например:

```text
Processed order: orderId=demo-valid-..., amount=125.50, status=ACCEPTED
Processed order: orderId=demo-invalid-..., amount=0, status=REJECTED
```

По умолчанию приложение использует `localhost:9092`. Адрес можно изменить через `KAFKA_BOOTSTRAP_SERVERS`.

Остановить Kafka:

```powershell
docker compose down
```

## Структура проекта

- `src/main/java/ru/kafka/orders/model` — DTO и статусы заказов.
- `src/main/java/ru/kafka/orders/service` — Kafka producer, consumer и демонстрационная отправка.
- `src/test/java/ru/kafka/orders` — интеграционные сценарии JUnit 5.
- `src/main/resources/application.yml` — Kafka, JSON и названия топиков.
- `docker-compose.yml` — локальный Kafka для ручной демонстрации.
