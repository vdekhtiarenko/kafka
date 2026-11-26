package ru.kafka.orders.model;

public class OrderResult {

    private String orderId;
    private OrderStatus status;

    public OrderResult() {
    }

    public OrderResult(String orderId, OrderStatus status) {
        this.orderId = orderId;
        this.status = status;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public void setStatus(OrderStatus status) {
        this.status = status;
    }
}
