package com.resilienceguard.orderservice.model;

import lombok.Data;
import java.time.Instant;
import java.util.UUID;

@Data
public class Order {
    private String orderId = UUID.randomUUID().toString();
    private String customerId;
    private double amount;
    private Instant createdAt = Instant.now();
}
