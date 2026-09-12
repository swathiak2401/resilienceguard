package com.resilienceguard.orderservice.service;

import com.resilienceguard.orderservice.model.Order;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderProducer {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    public void publishOrder(Order order) {
        kafkaTemplate.send("order-placed", order.getOrderId(), order);
    }
}
