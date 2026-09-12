package com.resilienceguard.orderservice.controller;

import com.resilienceguard.orderservice.model.Order;
import com.resilienceguard.orderservice.service.OrderProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/orders")
public class OrderController {

    @Autowired
    private OrderProducer orderProducer;

    @PostMapping
    public Order createOrder(@RequestBody Order order) {
        orderProducer.publishOrder(order);
        return order;
    }
}
