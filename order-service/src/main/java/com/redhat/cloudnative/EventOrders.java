package com.redhat.cloudnative;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;

import java.io.IOException;
import java.util.concurrent.CompletionStage;

import javax.inject.Inject;

import io.smallrye.reactive.messaging.annotations.Channel;
import io.smallrye.reactive.messaging.annotations.Emitter;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

@ApplicationScoped
public class EventOrders {

    private static final Logger LOG = LoggerFactory.getLogger(EventOrders.class);

    @Inject
    @Channel("invoices")
    private Emitter<String> producer;

    @Inject
    OrderService orderService;

    @Incoming("orders")
    public CompletionStage<Void> onMessage(Message<String> message)
            throws IOException {

        LOG.info("Event order message with value = {} arrived", message.getPayload());

        JsonObject orders = new JsonObject(message.getPayload());
        Order order = new Order();
        order.setOrderId(orders.getString("orderId"));
        order.setName(orders.getString("name"));
        order.setTotal(orders.getString("total"));
        order.setCcNumber(orders.getJsonObject("creditCard").getString("number"));
        order.setCcExp(orders.getJsonObject("creditCard").getString("expiration"));
        order.setBillingAddress(orders.getString("billingAddress"));
        order.setStatus("PROCESSING");
        
        producer.send(Json.encode(order));

        orderService.add(order);

        return message.ack();
    }

    @Incoming("payments")
    public CompletionStage<Void> onMessagePayments(Message<String> message)
            throws IOException {

        LOG.info("Event payment message with value = {} arrived", message.getPayload());

        JsonObject payments = new JsonObject(message.getPayload());
        orderService.updateStatus(payments.getString("orderId"), payments.getString("status"));

        return message.ack();
    }

}