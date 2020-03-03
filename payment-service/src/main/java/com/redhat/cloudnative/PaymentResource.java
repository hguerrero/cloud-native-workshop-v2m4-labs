package com.redhat.cloudnative;

import java.io.IOException;
import java.util.concurrent.CompletionStage;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Path;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.reactive.messaging.annotations.Channel;
import io.smallrye.reactive.messaging.annotations.Emitter;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

@Path("/")
@Singleton
public class PaymentResource {

    public static final Logger log = LoggerFactory.getLogger(PaymentResource.class);

    @Inject
    Vertx vertx;

    // TODO: Add Messaging Producer here
    @Inject
    @Channel("payments")
    private Emitter<String> producer;

    // TODO: Add handleCloudEvent method here
    public void handleCloudEvent(String cloudEventJson) {
        String orderId = "unknown";
        String paymentId = "" + ((int)(Math.floor(Math.random() * 100000)));

        try {
            log.info("received event: " + cloudEventJson);
            JsonObject event = new JsonObject(cloudEventJson);
            orderId = event.getString("orderId");
            String total = event.getString("total");
            String ccNumber = event.getString("ccNumber");
            String name = event.getString("name");

            // Reject AMX
            if (ccNumber.startsWith("4") || ccNumber.startsWith("5") || ccNumber.startsWith("6")) {
                pass(orderId, paymentId, "Payment of " + total + " succeeded for " + name + " CC details: " + ccNumber);
            }
            else {
                fail(orderId, paymentId, "Invalid Credit Card: " + ccNumber);
            }
        } catch (Exception ex) {
             fail(orderId, paymentId, "Unknown error: " + ex.getMessage() + " for payment: " + cloudEventJson);
        }
    }

    // TODO: Add pass method here
    private void pass(String orderId, String paymentId, String remarks) {
        JsonObject payload = new JsonObject();
        payload.put("orderId", orderId);
        payload.put("paymentId", paymentId);
        payload.put("remarks", remarks);
        payload.put("status", "COMPLETED");
        log.info("Sending payment success: " + payload.toString());
        producer.send(payload.toString());
    }

    // TODO: Add fail method here
    private void fail(String orderId, String paymentId, String remarks) {
        JsonObject payload = new JsonObject();
        payload.put("orderId", orderId);
        payload.put("paymentId", paymentId);
        payload.put("remarks", remarks);
        payload.put("status", "FAILED");
        log.info("Sending payment failure: " + payload.toString());
        producer.send( payload.toString());
    }

    // TODO: Add consumer method here
    @Incoming("invoices")
    public CompletionStage<Void> onMessage(Message<String> message)
            throws IOException {

        log.info("Event message with value = {} arrived", message.getPayload());
        // fake processing time between 1 and 30 seconds
        long randomTime = Double.valueOf(1000 + (Math.random() * 29000)).longValue();
        vertx.setTimer(randomTime, l -> {
            handleCloudEvent(message.getPayload());    
        });
        
        return message.ack();
    }

}