package com.redhat.cloudnative;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.enterprise.context.ApplicationScoped;

import com.github.javafaker.Faker;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Flowable;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

@ApplicationScoped
public class OrderGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(OrderGenerator.class);

    @Outgoing("orders")
    public Flowable<Message<String>> generateOrder() {
        return Flowable.interval(500, TimeUnit.MILLISECONDS)
                .onBackpressureDrop()
                .map(tick -> {
                    Order o = randomOrder();
                    LOG.info("sending order: {}", o);
                    return Message.of(Json.encode(o));
                });
    }

    private static final Faker datagen = new Faker();

	private Order randomOrder() {
        Order order = new Order();
        order.setName(datagen.name().fullName());
        order.setBillingAddress(datagen.address().fullAddress());
        order.setTotal(datagen.commerce().price(1, 300));
        order.setOrderId("B-" + UUID.randomUUID());
        order.setCreditCard(new CreditCard(
            datagen.finance().creditCard().replaceAll("-", ""), 
            credit_card_expiration(MONTHS_STRINGS, YEARS_STRINGS), 
            datagen.name().fullName()));
        return order;
    }

    private static final String[] MONTHS_STRINGS = new String[] { "01", 
            "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"};
    
    private static final String[] YEARS_STRINGS = new String[] { "20", 
            "21", "22", "23", "24", "25"};

    private String credit_card_expiration(String[] monthsList, String[] yearsList) {
        return monthsList[(int) Math.floor(Math.random() * monthsList.length)] + "/" + 
                yearsList[(int) Math.floor(Math.random() * yearsList.length)];
    }
}