package com.redhat.cloudnative;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonObject;


@ApplicationScoped
public class OrderPipelineKStreams {

    private static final Logger log = LoggerFactory.getLogger(OrderPipelineKStreams.class);

    final Serde<OrderSummary> summarySerde = new OrderSummary();

    @Produces
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> raw = builder.stream("dbserver1.order.order", Consumed.with(Serdes.String(), Serdes.String()));

        // Mask CC info
        KStream<String, String> orders = raw.mapValues(
            value -> {
				try {
                    JsonObject order = new JsonObject(value);
                    String ccNumber = order.getString("ccNumber");
                    order.put("ccNumber", "****" + ccNumber.substring(ccNumber.length()-4));
					return order.encode();
				} catch (Exception e) {
					e.printStackTrace();
                }
                return null;
			}
        );

        KStream<String, String>[] fraud = orders.branch(
            (key,value) -> {
                JsonObject o = new JsonObject(value);
                return Double.valueOf(o.getString("total")) > 250;
            }
        );

        KGroupedStream<Long,String> allOrders = orders.groupBy(
            (key, value) -> 1l, 
            Grouped.with(Serdes.Long(), Serdes.String())
        );

        KTable<Long, OrderSummary> aggregatedOrders = allOrders.aggregate(
            () -> new OrderSummary(0d, 0l), 
            (key, value, aggregation) -> {
                JsonObject o = new JsonObject(value);
                aggregation.setCount(aggregation.getCount() + 1l);
                double totalO = Double.valueOf(o.getString("total"));
                aggregation.setTotals(aggregation.getTotals() + totalO);
                return aggregation;
            },
            Materialized.as("orders-aggregated").with(Serdes.Long(), summarySerde)
        );

        aggregatedOrders.toStream().to("orders-summary");

        fraud[0].to("potential-fraud", Produced.with(Serdes.String(), Serdes.String()));
        // KStream<Long, Long> count = allOrders.count().toStream();
        // count
        // .peek((key, value) -> log.info("key => " + key + " value => " + value));

		return builder.build();
    }

}