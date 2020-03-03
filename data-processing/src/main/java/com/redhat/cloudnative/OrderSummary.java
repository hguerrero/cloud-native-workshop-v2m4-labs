package com.redhat.cloudnative;

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderSummary implements Serializer<OrderSummary>, Deserializer<OrderSummary>, Serde<OrderSummary> {
    
    static Logger log = LoggerFactory.getLogger(OrderSummary.class);
    
    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    Double totals;
    Long count;
    
    public OrderSummary() {}

    public OrderSummary(Double totals, Long count) {
        this.totals = totals;
        this.count = count;
    }

    @Override
    public String toString() {
        try {
            return OBJECT_MAPPER.writeValueAsString(this);
        } catch (Exception e) {
            log.error("coudl not serialize", e);
        }
        return super.toString();
    }

    public static OrderSummary parse(String json) {
        try {
			return OBJECT_MAPPER.readValue(json, OrderSummary.class);
		} catch (Exception e) {
			log.error("could not parse", e);
		}
		return new OrderSummary(0d,0l);
    }

    @Override
    public Serializer<OrderSummary> serializer() {
        return this;
    }

    @Override
    public Deserializer<OrderSummary> deserializer() {
        return this;
    }

    @Override
    public OrderSummary deserialize(String topic, byte[] data) {
        if (data == null) {
			return null;
		}
		try {
			return OBJECT_MAPPER.readValue(data, OrderSummary.class);
		} catch (final IOException e) {
			throw new SerializationException(e);
        }
    }

    @Override
    public byte[] serialize(String topic, OrderSummary data) {
		if (data == null) {
			return null;
		}
		try {
			return OBJECT_MAPPER.writeValueAsBytes(data);
		} catch (final Exception e) {
			throw new SerializationException("Error serializing JSON message", e);
		}
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public void close() {

    }

	public Double getTotals() {
		return totals;
	}

	public void setTotals(Double totals) {
		this.totals = totals;
	}

	public Long getCount() {
		return count;
	}

	public void setCount(Long count) {
		this.count = count;
	}

}