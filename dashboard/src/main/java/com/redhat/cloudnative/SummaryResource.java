package com.redhat.cloudnative;

import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.annotations.Channel;
import io.smallrye.reactive.messaging.kafka.KafkaMessage;

@Path("/")
public class SummaryResource {

    private static final Logger log = LoggerFactory.getLogger(SummaryResource.class);

    @Inject
    @Channel("orders")
    Flowable<KafkaMessage<Long, String>> summary; 

    @Inject
    @Channel("fraud")
    Flowable<KafkaMessage<Long, String>> fraud; 

    @Inject
    Template dashboard;

    @GET
    @Consumes(MediaType.TEXT_HTML)
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance index() {
        return dashboard.data("");
    }

    @GET
    @Path("consume")
    @Produces(MediaType.SERVER_SENT_EVENTS)
    public Publisher<String> consume() {
        return Flowable.merge(
            summary.map(e -> {
                String m = String.valueOf(e.getPayload());
                log.info("event=" + m);
                return m;
            }), 
            fraud.map(e -> String.valueOf(e.getPayload()))
        );
    } 
}