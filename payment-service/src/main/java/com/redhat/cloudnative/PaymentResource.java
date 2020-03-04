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

    // TODO: Add handleCloudEvent method here

    // TODO: Add pass method here

    // TODO: Add fail method here

    // TODO: Add consumer method here

}