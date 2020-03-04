package com.redhat.cloudnative;

import com.redhat.cloudnative.model.order.Order;
import com.redhat.cloudnative.model.ShoppingCart;
import com.redhat.cloudnative.service.ShoppingCartService;
import io.vertx.core.json.Json;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.quarkus.runtime.StartupEvent;
// TODO: Add imports

import java.util.Properties;


@Path("/api/cart")
public class CartResource {

    private static final Logger log = LoggerFactory.getLogger(CartResource.class);

    // TODO: Add annotation of orders messaging configuration here
    @Inject
    ShoppingCartService shoppingCartService;

    // TODO ADD getCart method


    @POST
    @Path("/{cartId}/{itemId}/{quantity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Add an Item with its quantity")
    public ShoppingCart add(@PathParam("cartId") String cartId,
                            @PathParam("itemId") String itemId,
                            @PathParam("quantity") int quantity) throws Exception {
        return shoppingCartService.addItem(cartId, itemId, quantity);
    }

    @POST
    @Path("/{cartId}/{tmpId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "Set the cart with a new Id")
    public ShoppingCart set(@PathParam("cartId") String cartId,
                            @PathParam("tmpId") String tmpId) throws Exception {
        return shoppingCartService.set(cartId, tmpId);
    }

    @DELETE
    @Path("/{cartId}/{itemId}/{quantity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "delete an the quantity or item from the cart")
    public ShoppingCart delete(@PathParam("cartId") String cartId,
                               @PathParam("itemId") String itemId,
                               @PathParam("quantity") int quantity) throws Exception {
        return shoppingCartService.deleteItem(cartId, itemId, quantity);
    }

    @POST
    @Path("/checkout/{cartId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary = "checkout")
    public ShoppingCart checkout(@PathParam("cartId") String cartId, Order order) {
        sendOrder(order, cartId);
        return shoppingCartService.checkout(cartId);
    }

    // TODO ADD for EDA
    private void sendOrder(Order order, String cartId) {

    }

}
